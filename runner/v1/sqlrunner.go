/*
 * Copyright (C) 2025, Xiongfa Li.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package v1

import (
	"context"
	"github.com/xfali/gobatis/v2/database/factory"
	"github.com/xfali/gobatis/v2/errors"
	"github.com/xfali/gobatis/v2/parsing/manager"
	"github.com/xfali/gobatis/v2/parsing/parser"
	"github.com/xfali/gobatis/v2/parsing/sqlparser"
	"github.com/xfali/lean/connection"
	"github.com/xfali/lean/mapping"
	"github.com/xfali/lean/session"
	"github.com/xfali/reflection"
	"github.com/xfali/xlog"
)

const (
	ContextSessionKey = "__gobatis_session__"
)

type ParserFactory func(sql string) (parser.Parser, error)

type SessionManager struct {
	logger        xlog.Logger
	driverName    string
	conn          connection.Connection
	registry      parser.Registry
	ParserFactory ParserFactory
}

func NewSessionManager(factory factory.Factory) *SessionManager {
	m, _ := manager.GetGlobalManagerRegistry().FindManager("xml")
	return &SessionManager{
		logger:        xlog.GetLogger(),
		driverName:    factory.GetDriverName(),
		conn:          factory.CreateConnection(),
		registry:      manager.GetGlobalParserRegistry(),
		ParserFactory: m.CreateDynamicStatementParser,
	}
}

type Runner interface {
	// Param 参数
	// 注意：如果没有参数也必须调用
	// 如果参数个数为1并且为struct，将解析struct获得参数
	// 如果参数个数大于1并且全部为简单类型，或则个数为1且为简单类型，则使用这些参数
	Param(params ...interface{}) Runner
	// Result 获得结果
	Result(bean interface{}) error
	// LastInsertId 最后插入的自增id
	LastInsertId() int64
	// Context 设置Context
	Context(ctx context.Context) Runner
}

type Session struct {
	ctx           context.Context
	logger        xlog.Logger
	session       session.Session
	driver        string
	registry      parser.Registry
	ParserFactory ParserFactory
}

type BaseRunner struct {
	session  session.Session
	parser   parser.Parser
	action   string
	metadata *parser.Metadata
	logger   xlog.Logger
	driver   string
	ctx      context.Context
	runner   Runner
}

type SelectRunner struct {
	BaseRunner
}

type InsertRunner struct {
	lastId int64
	BaseRunner
}

type UpdateRunner struct {
	BaseRunner
}

type DeleteRunner struct {
	BaseRunner
}

type ExecRunner struct {
	BaseRunner
}

// NewSession 使用一个session操作数据库
func (sm *SessionManager) NewSession() *Session {
	sess, err := sm.conn.GetSession()
	if err != nil {
		sm.logger.Errorln(err)
		return nil
	}
	return &Session{
		ctx:           context.Background(),
		logger:        xlog.GetLogger(),
		session:       sess,
		driver:        sm.driverName,
		registry:      sm.registry,
		ParserFactory: sm.ParserFactory,
	}
}

// Context 包含session的context
func (sm *SessionManager) Context(ctx context.Context) context.Context {
	sess, err := sm.conn.GetSession()
	if err != nil {
		sm.logger.Errorln(err)
		return ctx
	}
	sqlSess := &Session{
		ctx:           ctx,
		logger:        xlog.GetLogger(),
		session:       sess,
		driver:        sm.driverName,
		registry:      sm.registry,
		ParserFactory: sm.ParserFactory,
	}
	return context.WithValue(ctx, ContextSessionKey, sqlSess)
}

func WithSession(ctx context.Context, sess *Session) context.Context {
	return context.WithValue(ctx, ContextSessionKey, sess)
}

func FindSession(ctx context.Context) *Session {
	if ctx == nil {
		return nil
	}
	return ctx.Value(ContextSessionKey).(*Session)
}

func (sm *SessionManager) Close() error {
	return sm.conn.Close()
}

// SetParserFactory 修改sql解析器创建者
func (sm *SessionManager) SetParserFactory(fac ParserFactory) {
	sm.ParserFactory = fac
}

// SetParserRegistry 修改sql解析器注册仓库，用于查找解析器
func (sm *SessionManager) SetParserRegistry(registry parser.Registry) {
	sm.registry = registry
}

func (s *Session) SetContext(ctx context.Context) *Session {
	s.ctx = ctx
	return s
}

func (s *Session) GetContext() context.Context {
	return s.ctx
}

// SetParserFactory 修改sql解析器创建者
func (s *Session) SetParserFactory(fac ParserFactory) {
	s.ParserFactory = fac
}

// SetParserRegistry 修改sql解析器注册仓库
func (s *Session) SetParserRegistry(registry parser.Registry) {
	s.registry = registry
}

// Tx 开启事务执行语句
// 返回nil则提交，返回error回滚
// 抛出异常错误触发回滚
func (s *Session) Tx(ctx context.Context, txFunc func(session *Session) error) (err error) {
	e1 := s.session.Begin(ctx)
	if e1 != nil {
		return e1
	}
	defer func(err *error) {
		if r := recover(); r != nil {
			*err = s.session.Rollback(ctx)
			panic(r)
		}
	}(&err)

	if fnErr := txFunc(s); fnErr != nil {
		e := s.session.Rollback(ctx)
		if e != nil {
			s.logger.Warnf("Rollback error: %v , business error: %v\n", e, fnErr)
		}
		return fnErr
	} else {
		return s.session.Commit(ctx)
	}
}

func (s *Session) Select(sql string) Runner {
	return s.createSelect(s.findSqlParser(sql))
}

func (s *Session) Update(sql string) Runner {
	return s.createUpdate(s.findSqlParser(sql))
}

func (s *Session) Delete(sql string) Runner {
	return s.createDelete(s.findSqlParser(sql))
}

func (s *Session) Insert(sql string) Runner {
	return s.createInsert(s.findSqlParser(sql))
}

func (s *Session) Exec(sql string) Runner {
	return s.createExec(s.findSqlParser(sql))
}

func (baseRunner *BaseRunner) Param(params ...interface{}) Runner {
	//TODO: 使用缓存加速，避免每次都生成动态sql
	//测试发现性能提升非常有限，故取消
	//key := cache.CalcKey(baseRunner.sqlDynamicData.OriginData, paramMap)
	//md := cache.FindMetadata(key)
	//var err error
	//if md == nil {
	//    md, err := baseRunner.parser.Parse(params...)
	//    if err == nil {
	//        cache.CacheMetadata(key, md)
	//    }
	//}

	if baseRunner.parser == nil {
		baseRunner.logger.Warnf(errors.ParseParserNilError.Error())
		return baseRunner
	}

	md, err := baseRunner.parser.ParseMetadata(baseRunner.driver, params...)

	if err == nil {
		if baseRunner.action == "" || baseRunner.action == md.Action {
			baseRunner.metadata = md
		} else {
			//allow different action
			baseRunner.logger.Warnf("sql action not match expect %s get %s", baseRunner.action, md.Action)
			baseRunner.metadata = md
		}
	} else {
		baseRunner.logger.Warnf(err.Error())
	}
	return baseRunner.runner
}

//Context 设置执行的context
func (baseRunner *BaseRunner) Context(ctx context.Context) Runner {
	baseRunner.ctx = ctx
	return baseRunner.runner
}

func (r *SelectRunner) Result(bean interface{}) error {
	if r.metadata == nil {
		r.logger.Warnf("Sql Metadata is nil")
		return errors.RunnerNotReady
	}

	if reflection.IsNil(bean) {
		return errors.ResultPointerIsNil
	}

	ret, err := r.session.Query(r.ctx, r.metadata.PrepareSql, r.metadata.Params...)
	if err != nil {
		r.logger.Warnln(err)
		return err
	}

	defer ret.Close()
	_, err = mapping.ScanRows(bean, ret)
	if err != nil {
		r.logger.Warnln(err)
		return err
	}
	return nil
}

func (r *InsertRunner) Result(bean interface{}) error {
	if r.metadata == nil {
		r.logger.Warnf("Sql Metadata is nil")
		return errors.RunnerNotReady
	}
	ret, err := r.session.Execute(r.ctx, r.metadata.PrepareSql, r.metadata.Params...)
	if err != nil {
		r.logger.Warnln(err)
		return err
	}
	defer ret.Close()
	r.lastId, err = ret.LastInsertId()
	if err != nil {
		r.logger.Warnln(err)
	}
	if reflection.CanSet(bean) {
		err = reflection.SetValueInterface(bean, r.lastId)
	}
	return err
}

func (r *InsertRunner) LastInsertId() int64 {
	return r.lastId
}

func (r *UpdateRunner) Result(bean interface{}) error {
	if r.metadata == nil {
		r.logger.Warnf("Sql Metadata is nil")
		return errors.RunnerNotReady
	}
	ret, err := r.session.Execute(r.ctx, r.metadata.PrepareSql, r.metadata.Params...)
	if err != nil {
		r.logger.Warnln(err)
		return err
	}
	defer ret.Close()
	i, err := ret.RowsAffected()
	if err != nil {
		r.logger.Warnln(err)
	}

	if reflection.CanSet(bean) {
		err = reflection.SetValueInterface(bean, i)
	}
	return err
}

func (r *ExecRunner) Result(bean interface{}) error {
	if r.metadata == nil {
		r.logger.Warnf("Sql Metadata is nil")
		return errors.RunnerNotReady
	}
	ret, err := r.session.Execute(r.ctx, r.metadata.PrepareSql, r.metadata.Params...)
	if err != nil {
		r.logger.Warnln(err)
		return err
	}
	defer ret.Close()
	i, err := ret.RowsAffected()
	if err != nil {
		r.logger.Warnln(err)
	}
	if reflection.CanSet(bean) {
		err = reflection.SetValueInterface(bean, i)
	}
	return err
}

func (r *DeleteRunner) Result(bean interface{}) error {
	if r.metadata == nil {
		r.logger.Warnf("Sql Metadata is nil")
		return errors.RunnerNotReady
	}
	ret, err := r.session.Execute(r.ctx, r.metadata.PrepareSql, r.metadata.Params...)
	if err != nil {
		r.logger.Warnln(err)
		return err
	}
	defer ret.Close()
	i, err := ret.RowsAffected()
	if err != nil {
		r.logger.Warnln(err)
	}
	if reflection.CanSet(bean) {
		err = reflection.SetValueInterface(bean, i)
	}
	return err
}

func (baseRunner *BaseRunner) Result(bean interface{}) error {
	//FAKE RETURN
	panic("Cannot be here")
	//return nil, nil
}

func (baseRunner *BaseRunner) LastInsertId() int64 {
	return -1
}

func (s *Session) createSelect(parser parser.Parser) Runner {
	ret := &SelectRunner{}
	ret.action = sqlparser.SELECT
	ret.logger = s.logger
	ret.session = s.session
	ret.parser = parser
	ret.ctx = s.ctx
	ret.driver = s.driver
	ret.runner = ret
	return ret
}

func (s *Session) createUpdate(parser parser.Parser) Runner {
	ret := &UpdateRunner{}
	ret.action = sqlparser.UPDATE
	ret.logger = s.logger
	ret.session = s.session
	ret.parser = parser
	ret.ctx = s.ctx
	ret.driver = s.driver
	ret.runner = ret
	return ret
}

func (s *Session) createDelete(parser parser.Parser) Runner {
	ret := &DeleteRunner{}
	ret.action = sqlparser.DELETE
	ret.logger = s.logger
	ret.session = s.session
	ret.parser = parser
	ret.ctx = s.ctx
	ret.driver = s.driver
	ret.runner = ret
	return ret
}

func (s *Session) createInsert(parser parser.Parser) Runner {
	ret := &InsertRunner{}
	ret.action = sqlparser.INSERT
	ret.logger = s.logger
	ret.session = s.session
	ret.parser = parser
	ret.ctx = s.ctx
	ret.driver = s.driver
	ret.runner = ret
	return ret
}

func (s *Session) createExec(parser parser.Parser) Runner {
	ret := &ExecRunner{}
	ret.action = ""
	ret.logger = s.logger
	ret.session = s.session
	ret.parser = parser
	ret.ctx = s.ctx
	ret.driver = s.driver
	ret.runner = ret
	return ret
}

func (s *Session) findSqlParser(sqlId string) parser.Parser {
	ret, ok := s.registry.FindParser(sqlId)
	//FIXME: 当没有查找到sqlId对应的sql语句，则尝试使用sqlId直接操作数据库
	//该设计可能需要设计一个更合理的方式
	if !ok {
		d, err := s.ParserFactory(sqlId)
		if err != nil {
			s.logger.Warnf(err.Error())
			return nil
		}
		return d
	}
	return ret
}
