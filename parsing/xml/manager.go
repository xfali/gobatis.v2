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

package xml

import (
	"github.com/xfali/xlog"
	"sync"

	"github.com/xfali/gobatis/v2/errors"
	"github.com/xfali/gobatis/v2/parsing"
	"github.com/xfali/gobatis/v2/parsing/sqlparser"
)

type Manager struct {
	logger xlog.Logger
	sqlMap map[string]*parsing.DynamicData
	lock   sync.Mutex
}

func NewManager() *Manager {
	return &Manager{
		logger: xlog.GetLogger(),
		sqlMap: map[string]*parsing.DynamicData{},
	}
}

func (manager *Manager) SupportFileFormat() []string {
	return []string{
		"xml",
	}
}

func (manager *Manager) RegisterMapperData(data []byte) error {
	return manager.RegisterData(data)
}

func (manager *Manager) RegisterMapperFile(file string) error {
	return manager.RegisterFile(file)
}

func (manager *Manager) FindDynamicStatementParser(sqlId string) (sqlparser.SqlParser, bool) {
	return manager.FindSqlParser(sqlId)
}

func (manager *Manager) CreateDynamicStatementParser(sql string) (sqlparser.SqlParser, error) {
	return &parsing.DynamicData{OriginData: sql}, nil
}

func (manager *Manager) RegisterData(data []byte) error {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	mapper, err := Parse(data)
	if err != nil {
		manager.logger.Warnf("register mapper data failed: %s err: %v\n", string(data), err)
		return err
	}

	return manager.formatMapper(mapper)
}

func (manager *Manager) RegisterFile(file string) error {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	mapper, err := ParseFile(file)
	if err != nil {
		manager.logger.Warnf("register mapper file failed: %s err: %v\n", file, err)
		return err
	}

	return manager.formatMapper(mapper)
}

func (manager *Manager) formatMapper(mapper *Mapper) error {
	ret := mapper.Format()
	for k, v := range ret {
		if _, ok := manager.sqlMap[k]; ok {
			return errors.SqlIdDuplicates
		} else {
			manager.sqlMap[k] = v
		}
	}
	return nil
}

func (manager *Manager) FindSqlParser(sqlId string) (sqlparser.SqlParser, bool) {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	v, ok := manager.sqlMap[sqlId]
	return v, ok
}

func (manager *Manager) RegisterSql(sqlId string, sql string) error {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	if _, ok := manager.sqlMap[sqlId]; ok {
		return errors.SqlIdDuplicates
	} else {
		dd := &parsing.DynamicData{OriginData: sql}
		manager.sqlMap[sqlId] = dd
	}
	return nil
}

func (manager *Manager) UnregisterSql(sqlId string) {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	delete(manager.sqlMap, sqlId)
}
