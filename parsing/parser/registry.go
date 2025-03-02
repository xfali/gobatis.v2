/*
 * Copyright (C) 2023-2025, Xiongfa Li.
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

package parser

import (
	"fmt"
	"github.com/xfali/gobatis/v2/errors"
	"sync"
)

type Metadata struct {
	Action     string
	PrepareSql string
	Vars       []string
	Params     []interface{}
}

func (md *Metadata) String() string {
	return fmt.Sprintf("action: %s, prepareSql: %s, varmap: %v, params: %v", md.Action, md.PrepareSql, md.Vars, md.Params)
}

type Parser interface {
	ParseMetadata(driverName string, params ...interface{}) (*Metadata, error)
}

type Registry interface {
	AddParser(sqlId string, parser Parser) error

	RemoveParser(sqlId string) bool

	FindParser(sqlId string) (Parser, bool)

	LoadOrCreateParser(sqlId string, statement string, creator func(statement string) (Parser, error)) (Parser, error)

	Direct(f func(r Registry) error) error
}

type simpleRegistry struct {
	parserMap map[string]Parser
}

func NewSimpleRegistry() *simpleRegistry {
	return &simpleRegistry{
		parserMap: map[string]Parser{},
	}
}

func (r *simpleRegistry) AddParser(sqlId string, parser Parser) error {
	if _, ok := r.parserMap[sqlId]; ok {
		return errors.SqlIdDuplicates
	} else {
		r.parserMap[sqlId] = parser
	}
	return nil
}

func (r *simpleRegistry) RemoveParser(sqlId string) bool {
	if _, ok := r.parserMap[sqlId]; ok {
		delete(r.parserMap, sqlId)
		return true
	}
	return false
}

func (r *simpleRegistry) LoadOrCreateParser(sqlId string, statement string, creator func(statement string) (Parser, error)) (Parser, error) {
	if v, ok := r.parserMap[sqlId]; ok {
		return v, errors.SqlIdDuplicates
	} else {
		dd, err := creator(statement)
		if err != nil {
			return nil, err
		}
		r.parserMap[sqlId] = dd
		return dd, nil
	}
}

func (r *simpleRegistry) Direct(f func(r Registry) error) error {
	return f(r)
}

func (r *simpleRegistry) FindParser(sqlId string) (Parser, bool) {
	v, ok := r.parserMap[sqlId]
	return v, ok
}

type defaultParserRegistry struct {
	rr   *simpleRegistry
	lock sync.RWMutex
}

func NewRegistry() *defaultParserRegistry {
	return &defaultParserRegistry{
		rr: NewSimpleRegistry(),
	}
}

func (r *defaultParserRegistry) AddParser(sqlId string, parser Parser) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.rr.AddParser(sqlId, parser)
}

func (r *defaultParserRegistry) RemoveParser(sqlId string) bool {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.rr.RemoveParser(sqlId)
}

func (r *defaultParserRegistry) LoadOrCreateParser(sqlId string, statement string, creator func(statement string) (Parser, error)) (Parser, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.rr.LoadOrCreateParser(sqlId, statement, creator)
}

func (r *defaultParserRegistry) Direct(f func(r Registry) error) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	return f(r.rr)
}

func (r *defaultParserRegistry) FindParser(sqlId string) (Parser, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.rr.FindParser(sqlId)
}
