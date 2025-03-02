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
	"github.com/xfali/gobatis/v2/parsing"
	"github.com/xfali/gobatis/v2/parsing/parser"
	"github.com/xfali/xlog"
)

type Manager struct {
	logger   xlog.Logger
	registry parser.Registry
}

func NewManager(registry parser.Registry) *Manager {
	if registry == nil {
		registry = parser.NewRegistry()
	}
	return &Manager{
		logger:   xlog.GetLogger(),
		registry: registry,
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

func (manager *Manager) FindDynamicStatementParser(sqlId string) (parser.Parser, bool) {
	return manager.FindSqlParser(sqlId)
}

func (manager *Manager) CreateDynamicStatementParser(sql string) (parser.Parser, error) {
	return &parsing.DynamicData{OriginData: sql}, nil
}

func (manager *Manager) RegisterData(data []byte) error {
	return manager.registry.Direct(func(r parser.Registry) error {
		mapper, err := Parse(data)
		if err != nil {
			manager.logger.Warnf("register mapper data failed: %s err: %v\n", string(data), err)
			return err
		}

		return manager.formatMapper(r, mapper)
	})
}

func (manager *Manager) RegisterFile(file string) error {
	return manager.registry.Direct(func(r parser.Registry) error {
		mapper, err := ParseFile(file)
		if err != nil {
			manager.logger.Warnf("register mapper file failed: %s err: %v\n", file, err)
			return err
		}

		return manager.formatMapper(r, mapper)
	})
}

func (manager *Manager) formatMapper(registry parser.Registry, mapper *Mapper) error {
	ret := mapper.Format()
	for k, v := range ret {
		err := registry.AddParser(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (manager *Manager) FindSqlParser(sqlId string) (parser.Parser, bool) {
	return manager.registry.FindParser(sqlId)
}

func (manager *Manager) RegisterSql(sqlId string, sql string) error {
	_, err := manager.registry.LoadOrCreateParser(sqlId, sql, func(statement string) (parser.Parser, error) {
		return &parsing.DynamicData{OriginData: sql}, nil
	})
	return err
}

func (manager *Manager) UnregisterSql(sqlId string) {
	manager.registry.RemoveParser(sqlId)
}
