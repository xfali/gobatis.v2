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

package gobatis

import (
	"github.com/xfali/gobatis/v2/parsing"
	"github.com/xfali/gobatis/v2/parsing/sqlparser"
	"github.com/xfali/gobatis/v2/parsing/template"
	"github.com/xfali/gobatis/v2/parsing/xml"
	"os"
	"path/filepath"
)

type sqlManager struct {
	dynamicSqlMgr  *xml.Manager
	templateSqlMgr *template.Manager
}

func NewSqlManager() *sqlManager {
	return &sqlManager{
		dynamicSqlMgr:  xml.NewManager(),
		templateSqlMgr: template.NewManager(),
	}
}

var globalSqlMgr = NewSqlManager()

func (m *sqlManager) RegisterSql(sqlId string, sql string) error {
	return m.dynamicSqlMgr.RegisterSql(sqlId, sql)
}

func (m *sqlManager) UnregisterSql(sqlId string) {
	m.dynamicSqlMgr.UnregisterSql(sqlId)
}

func (m *sqlManager) RegisterMapperData(data []byte) error {
	return m.dynamicSqlMgr.RegisterData(data)
}

func (m *sqlManager) RegisterMapperFile(file string) error {
	return m.dynamicSqlMgr.RegisterFile(file)
}

func (m *sqlManager) FindDynamicSqlParser(sqlId string) (sqlparser.SqlParser, bool) {
	return m.dynamicSqlMgr.FindSqlParser(sqlId)
}

func (m *sqlManager) RegisterTemplateData(data []byte) error {
	return m.templateSqlMgr.RegisterData(data)
}

func (m *sqlManager) RegisterTemplateFile(file string) error {
	return m.templateSqlMgr.RegisterFile(file)
}

func (m *sqlManager) FindTemplateSqlParser(sqlId string) (sqlparser.SqlParser, bool) {
	return m.templateSqlMgr.FindSqlParser(sqlId)
}

func (m *sqlManager) ScanMapperFile(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			filename := filepath.Base(path)
			length := len(filename)
			if length > 4 {
				if filename[length-4:] == ".xml" {
					err := m.RegisterMapperFile(path)
					if err != nil {
						return err
					}
				}
				if filename[length-4:] == ".tpl" {
					err := m.RegisterTemplateFile(path)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

func RegisterSql(sqlId string, sql string) error {
	return globalSqlMgr.RegisterSql(sqlId, sql)
}

func UnregisterSql(sqlId string) {
	globalSqlMgr.UnregisterSql(sqlId)
}

func RegisterMapperData(data []byte) error {
	return globalSqlMgr.RegisterMapperData(data)
}

func RegisterMapperFile(file string) error {
	return globalSqlMgr.RegisterMapperFile(file)
}

func FindDynamicSqlParser(sqlId string) (sqlparser.SqlParser, bool) {
	return globalSqlMgr.FindDynamicSqlParser(sqlId)
}

func RegisterTemplateData(data []byte) error {
	return globalSqlMgr.RegisterTemplateData(data)
}

func RegisterTemplateFile(file string) error {
	return globalSqlMgr.RegisterTemplateFile(file)
}

func FindTemplateSqlParser(sqlId string) (sqlparser.SqlParser, bool) {
	return globalSqlMgr.FindTemplateSqlParser(sqlId)
}

func DynamicParserFactory(sql string) (sqlparser.SqlParser, error) {
	return &parsing.DynamicData{OriginData: sql}, nil
}

func TemplateParserFactory(sql string) (sqlparser.SqlParser, error) {
	return template.CreateParser([]byte(sql))
}

func ScanMapperFile(dir string) error {
	return globalSqlMgr.ScanMapperFile(dir)
}
