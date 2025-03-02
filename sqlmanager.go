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

var sqlMgr = sqlManager{
	dynamicSqlMgr:  xml.NewManager(),
	templateSqlMgr: template.NewManager(),
}

func RegisterSql(sqlId string, sql string) error {
	return sqlMgr.dynamicSqlMgr.RegisterSql(sqlId, sql)
}

func UnregisterSql(sqlId string) {
	sqlMgr.dynamicSqlMgr.UnregisterSql(sqlId)
}

func RegisterMapperData(data []byte) error {
	return sqlMgr.dynamicSqlMgr.RegisterData(data)
}

func RegisterMapperFile(file string) error {
	return sqlMgr.dynamicSqlMgr.RegisterFile(file)
}

func FindDynamicSqlParser(sqlId string) (sqlparser.SqlParser, bool) {
	return sqlMgr.dynamicSqlMgr.FindSqlParser(sqlId)
}

func RegisterTemplateData(data []byte) error {
	return sqlMgr.templateSqlMgr.RegisterData(data)
}

func RegisterTemplateFile(file string) error {
	return sqlMgr.templateSqlMgr.RegisterFile(file)
}

func FindTemplateSqlParser(sqlId string) (sqlparser.SqlParser, bool) {
	return sqlMgr.templateSqlMgr.FindSqlParser(sqlId)
}

type ParserFactory func(sql string) (sqlparser.SqlParser, error)

func DynamicParserFactory(sql string) (sqlparser.SqlParser, error) {
	return &parsing.DynamicData{OriginData: sql}, nil
}

func TemplateParserFactory(sql string) (sqlparser.SqlParser, error) {
	return template.CreateParser([]byte(sql))
}

func ScanMapperFile(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			filename := filepath.Base(path)
			length := len(filename)
			if length > 4 {
				if filename[length-4:] == ".xml" {
					err := RegisterMapperFile(path)
					if err != nil {
						return err
					}
				}
				if filename[length-4:] == ".tpl" {
					err := RegisterTemplateFile(path)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}
