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

package template

import (
	"github.com/xfali/gobatis/v2/parsing/parser"
	"github.com/xfali/xlog"
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/xfali/gobatis/v2/errors"
)

const (
	namespaceTmplName = "namespace"
)

type Parser struct {
	//template
	tpl *template.Template
}

func CreateParser(data []byte) (*Parser, error) {
	tpl := template.New("")
	tpl = tpl.Funcs(dummyFuncMap)
	tpl, err := tpl.Parse(string(data))
	if err != nil {
		return nil, err
	}
	return &Parser{tpl: tpl}, nil
}

// ParseMetadata only use first param
func (p *Parser) ParseMetadata(driverName string, params ...interface{}) (*parser.Metadata, error) {
	if p.tpl == nil {
		return nil, errors.ParseTemplateNilError
	}
	b := strings.Builder{}
	var param interface{} = nil
	if len(params) == 1 {
		param = params[0]
	} else {
		param = params
	}
	dynamic := selectDynamic(driverName)
	tpl := p.tpl.Funcs(dynamic.getFuncMap())
	err := tpl.Execute(&b, param)
	if err != nil {
		return nil, err
	}

	ret := &parser.Metadata{}
	sql := strings.TrimSpace(b.String())
	action := sql[:6]
	action = strings.ToLower(action)
	ret.Action = action
	ret.PrepareSql, ret.Params = dynamic.format(sql)

	return ret, nil
}

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
		"tpl",
	}
}

func (manager *Manager) RegisterSql(sqlId string, sql string) error {
	_, err := manager.registry.LoadOrCreateParser(sqlId, sql, func(statement string) (parser.Parser, error) {
		return CreateParser([]byte(sql))
	})
	return err
}

func (manager *Manager) UnregisterSql(sqlId string) {
	manager.registry.RemoveParser(sqlId)
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
	return CreateParser([]byte(sql))
}

func (manager *Manager) RegisterData(data []byte) error {
	return manager.registry.Direct(func(r parser.Registry) error {
		tpl := template.New("")
		tpl = tpl.Funcs(dummyFuncMap)
		tpl, err := tpl.Parse(string(data))
		if err != nil {
			manager.logger.Warnf("register template data failed: %s err: %v\n", string(data), err)
			return err
		}

		ns := getNamespace(tpl)
		tpls := tpl.Templates()
		for _, v := range tpls {
			if v.Name() != "" && v.Name() != namespaceTmplName {
				addErr := r.AddParser(ns+v.Name(), &Parser{tpl: v})
				if addErr != nil {
					return addErr
				}
			}
		}

		return nil
	})
}

func (manager *Manager) RegisterFile(file string) error {
	return manager.registry.Direct(func(r parser.Registry) error {
		tpl := template.New("")
		data, err := ioutil.ReadFile(file)
		if err != nil {
			manager.logger.Warnf("register template file failed: %s err: %v\n", file, err)
			return err
		}
		tpl = tpl.Funcs(dummyFuncMap)
		tpl, err = tpl.Parse(string(data))
		if err != nil {
			manager.logger.Warnf("register template file failed: %s err: %v\n", file, err)
			return err
		}

		ns := getNamespace(tpl)
		tpls := tpl.Templates()
		for _, v := range tpls {
			if v.Name() != "" && v.Name() != namespaceTmplName {
				addErr := r.AddParser(ns+v.Name(), &Parser{tpl: v})
				if addErr != nil {
					return addErr
				}
			}
		}

		return nil
	})
}

func getNamespace(tpl *template.Template) string {
	ns := strings.Builder{}
	nsTpl := tpl.Lookup(namespaceTmplName)
	if nsTpl != nil {
		err := nsTpl.Execute(&ns, nil)
		if err != nil {
			ns.Reset()
		}
	}

	ret := strings.TrimSpace(ns.String())

	if ret != "" {
		ret = ret + "."
	}
	return ret
}

func (manager *Manager) FindSqlParser(sqlId string) (*Parser, bool) {
	v, have := manager.registry.FindParser(sqlId)
	if have {
		if ret, ok := v.(*Parser); ok {
			return ret, true
		}
	}
	return nil, false
}
