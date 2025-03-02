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
	"github.com/xfali/gobatis/v2/errors"
	"github.com/xfali/gobatis/v2/parsing/parser"
	"github.com/xfali/gobatis/v2/parsing/template"
	"github.com/xfali/gobatis/v2/parsing/xml"
	"os"
	"path/filepath"
	"sync"
)

type ManagerRegistry interface {
	RegisterManager(mgr Manager) error

	FindManager(format string) (Manager, bool)
	
	ScanMapperFile(dir string) error
}

type defaultManagerRegistry struct {
	dynamicStmtMgrs map[string]Manager
	locker          sync.RWMutex
}

func NewManagerRegistry() *defaultManagerRegistry {
	return &defaultManagerRegistry{
		dynamicStmtMgrs: map[string]Manager{},
	}
}

func initGlobalMgrRegistry() *defaultManagerRegistry {
	m := NewManagerRegistry()
	pr := parser.NewRegistry()
	_ = m.RegisterManager(xml.NewManager(pr))
	_ = m.RegisterManager(template.NewManager(pr))
	return m
}

var globalMgrRegistry = initGlobalMgrRegistry()

func (m *defaultManagerRegistry) RegisterManager(mgr Manager) error {
	m.locker.Lock()
	defer m.locker.Unlock()

	supports := mgr.SupportFileFormat()
	for _, v := range supports {
		if _, ok := m.dynamicStmtMgrs[v]; ok {
			return errors.ParseManagerDuplicates
		} else {
			m.dynamicStmtMgrs[v] = mgr
		}
	}
	return nil
}

func (m *defaultManagerRegistry) FindManager(format string) (Manager, bool) {
	m.locker.RLock()
	defer m.locker.RUnlock()

	v, ok := m.dynamicStmtMgrs[format]
	return v, ok
}

func (m *defaultManagerRegistry) ScanMapperFile(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			ext := filepath.Ext(path)
			length := len(ext)
			if length > 0 {
				if mgr, ok := m.FindManager(ext[1:]); ok {
					err := mgr.RegisterMapperFile(path)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

func RegisterManager(mgr Manager) error {
	return globalMgrRegistry.RegisterManager(mgr)
}

func FindManager(format string) (Manager, bool) {
	return globalMgrRegistry.FindManager(format)
}

func ScanMapperFile(dir string) error {
	return globalMgrRegistry.ScanMapperFile(dir)
}
