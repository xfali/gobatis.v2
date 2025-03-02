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
	"github.com/xfali/gobatis/v2/parsing/template"
	"github.com/xfali/gobatis/v2/parsing/xml"
	"os"
	"path/filepath"
	"sync"
)

type ManagerRegistry struct {
	dynamicStmtMgrs map[string]Manager
	locker          sync.RWMutex
}

func NewManagerRegistry() *ManagerRegistry {
	return &ManagerRegistry{
		dynamicStmtMgrs: map[string]Manager{},
	}
}

func initGlobalMgrRegistry() *ManagerRegistry {
	m := NewManagerRegistry()
	_ = m.RegisterManager(xml.NewManager())
	_ = m.RegisterManager(template.NewManager())
	return m
}

var globalMgrRegistry = initGlobalMgrRegistry()

func (m *ManagerRegistry) RegisterManager(mgr Manager) error {
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

func (m *ManagerRegistry) FindManager(format string) (Manager, bool) {
	m.locker.RLock()
	defer m.locker.RUnlock()

	v, ok := m.dynamicStmtMgrs[format]
	return v, ok
}

func (m *ManagerRegistry) ScanMapperFile(dir string) error {
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

func ScanMapperFile(dir string) error {
	return globalMgrRegistry.ScanMapperFile(dir)
}
