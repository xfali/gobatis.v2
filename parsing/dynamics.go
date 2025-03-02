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

package parsing

import (
	"github.com/xfali/gobatis/v2/parsing/parser"
	"github.com/xfali/gobatis/v2/reflection"
	reflection2 "github.com/xfali/reflection"
	"github.com/xfali/xlog"
	"strings"
	"time"

	"github.com/xfali/gobatis/v2/parsing/sqlparser"
)

type GetFunc func(key string) string

type DynamicElement interface {
	Format(func(key string) string) string
}

type DynamicData struct {
	OriginData     string
	DynamicElemMap map[string]DynamicElement
}

func (dynamicData *DynamicData) Replace(params ...interface{}) string {
	objMap := reflection.ParseParams(params...)
	return dynamicData.ReplaceWithMap(objMap)
}

// ReplaceWithMap 需要外部确保param是一个struct
func (dynamicData *DynamicData) ReplaceWithMap(objParams map[string]interface{}) string {
	if len(dynamicData.DynamicElemMap) == 0 || len(objParams) == 0 {
		xlog.Infoln("map is empty")
		//return dynamicData.OriginData
	}

	getFunc := func(s string) string {
		if o, ok := objParams[s]; ok {
			if str, ok := o.(string); ok {
				return str
			}

			//zero time convert to empty string (for <if> </if> element)
			if ti, ok := o.(time.Time); ok {
				if ti.IsZero() {
					return ""
				} else {
					return ti.String()
				}
			}

			var str string
			_ = reflection2.SetValueInterface(&str, o)
			return str
		}
		return ""
	}

	ret := dynamicData.OriginData
	for k, v := range dynamicData.DynamicElemMap {
		ret = strings.Replace(ret, k, v.Format(getFunc), -1)
	}
	return ret
}

func (dynamicData *DynamicData) ParseMetadata(driverName string, params ...interface{}) (*parser.Metadata, error) {
	paramMap := reflection.ParseParams(params...)
	sqlStr := dynamicData.ReplaceWithMap(paramMap)
	return sqlparser.ParseWithParamMap(driverName, sqlStr, paramMap)
}
