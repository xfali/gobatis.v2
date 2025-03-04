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

package reflection

import (
	"fmt"
	"github.com/xfali/reflection"
	"reflect"
	"strconv"
	"strings"
)

const (
	sliceParamSeparator = "_&eLEm_"
)

type paramParser struct {
	ret   map[string]interface{}
	index int
}

func ParseParams(params ...interface{}) map[string]interface{} {
	parser := paramParser{
		ret:   map[string]interface{}{},
		index: 0,
	}
	parser.innerParse(params...)
	return parser.ret
}

func (parser *paramParser) innerParse(params ...interface{}) {
	for i := range params {
		parser.parseOne("", params[i])
	}
}

func (parser *paramParser) parseOne(parentKey string, v interface{}) {
	rt := reflect.TypeOf(v)
	rv := reflect.ValueOf(v)

	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
		rv = rv.Elem()
	}

	if reflection.IsSimpleType(rt) {
		if parentKey == "" {
			parser.ret[parentKey+strconv.Itoa(parser.index)] = v
			parser.index++
		} else {
			parser.ret[parentKey[:len(parentKey)-1]] = v
		}
	} else if rt.Kind() == reflect.Struct {
		oi, _ := reflection.GetStructInfo(v)
		structMap := oi.MapValue()
		for key, value := range structMap {
			parser.ret[parentKey+structKey(oi, key)] = value
		}
	} else if rt.Kind() == reflect.Slice {
		l := rv.Len()
		for i := 0; i < l; i++ {
			elemV := rv.Index(i)
			if !elemV.CanInterface() {
				elemV = reflect.Indirect(elemV)
			}
			parser.parseOne(fmt.Sprintf("%s%d[%d].", parentKey, parser.index, i), elemV.Interface())
		}
		parser.ret[strconv.Itoa(parser.index)] = l
		parser.index++
		//l := rv.Len()
		//builder := strings.Builder{}
		//for i := 0; i < l; i++ {
		//	elemV := rv.Index(i)
		//	if !elemV.CanInterface() {
		//		elemV = reflect.Indirect(elemV)
		//	}
		//	if elemV.Kind() == reflect.String {
		//		builder.WriteString(elemV.String())
		//	} else {
		//		var str string
		//		if SafeSetValue(reflect.ValueOf(&str), elemV.Interface()) {
		//			builder.WriteString(str)
		//		} else {
		//			//log
		//		}
		//	}
		//
		//	if i < l-1 {
		//		builder.WriteString(slice_param_separator)
		//	}
		//}
		//parser.ret[strconv.Itoa(parser.index)] = builder.String()
		//parser.index++
	} else if rt.Kind() == reflect.Map {
		keys := rv.MapKeys()
		for _, key := range keys {
			if key.Kind() == reflect.String {
				value := rv.MapIndex(key)
				value = value.Elem()
				if reflection.IsSimpleType(value.Type()) {
					if !value.CanInterface() {
						value = reflect.Indirect(value)
					}
					parser.ret[parentKey+key.String()] = value
				}
			}
		}
	}
}

func ParseSliceParamString(src string) []string {
	return strings.Split(src, sliceParamSeparator)
}

func (parser *paramParser) setSliceValue(parentKey string) string {
	key := fmt.Sprintf("%s%d[", parentKey, parser.index)
	builder := strings.Builder{}
	parser.ret[strconv.Itoa(parser.index)] = builder.String()
	for k := range parser.ret {
		if strings.Index(k, key) == 0 {
			builder.WriteString(k)
			builder.WriteString(sliceParamSeparator)
		}
	}

	s := builder.String()
	if len(s) > 7 {
		return s[:len(s)-7]
	} else {
		return s
	}
}

func structKey(oi *reflection.StructInfo, field string) string {
	return oi.Name + "." + field
}
