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
	"strconv"
)

type Holder func(int) string

var gHolderMap = map[string]Holder{
	"mysql":    MysqlHolder,    //mysql
	"postgres": PostgresHolder, //postgresql
	"oci8":     Oci8Holder,     //oracle
	"adodb":    MysqlHolder,    //sqlserver
}

func RegisterParamHolder(driverName string, h Holder) bool {
	_, ok := GetHolder(driverName)
	gHolderMap[driverName] = h
	return ok
}

func SelectHolder(driverName string) Holder {
	if v, ok := GetHolder(driverName); ok {
		return v
	}
	return MysqlHolder
}

func GetHolder(driverName string) (Holder, bool) {
	v, ok := gHolderMap[driverName]
	return v, ok
}

func MysqlHolder(int) string {
	return "?"
}

func PostgresHolder(i int) string {
	return "$" + strconv.Itoa(i)
}

func Oci8Holder(i int) string {
	return ":" + strconv.Itoa(i)
}
