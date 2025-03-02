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
	"strings"
)

type Holder func(int) string

type ParamPlaceHolder interface {
	GetByIndex(index int) string
	GetByName(name string) string
	Replace(s, old string, index int, name string) string
}

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

type MysqlParamPlaceHolder struct {
}

func (h *MysqlParamPlaceHolder) GetByIndex(index int) string {
	return MysqlHolder(index)
}

func (h *MysqlParamPlaceHolder) GetByName(name string) string {
	return MysqlHolder(0)
}

func (h *MysqlParamPlaceHolder) Replace(s, old string, index int, name string) string {
	return strings.Replace(s, old, h.GetByIndex(index), -1)
}

func PostgresHolder(i int) string {
	return "$" + strconv.Itoa(i)
}

type PostgresParamPlaceHolder struct {
}

func (h *PostgresParamPlaceHolder) GetByIndex(index int) string {
	return PostgresHolder(index)
}

func (h *PostgresParamPlaceHolder) GetByName(name string) string {
	return "?"
}

func (h *PostgresParamPlaceHolder) Replace(s, old string, index int, name string) (string, bool) {
	return strings.Replace(s, old, h.GetByIndex(index), 1), true
}

func Oci8Holder(i int) string {
	return ":" + strconv.Itoa(i)
}

type Oci8ParamPlaceHolder struct {
}

func (h *Oci8ParamPlaceHolder) GetByIndex(index int) string {
	return Oci8Holder(index)
}

func (h *Oci8ParamPlaceHolder) GetByName(name string) string {
	return "?"
}

func (h *Oci8ParamPlaceHolder) Replace(s, old string, index int, name string) string {
	return strings.Replace(s, old, h.GetByIndex(index), 1)
}
