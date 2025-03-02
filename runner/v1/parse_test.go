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

package v1

import (
	"github.com/xfali/gobatis/v2/parsing/manager"
	"github.com/xfali/gobatis/v2/parsing/sqlparser"
	"github.com/xfali/xlog"
	"testing"
)

type testData struct {
	Id   int64
	Name string
}

func TestPostgresParse(t *testing.T) {
	m, _ := manager.GetGlobalManagerRegistry().FindManager("xml")
	t.Run("simple", func(t *testing.T) {
		parser, _ := m.CreateDynamicStatementParser("SELECT * FROM tbl_user WHERE id = #{0} AND name = #{1}")
		ret := &SelectRunner{}
		ret.action = sqlparser.SELECT
		ret.logger = xlog.GetLogger()
		ret.session = nil
		ret.parser = parser
		ret.driver = "postgres"
		ret.runner = ret

		ret.Param(100, "hello world")

		t.Log(ret.metadata)
	})
	t.Run("struct", func(t *testing.T) {
		parser, _ := m.CreateDynamicStatementParser("SELECT * FROM tbl_user WHERE id = #{testData.Id} AND name = #{testData.Name} AND friend_id != #{testData.Id}")
		ret := &SelectRunner{}
		ret.action = sqlparser.SELECT
		ret.logger = xlog.GetLogger()
		ret.session = nil
		ret.parser = parser
		ret.driver = "postgres"
		ret.runner = ret

		ret.Param(testData{100, "hello world"})

		t.Log(ret.metadata)
	})
}
