// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package ddl_test

import (
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

func TestFailBeforeDecodeArgs(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t1 (c1 int, c2 int);")
	tk.MustExec("insert t1 values (1, 2);")

	var tableID int64
	rs := tk.MustQuery("select TIDB_TABLE_ID from information_schema.tables where table_name='t1' and table_schema='test';")
	tableIDi, _ := strconv.Atoi(rs.Rows()[0][0].(string))
	tableID = int64(tableIDi)

	first := true
	stateCnt := 0
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		// It can be other schema states except failed schema state.
		// This schema state can only appear once.
		if job.SchemaState == model.StateWriteOnly {
			stateCnt++
		} else if job.SchemaState == model.StateWriteReorganization {
			if first {
				require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/errorBeforeDecodeArgs", `return(true)`))
				first = false
			} else {
				require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/errorBeforeDecodeArgs"))
			}
		}
	})
	defaultValue := int64(3)
	jobID := testCreateColumn(tk, t, testkit.NewTestKit(t, store).Session(), tableID, "c3", "", defaultValue, dom)
	// Make sure the schema state only appears once.
	require.Equal(t, 1, stateCnt)
	testCheckJobDone(t, store, jobID, true)
}
