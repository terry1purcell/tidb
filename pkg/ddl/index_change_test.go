// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestIndexChange(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int primary key, c2 int)")
	tk.MustExec("insert t values (1, 1), (2, 2), (3, 3);")

	// set up hook
	prevState := model.StateNone
	addIndexDone := false
	var jobID atomic.Int64
	var (
		deleteOnlyTable table.Table
		writeOnlyTable  table.Table
		publicTable     table.Table
	)
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		if job.Type != model.ActionAddIndex || job.TableName != "t" {
			return
		}
		if job.SchemaState == prevState {
			return
		}
		jobID.Store(job.ID)
		ctx1 := testNewContext(t, store)
		prevState = job.SchemaState
		require.NoError(t, dom.Reload())
		tbl, exist := dom.InfoSchema().TableByID(context.Background(), job.TableID)
		require.True(t, exist)
		switch job.SchemaState {
		case model.StateDeleteOnly:
			deleteOnlyTable = tbl
		case model.StateWriteOnly:
			writeOnlyTable = tbl
			err := checkAddWriteOnlyForAddIndex(ctx1, deleteOnlyTable, writeOnlyTable)
			require.NoError(t, err)
		case model.StatePublic:
			require.Equalf(t, int64(3), job.GetRowCount(), "job's row count %d != 3", job.GetRowCount())
			publicTable = tbl
			err := checkAddPublicForAddIndex(ctx1, writeOnlyTable, publicTable)
			require.NoError(t, err)
			if job.State == model.JobStateSynced {
				addIndexDone = true
			}
		}
	})
	tk.MustExec("alter table t add index c2(c2)")
	// We need to make sure onJobUpdated is called in the first hook.
	// After testCreateIndex(), onJobUpdated() may not be called when job.state is Sync.
	// If we skip this check, prevState may wrongly set to StatePublic.
	for i := 0; i <= 100; i++ {
		if addIndexDone {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	v := getSchemaVer(t, tk.Session())
	checkHistoryJobArgs(t, tk.Session(), jobID.Load(), &historyJobArgs{ver: v, tbl: publicTable.Meta()})

	prevState = model.StateNone
	var noneTable table.Table
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterWaitSchemaSynced", func(job *model.Job) {
		jobID.Store(job.ID)
		if job.SchemaState == prevState {
			return
		}
		prevState = job.SchemaState
		var err error
		require.NoError(t, dom.Reload())
		tbl, exist := dom.InfoSchema().TableByID(context.Background(), job.TableID)
		require.True(t, exist)
		ctx1 := testNewContext(t, store)
		switch job.SchemaState {
		case model.StateWriteOnly:
			writeOnlyTable = tbl
			err = checkDropWriteOnly(ctx1, publicTable, writeOnlyTable)
			require.NoError(t, err)
		case model.StateDeleteOnly:
			deleteOnlyTable = tbl
			err = checkDropDeleteOnly(ctx1, writeOnlyTable, deleteOnlyTable)
			require.NoError(t, err)
		case model.StateNone:
			noneTable = tbl
			require.Equalf(t, 0, len(noneTable.Indices()), "index should have been dropped")
		}
	})
	tk.MustExec("alter table t drop index c2")
	v = getSchemaVer(t, tk.Session())
	checkHistoryJobArgs(t, tk.Session(), jobID.Load(), &historyJobArgs{ver: v, tbl: noneTable.Meta()})
}

func checkIndexExists(ctx sessionctx.Context, tbl table.Table, indexValue any, handle int64, exists bool) error {
	idx := tbl.Indices()[0]
	txn, err := ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	sc := ctx.GetSessionVars().StmtCtx
	doesExist, _, err := idx.Exist(sc.ErrCtx(), sc.TimeZone(), txn, types.MakeDatums(indexValue), kv.IntHandle(handle))
	if err != nil {
		return errors.Trace(err)
	}
	if exists != doesExist {
		if exists {
			return errors.New("index should exists")
		}
		return errors.New("index should not exists")
	}
	return nil
}

func checkAddWriteOnlyForAddIndex(ctx sessionctx.Context, delOnlyTbl, writeOnlyTbl table.Table) error {
	// DeleteOnlyTable: insert t values (4, 4);
	txn, err := newTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = delOnlyTbl.AddRecord(ctx.GetTableCtx(), txn, types.MakeDatums(4, 4))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeOnlyTbl, 4, 4, false)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyTable: insert t values (5, 5);
	_, err = writeOnlyTbl.AddRecord(ctx.GetTableCtx(), txn, types.MakeDatums(5, 5))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeOnlyTbl, 5, 5, true)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyTable: update t set c2 = 1 where c1 = 4 and c2 = 4
	err = writeOnlyTbl.UpdateRecord(ctx.GetTableCtx(), txn, kv.IntHandle(4), types.MakeDatums(4, 4), types.MakeDatums(4, 1), touchedSlice(writeOnlyTbl))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeOnlyTbl, 1, 4, true)
	if err != nil {
		return errors.Trace(err)
	}

	// DeleteOnlyTable: update t set c2 = 3 where c1 = 4 and c2 = 1
	err = delOnlyTbl.UpdateRecord(ctx.GetTableCtx(), txn, kv.IntHandle(4), types.MakeDatums(4, 1), types.MakeDatums(4, 3), touchedSlice(writeOnlyTbl))
	if err != nil {
		return errors.Trace(err)
	}
	// old value index not exists.
	err = checkIndexExists(ctx, writeOnlyTbl, 1, 4, false)
	if err != nil {
		return errors.Trace(err)
	}
	// new value index not exists.
	err = checkIndexExists(ctx, writeOnlyTbl, 3, 4, false)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyTable: delete t where c1 = 4 and c2 = 3
	err = writeOnlyTbl.RemoveRecord(ctx.GetTableCtx(), txn, kv.IntHandle(4), types.MakeDatums(4, 3))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeOnlyTbl, 3, 4, false)
	if err != nil {
		return errors.Trace(err)
	}

	// DeleteOnlyTable: delete t where c1 = 5
	err = delOnlyTbl.RemoveRecord(ctx.GetTableCtx(), txn, kv.IntHandle(5), types.MakeDatums(5, 5))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, writeOnlyTbl, 5, 5, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func checkAddPublicForAddIndex(ctx sessionctx.Context, writeTbl, publicTbl table.Table) error {
	var err1 error
	// WriteOnlyTable: insert t values (6, 6)
	txn, err := newTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = writeTbl.AddRecord(ctx.GetTableCtx(), txn, types.MakeDatums(6, 6))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, publicTbl, 6, 6, true)
	if vardef.EnableFastReorg.Load() {
		// Need check temp index also.
		err1 = checkIndexExists(ctx, writeTbl, 6, 6, true)
	}
	if err != nil && err1 != nil {
		return errors.Trace(err)
	}
	// PublicTable: insert t values (7, 7)
	_, err = publicTbl.AddRecord(ctx.GetTableCtx(), txn, types.MakeDatums(7, 7))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, publicTbl, 7, 7, true)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyTable: update t set c2 = 5 where c1 = 7 and c2 = 7
	err = writeTbl.UpdateRecord(ctx.GetTableCtx(), txn, kv.IntHandle(7), types.MakeDatums(7, 7), types.MakeDatums(7, 5), touchedSlice(writeTbl))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, publicTbl, 5, 7, true)
	if vardef.EnableFastReorg.Load() {
		// Need check temp index also.
		err1 = checkIndexExists(ctx, writeTbl, 5, 7, true)
	}
	if err != nil && err1 != nil {
		return errors.Trace(err)
	}
	if vardef.EnableFastReorg.Load() {
		err = checkIndexExists(ctx, writeTbl, 7, 7, false)
	} else {
		err = checkIndexExists(ctx, publicTbl, 7, 7, false)
	}
	if err != nil {
		return errors.Trace(err)
	}
	// WriteOnlyTable: delete t where c1 = 6
	err = writeTbl.RemoveRecord(ctx.GetTableCtx(), txn, kv.IntHandle(6), types.MakeDatums(6, 6))
	if err != nil {
		return errors.Trace(err)
	}
	err = checkIndexExists(ctx, publicTbl, 6, 6, false)
	if err != nil {
		return errors.Trace(err)
	}

	var rows [][]types.Datum
	err = tables.IterRecords(publicTbl, ctx, publicTbl.Cols(),
		func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
			rows = append(rows, data)
			return true, nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		return errors.New("table is empty")
	}
	for _, row := range rows {
		idxVal := row[1].GetInt64()
		handle := row[0].GetInt64()
		err = checkIndexExists(ctx, publicTbl, idxVal, handle, true)
		if vardef.EnableFastReorg.Load() {
			// Need check temp index also.
			err1 = checkIndexExists(ctx, writeTbl, idxVal, handle, true)
		}
		if err != nil && err1 != nil {
			return errors.Trace(err)
		}
	}
	return txn.Commit(context.Background())
}

func checkDropWriteOnly(ctx sessionctx.Context, publicTbl, writeTbl table.Table) error {
	// WriteOnlyTable insert t values (8, 8)
	txn, err := newTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = writeTbl.AddRecord(ctx.GetTableCtx(), txn, types.MakeDatums(8, 8))
	if err != nil {
		return errors.Trace(err)
	}

	err = checkIndexExists(ctx, publicTbl, 8, 8, true)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyTable update t set c2 = 7 where c1 = 8 and c2 = 8
	err = writeTbl.UpdateRecord(ctx.GetTableCtx(), txn, kv.IntHandle(8), types.MakeDatums(8, 8), types.MakeDatums(8, 7), touchedSlice(writeTbl))
	if err != nil {
		return errors.Trace(err)
	}

	err = checkIndexExists(ctx, publicTbl, 7, 8, true)
	if err != nil {
		return errors.Trace(err)
	}

	// WriteOnlyTable delete t where c1 = 8
	err = writeTbl.RemoveRecord(ctx.GetTableCtx(), txn, kv.IntHandle(8), types.MakeDatums(8, 7))
	if err != nil {
		return errors.Trace(err)
	}

	err = checkIndexExists(ctx, publicTbl, 7, 8, false)
	if err != nil {
		return errors.Trace(err)
	}
	return txn.Commit(context.Background())
}

func checkDropDeleteOnly(ctx sessionctx.Context, writeTbl, delTbl table.Table) error {
	// WriteOnlyTable insert t values (9, 9)
	txn, err := newTxn(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = writeTbl.AddRecord(ctx.GetTableCtx(), txn, types.MakeDatums(9, 9))
	if err != nil {
		return errors.Trace(err)
	}

	err = checkIndexExists(ctx, writeTbl, 9, 9, true)
	if err != nil {
		return errors.Trace(err)
	}

	// DeleteOnlyTable insert t values (10, 10)
	_, err = delTbl.AddRecord(ctx.GetTableCtx(), txn, types.MakeDatums(10, 10))
	if err != nil {
		return errors.Trace(err)
	}

	err = checkIndexExists(ctx, writeTbl, 10, 10, false)
	if err != nil {
		return errors.Trace(err)
	}

	// DeleteOnlyTable update t set c2 = 10 where c1 = 9
	err = delTbl.UpdateRecord(ctx.GetTableCtx(), txn, kv.IntHandle(9), types.MakeDatums(9, 9), types.MakeDatums(9, 10), touchedSlice(delTbl))
	if err != nil {
		return errors.Trace(err)
	}

	err = checkIndexExists(ctx, writeTbl, 9, 9, false)
	if err != nil {
		return errors.Trace(err)
	}

	err = checkIndexExists(ctx, writeTbl, 10, 9, false)
	if err != nil {
		return errors.Trace(err)
	}
	return txn.Commit(context.Background())
}

func TestAddIndexRowCountUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (c1 int primary key, c2 int)")
	tk.MustExec("insert t values (1, 1), (2, 2), (3, 3);")
	tk.MustExec("set @@tidb_ddl_reorg_worker_cnt = 1;")
	tk.MustExec("set global tidb_ddl_enable_fast_reorg = 0;")
	tk.MustExec("set global tidb_enable_dist_task = 0;")

	var jobID int64
	rowCntUpdated := make(chan struct{})
	backfillDone := make(chan struct{})
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/updateProgressIntervalInMs", "return(50)")
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterHandleBackfillTask", func(id int64) {
		jobID = id
		backfillDone <- struct{}{}
		<-rowCntUpdated
	})
	go func() {
		defer func() {
			rowCntUpdated <- struct{}{}
		}()
		<-backfillDone
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		require.Eventually(t, func() bool {
			rs := tk2.MustQuery("admin show ddl jobs 1;").Rows()
			idStr := rs[0][0].(string)
			id, err := strconv.Atoi(idStr)
			require.NoError(t, err)
			require.Equal(t, int64(id), jobID)
			rcStr := rs[0][7].(string)
			rc, err := strconv.Atoi(rcStr)
			require.NoError(t, err)
			return rc > 0
		}, 2*time.Minute, 60*time.Millisecond)
	}()
	tk.MustExec("alter table t add index idx(c2);")
}
