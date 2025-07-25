// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// StoreBootstrappedKey is used by store.G/SetOption to store related bootstrap context for kv.Storage.
const StoreBootstrappedKey = "bootstrap"

type domainMap struct {
	mu      syncutil.Mutex
	domains map[string]*domain.Domain
}

// Get or create the domain for store.
// TODO decouple domain create from it, it's more clear to create domain explicitly
// before any usage of it.
func (dm *domainMap) Get(store kv.Storage) (d *domain.Domain, err error) {
	return dm.getWithEtcdClient(store, nil)
}

func (dm *domainMap) getWithEtcdClient(store kv.Storage, etcdClient *clientv3.Client) (d *domain.Domain, err error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if store == nil {
		for _, d := range dm.domains {
			// return available domain if any
			return d, nil
		}
		return nil, errors.New("can not find available domain for a nil store")
	}

	key := store.UUID()

	d = dm.domains[key]
	if d != nil {
		return
	}

	ddlLease := time.Duration(atomic.LoadInt64(&schemaLease))
	statisticLease := time.Duration(atomic.LoadInt64(&statsLease))
	planReplayerGCLease := GetPlanReplayerGCLease()
	err = util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, func() (retry bool, err1 error) {
		logutil.BgLogger().Info("new domain",
			zap.String("store", store.UUID()),
			zap.Stringer("ddl lease", ddlLease),
			zap.Stringer("stats lease", statisticLease))
		factory := getSessionFactory(store)
		sysFactory := getSessionFactoryWithDom(store)
		d = domain.NewDomainWithEtcdClient(store, ddlLease, statisticLease, planReplayerGCLease, factory,
			func(targetKS string) pools.Factory {
				return getCrossKSSessionFactory(store, targetKS)
			},
			etcdClient,
		)

		var ddlInjector func(ddl.DDL, ddl.Executor, *infoschema.InfoCache) *schematracker.Checker
		if injector, ok := store.(schematracker.StorageDDLInjector); ok {
			ddlInjector = injector.Injector
		}
		err1 = d.Init(sysFactory, ddlInjector)
		if err1 != nil {
			// If we don't clean it, there are some dirty data when retrying the function of Init.
			d.Close()
			logutil.BgLogger().Error("init domain failed", zap.String("category", "ddl"),
				zap.Error(err1))
		}
		return true, err1
	})
	if err != nil {
		return nil, err
	}
	dm.domains[key] = d
	d.SetOnClose(func() {
		dm.Delete(store)
	})

	return
}

func (dm *domainMap) Delete(store kv.Storage) {
	dm.mu.Lock()
	delete(dm.domains, store.UUID())
	dm.mu.Unlock()
}

var (
	domap = &domainMap{
		domains: map[string]*domain.Domain{},
	}

	// schemaLease is lease of info schema, we use this to check whether info schema
	// is valid in SchemaChecker. we also use half of it as info schema reload interval.
	// Default info schema lease 45s which is init at main, we set it to 1 second
	// here for tests. you can change it with a proper time, but you must know that
	// too little may cause badly performance degradation.
	schemaLease = int64(1 * time.Second)

	// statsLease is the time for reload stats table.
	statsLease = int64(3 * time.Second)

	// planReplayerGCLease is the time for plan replayer gc.
	planReplayerGCLease = int64(10 * time.Minute)
)

// ResetStoreForWithTiKVTest is only used in the test code.
// TODO: Remove domap and storeBootstrapped. Use store.SetOption() to do it.
func ResetStoreForWithTiKVTest(store kv.Storage) {
	domap.Delete(store)
	store.SetOption(StoreBootstrappedKey, nil)
}

// SetSchemaLease changes the default schema lease time for DDL.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	atomic.StoreInt64(&schemaLease, int64(lease))
}

// SetStatsLease changes the default stats lease time for loading stats info.
func SetStatsLease(lease time.Duration) {
	atomic.StoreInt64(&statsLease, int64(lease))
}

// SetPlanReplayerGCLease changes the default plan repalyer gc lease time.
func SetPlanReplayerGCLease(lease time.Duration) {
	atomic.StoreInt64(&planReplayerGCLease, int64(lease))
}

// GetPlanReplayerGCLease returns the plan replayer gc lease time.
func GetPlanReplayerGCLease() time.Duration {
	return time.Duration(atomic.LoadInt64(&planReplayerGCLease))
}

// DisableStats4Test disables the stats for tests.
func DisableStats4Test() {
	SetStatsLease(-1)
}

// Parse parses a query string to raw ast.StmtNode.
func Parse(ctx sessionctx.Context, src string) ([]ast.StmtNode, error) {
	logutil.BgLogger().Debug("compiling", zap.String("source", src))
	sessVars := ctx.GetSessionVars()
	p := parser.New()
	p.SetParserConfig(sessVars.BuildParserConfig())
	p.SetSQLMode(sessVars.SQLMode)
	stmts, warns, err := p.ParseSQL(src, sessVars.GetParseParams()...)
	for _, warn := range warns {
		sessVars.StmtCtx.AppendWarning(warn)
	}
	if err != nil {
		logutil.BgLogger().Warn("compiling",
			zap.String("source", src),
			zap.Error(err))
		return nil, err
	}
	return stmts, nil
}

func recordAbortTxnDuration(sessVars *variable.SessionVars, isInternal bool) {
	duration := time.Since(sessVars.TxnCtx.CreateTime).Seconds()
	if sessVars.TxnCtx.IsPessimistic {
		if isInternal {
			session_metrics.TransactionDurationPessimisticAbortInternal.Observe(duration)
		} else {
			session_metrics.TransactionDurationPessimisticAbortGeneral.Observe(duration)
		}
	} else {
		if isInternal {
			session_metrics.TransactionDurationOptimisticAbortInternal.Observe(duration)
		} else {
			session_metrics.TransactionDurationOptimisticAbortGeneral.Observe(duration)
		}
	}
}

func finishStmt(ctx context.Context, se *session, meetsErr error, sql sqlexec.Statement) error {
	failpoint.Inject("finishStmtError", func() {
		failpoint.Return(errors.New("occur an error after finishStmt"))
	})
	sessVars := se.sessionVars
	if !sql.IsReadOnly(sessVars) {
		// All the history should be added here.
		if meetsErr == nil && sessVars.TxnCtx.CouldRetry {
			GetHistory(se).Add(sql, sessVars.StmtCtx)
		}

		// Handle the stmt commit/rollback.
		if se.txn.Valid() {
			if meetsErr != nil {
				se.StmtRollback(ctx, false)
			} else {
				se.StmtCommit(ctx)
			}
		}
	}
	err := autoCommitAfterStmt(ctx, se, meetsErr, sql)
	if se.txn.pending() {
		// After run statement finish, txn state is still pending means the
		// statement never need a Txn(), such as:
		//
		// set @@tidb_general_log = 1
		// set @@autocommit = 0
		// select 1
		//
		// Reset txn state to invalid to dispose the pending start ts.
		se.txn.changeToInvalid()
	}
	if err != nil {
		return err
	}
	return checkStmtLimit(ctx, se, true)
}

func autoCommitAfterStmt(ctx context.Context, se *session, meetsErr error, sql sqlexec.Statement) error {
	isInternal := false
	if internal := se.txn.GetOption(kv.RequestSourceInternal); internal != nil && internal.(bool) {
		isInternal = true
	}
	sessVars := se.sessionVars
	if meetsErr != nil {
		if !sessVars.InTxn() {
			logutil.BgLogger().Info("rollbackTxn called due to ddl/autocommit failure")
			se.RollbackTxn(ctx)
			recordAbortTxnDuration(sessVars, isInternal)
		} else if se.txn.Valid() && se.txn.IsPessimistic() && exeerrors.ErrDeadlock.Equal(meetsErr) {
			logutil.BgLogger().Info("rollbackTxn for deadlock", zap.Uint64("txn", se.txn.StartTS()))
			se.RollbackTxn(ctx)
			recordAbortTxnDuration(sessVars, isInternal)
		}
		return meetsErr
	}

	if !sessVars.InTxn() {
		if err := se.CommitTxn(ctx); err != nil {
			if _, ok := sql.(*executor.ExecStmt).StmtNode.(*ast.CommitStmt); ok {
				err = errors.Annotatef(err, "previous statement: %s", se.GetSessionVars().PrevStmt)
			}
			return err
		}
		return nil
	}
	return nil
}

func checkStmtLimit(ctx context.Context, se *session, isFinish bool) error {
	// If the user insert, insert, insert ... but never commit, TiDB would OOM.
	// So we limit the statement count in a transaction here.
	var err error
	sessVars := se.GetSessionVars()
	history := GetHistory(se)
	stmtCount := history.Count()
	if !isFinish {
		// history stmt count + current stmt, since current stmt is not finish, it has not add to history.
		stmtCount++
	}
	if stmtCount > int(config.GetGlobalConfig().Performance.StmtCountLimit) {
		if !sessVars.BatchCommit {
			se.RollbackTxn(ctx)
			return errors.Errorf("statement count %d exceeds the transaction limitation, transaction has been rollback, autocommit = %t",
				stmtCount, sessVars.IsAutocommit())
		}
		if !isFinish {
			// if the stmt is not finish execute, then just return, since some work need to be done such as StmtCommit.
			return nil
		}
		// If the stmt is finish execute, and exceed the StmtCountLimit, and BatchCommit is true,
		// then commit the current transaction and create a new transaction.
		err = sessiontxn.NewTxn(ctx, se)
		// The transaction does not committed yet, we need to keep it in transaction.
		// The last history could not be "commit"/"rollback" statement.
		// It means it is impossible to start a new transaction at the end of the transaction.
		// Because after the server executed "commit"/"rollback" statement, the session is out of the transaction.
		sessVars.SetInTxn(true)
	}
	return err
}

// GetHistory get all stmtHistory in current txn. Exported only for test.
// If stmtHistory is nil, will create a new one for current txn.
func GetHistory(ctx sessionctx.Context) *StmtHistory {
	hist, ok := ctx.GetSessionVars().TxnCtx.History.(*StmtHistory)
	if ok {
		return hist
	}
	hist = new(StmtHistory)
	ctx.GetSessionVars().TxnCtx.History = hist
	return hist
}

// GetRows4Test gets all the rows from a RecordSet, only used for test.
func GetRows4Test(ctx context.Context, _ sessionctx.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	req := rs.NewChunk(nil)
	// Must reuse `req` for imitating server.(*clientConn).writeChunks
	for {
		err := rs.Next(ctx, req)
		if err != nil {
			return nil, err
		}
		if req.NumRows() == 0 {
			break
		}

		iter := chunk.NewIterator4Chunk(req.CopyConstruct())
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// ResultSetToStringSlice changes the RecordSet to [][]string.
func ResultSetToStringSlice(ctx context.Context, s sessionapi.Session, rs sqlexec.RecordSet) ([][]string, error) {
	rows, err := GetRows4Test(ctx, s, rs)
	if err != nil {
		return nil, err
	}
	err = rs.Close()
	if err != nil {
		return nil, err
	}
	sRows := make([][]string, len(rows))
	for i := range rows {
		row := rows[i]
		iRow := make([]string, row.Len())
		for j := range row.Len() {
			if row.IsNull(j) {
				iRow[j] = "<nil>"
			} else {
				d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
				iRow[j], err = d.ToString()
				if err != nil {
					return nil, err
				}
			}
		}
		sRows[i] = iRow
	}
	return sRows, nil
}

// Session errors.
var (
	ErrForUpdateCantRetry = dbterror.ClassSession.NewStd(errno.ErrForUpdateCantRetry)
)
