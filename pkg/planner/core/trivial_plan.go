// Copyright 2026 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// TryTrivialPlan attempts to build a physical plan directly for trivial queries
// that don't benefit from the full optimization pipeline. This is a fast path
// for queries like "SELECT * FROM t" or "SELECT a, b FROM t" on tables where
// the only viable plan is a full table scan (no secondary indexes, no TiFlash
// replicas, no predicates, etc.).
//
// This avoids all logical optimization rules, stats loading, cost estimation,
// and post-optimization passes that are unnecessary when the plan is predetermined.
// It is designed for execution performance, not EXPLAIN accuracy.
func TryTrivialPlan(ctx base.PlanContext, node *resolve.NodeW) (base.Plan, types.NameSlice) {
	if checkStableResultMode(ctx) {
		return nil, nil
	}
	if fixcontrol.GetBoolWithDefault(ctx.GetSessionVars().OptimizerFixControl, fixcontrol.Fix52592, false) {
		return nil, nil
	}

	// Skip during EXPLAIN — the normal optimizer produces richer plan
	// metadata (costs, stats annotations) that EXPLAIN output depends on.
	if ctx.GetSessionVars().StmtCtx.InExplainStmt {
		return nil, nil
	}

	sel, ok := node.Node.(*ast.SelectStmt)
	if !ok {
		return nil, nil
	}
	if !isTrivialSelect(sel) {
		return nil, nil
	}

	// Single table, no joins or subqueries.
	tblName, tblAlias := getSingleTableNameAndAlias(sel.From)
	if tblName == nil {
		return nil, nil
	}
	// Index hints (USE/FORCE/IGNORE INDEX) affect path selection.
	if len(tblName.IndexHints) > 0 {
		return nil, nil
	}

	// Look up the already-resolved table info from the resolve context.
	resolveCtx := node.GetResolveContext()
	tnW := resolveCtx.GetTableName(tblName)
	if tnW == nil {
		return nil, nil
	}
	tblInfo := tnW.TableInfo
	if tblInfo == nil {
		return nil, nil
	}

	// Resolve database name.
	dbName := tblName.Schema
	if dbName.L == "" {
		dbName = ast.NewCIStr(ctx.GetSessionVars().CurrentDB)
	}

	// System/memory tables use special executors; skip fast path.
	if metadef.IsMemDB(dbName.L) {
		return nil, nil
	}

	if !isTrivialTable(tblInfo) {
		return nil, nil
	}

	// Privilege check.
	if err := checkFastPlanPrivilege(ctx, dbName.L, tblInfo.Name.L, mysql.SelectPriv); err != nil {
		return nil, nil
	}

	// Build output schema and column names from the SELECT field list.
	// buildSchemaFromFields returns nil when the field list contains expressions,
	// function calls, or other constructs that need the full planner.
	schema, names := buildSchemaFromFields(dbName, tblInfo, tblAlias, sel.Fields.Fields)
	if schema == nil {
		return nil, nil
	}

	// Build the full set of table columns (for row-size estimation) and the
	// pruned set of scan columns (matching the schema the SELECT requests).
	allColumns := make([]*model.ColumnInfo, 0, len(tblInfo.Columns))
	tblCols := make([]*expression.Column, 0, len(tblInfo.Columns))
	for i, col := range tblInfo.Columns {
		if col.State == model.StatePublic {
			allColumns = append(allColumns, col)
			tblCols = append(tblCols, colInfoToColumn(col, i))
		}
	}

	// Prune scan columns to only those present in the output schema. This
	// ensures TiKV returns only the columns the client expects.
	schemaColIDs := make(map[int64]struct{}, schema.Len())
	for _, col := range schema.Columns {
		schemaColIDs[col.ID] = struct{}{}
	}
	columns := make([]*model.ColumnInfo, 0, schema.Len())
	for _, col := range allColumns {
		if _, ok := schemaColIDs[col.ID]; ok {
			columns = append(columns, col)
		}
	}

	// Row count estimate from stats cache (no synchronous loading).
	rowCount := trivialRowCountEstimate(ctx, tblInfo)
	statsInfo := &property.StatsInfo{RowCount: rowCount}

	// Provide a HistColl so downstream callers (e.g. GetAvgRowSize for
	// network buffer sizing) don't hit a nil dereference.
	pseudoHist := statistics.PseudoHistColl(tblInfo.ID, false)

	ctx.GetSessionVars().PlanID.Store(0)
	ctx.GetSessionVars().PlanColumnID.Store(0)

	// Determine the correct full range based on handle type.
	var scanRanges ranger.Ranges
	if tblInfo.IsCommonHandle {
		scanRanges = ranger.FullRange()
	} else {
		isUnsigned := false
		if tblInfo.PKIsHandle {
			if pkColInfo := tblInfo.GetPkColInfo(); pkColInfo != nil {
				isUnsigned = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
			}
		}
		scanRanges = ranger.FullIntRange(isUnsigned)
	}

	// Build PhysicalTableScan.
	ts := physicalop.PhysicalTableScan{
		Table:           tblInfo,
		Columns:         columns,
		DBName:          dbName,
		TableAsName:     &tblAlias,
		Ranges:          scanRanges,
		AccessCondition: nil,
		StoreType:       kv.TiKV,
		TblCols:         tblCols,
		TblColHists:     &pseudoHist,
	}.Init(ctx, 0)
	ts.SetSchema(schema.Clone())
	ts.SetStats(statsInfo)

	// Wrap in PhysicalTableReader.
	tr := physicalop.PhysicalTableReader{
		TablePlan:      ts,
		StoreType:      kv.TiKV,
		IsCommonHandle: tblInfo.IsCommonHandle,
	}.Init(ctx, 0)
	// Init() flattens TablePlan into TablePlans, copies schema, sets ReadReqType.
	tr.SetStats(statsInfo)

	// Record table access for privilege and lock checking.
	ctx.GetSessionVars().StmtCtx.Tables = []stmtctx.TableEntry{
		{DB: dbName.L, Table: tblInfo.Name.L},
	}

	return tr, names
}

// isTrivialSelect checks whether a SelectStmt is simple enough for the trivial
// fast path: single table, no predicates, no ordering, no grouping, no limits,
// no hints, no locking, no CTEs, no DISTINCT, no INTO.
func isTrivialSelect(sel *ast.SelectStmt) bool {
	return sel.Kind == ast.SelectStmtKindSelect &&
		sel.From != nil &&
		sel.Where == nil &&
		sel.GroupBy == nil &&
		sel.Having == nil &&
		sel.OrderBy == nil &&
		sel.Limit == nil &&
		sel.WindowSpecs == nil &&
		!sel.Distinct &&
		sel.SelectIntoOpt == nil &&
		sel.LockInfo == nil &&
		sel.With == nil &&
		len(sel.TableHints) == 0
}

// isTrivialTable checks whether a table's metadata allows the trivial plan
// fast path: no partitioning, no TiFlash replicas, no secondary indexes,
// and no virtual generated columns.
func isTrivialTable(tblInfo *model.TableInfo) bool {
	if tblInfo.GetPartitionInfo() != nil {
		return false
	}
	if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
		return false
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic && !idx.Primary {
			return false
		}
	}
	for _, col := range tblInfo.Columns {
		if col.IsGenerated() && !col.GeneratedStored {
			return false
		}
	}
	return true
}

// trivialRowCountEstimate returns a row count estimate without triggering
// synchronous stats loading. Uses the cached RealtimeCount when available,
// falling back to PseudoRowCount.
func trivialRowCountEstimate(ctx base.PlanContext, tblInfo *model.TableInfo) float64 {
	statsHandle := domain.GetDomain(ctx).StatsHandle()
	if statsHandle != nil {
		statsTbl := statsHandle.GetPhysicalTableStats(tblInfo.ID, tblInfo)
		if statsTbl != nil && statsTbl.RealtimeCount > 0 {
			return float64(statsTbl.RealtimeCount)
		}
	}
	return statistics.PseudoRowCount
}
