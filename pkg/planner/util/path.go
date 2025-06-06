// Copyright 2019 PingCAP, Inc.
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

package util

import (
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// AccessPath indicates the way we access a table: by using single index, or by using multiple indexes,
// or just by using table scan.
type AccessPath struct {
	Index          *model.IndexInfo
	FullIdxCols    []*expression.Column
	FullIdxColLens []int
	IdxCols        []*expression.Column
	IdxColLens     []int
	// ConstCols indicates whether the column is constant under the given conditions for all index columns.
	ConstCols []bool
	Ranges    []*ranger.Range
	// CountAfterAccess is the row count after we apply range seek and before we use other filter to filter data.
	// For index merge path, CountAfterAccess is the row count after partial paths and before we apply table filters.
	CountAfterAccess float64
	// CorrCountAfterAccess is the row count after only applying the most filtering index columns.
	// against the index. This is used when we don't have a full index statistics
	// and we need to use the exponential backoff to estimate the row count.
	// Case CorrCountAfterAccess > 0 : we use the exponential backoff to estimate the row count (such as we don't have a full index statistics)
	// Default CorrCountAfterAccess = 0 : we use index of table estimate row coun directly (such as table full scan, point get etc)
	CorrCountAfterAccess float64
	// CountAfterIndex is the row count after we apply filters on index and before we apply the table filters.
	CountAfterIndex float64
	AccessConds     []expression.Expression
	EqCondCount     int
	EqOrInCondCount int
	IndexFilters    []expression.Expression
	TableFilters    []expression.Expression
	// PartialIndexPaths store all index access paths.
	// If there are extra filters, store them in TableFilters.
	PartialIndexPaths []*AccessPath

	// The 3 fields below are for another case for building IndexMerge path besides AccessPath.PartialIndexPaths.
	// Currently, it only applies to OR type IndexMerge.
	// For every item in the OR list, there might be multiple candidate paths that satisfy the filters.
	// The AccessPath.PartialIndexPaths case decides on one of them when building AccessPath. But here, we keep all the
	// alternatives and make the decision later in findBestTask (see matchPropForIndexMergeAlternatives()).
	// It's because we only know the required Sort property at that time. Delaying the decision to findBestTask can make
	// us able to consider and try to satisfy the required Sort property.
	/* For example:
		create table t (a int, b int, c int, key a(a), key b(b), key ac(a, c), key bc(b, c));
		explain format='verbose' select * from t where a=1 or b=1 order by c;
	For a=1, it has two partial alternative paths: [a, ac]
	For b=1, it has two partial alternative paths: [b, bc]
	Then we build such a AccessPath:
		AccessPath {
			PartialAlternativeIndexPaths: [[[a], [ac]], [[b], [bc]]]
			IndexMergeORSourceFilter: a = 1 or b = 1
		}
	*/

	// PartialAlternativeIndexPaths stores all the alternative paths for each OR branch.
	// meaning of the 3 dimensions:
	// each OR branch -> each alternative for this OR branch -> each access path of this alternative (One JSON filter on
	// MV index may build into multiple partial paths. For example, json_overlap(a, '[1, 2, 3]') builds into 3 partial
	// paths in the final plan. For non-MV index, each alternative only has one AccessPath.)
	PartialAlternativeIndexPaths [][][]*AccessPath
	// KeepIndexMergeORSourceFilter indicates if we need to keep IndexMergeORSourceFilter in the final Selection of the
	// IndexMerge plan.
	// It has 2 cases:
	// 1. The AccessPath.PartialAlternativeIndexPaths is set.
	// If this field is true, the final plan should keep the filter.
	// 2. It's a children of AccessPath.PartialAlternativeIndexPaths.
	// If the final plan contains this alternative, it should keep the filter.
	KeepIndexMergeORSourceFilter bool
	// IndexMergeORSourceFilter is the original OR list for building the IndexMerge path.
	IndexMergeORSourceFilter expression.Expression

	// IndexMergeIsIntersection means whether it's intersection type or union type IndexMerge path.
	// It's only valid for a IndexMerge path.
	// Intersection type is for expressions connected by `AND` and union type is for `OR`.
	IndexMergeIsIntersection bool
	// IndexMergeAccessMVIndex indicates whether this IndexMerge path accesses a MVIndex.
	IndexMergeAccessMVIndex bool

	StoreType kv.StoreType

	// If the top level of the filters is an OR list, IsDNFCond is true.
	// In this case, MinAccessCondsForDNFCond will record the minimum number of access conditions among all DNF items.
	// For example, if the filter is (a=1 and b=2) or (a=3 and b=4) or (a=5 and b=6 and c=7),
	// for index (a) or index (b), MinAccessCondsForDNFCond will be 1;
	// for index (a, b, c), MinAccessCondsForDNFCond will be 2.
	IsDNFCond                bool
	MinAccessCondsForDNFCond int

	// IsIntHandlePath indicates whether this path is table path.
	IsIntHandlePath    bool
	IsCommonHandlePath bool
	// Forced means this path is generated by `use/force index()`.
	Forced           bool
	ForceKeepOrder   bool
	ForceNoKeepOrder bool
	// IsSingleScan indicates whether the path is a single index/table scan or table access after index scan.
	IsSingleScan bool

	// Maybe added in model.IndexInfo better, but the cache of model.IndexInfo may lead side effect
	IsUkShardIndexPath bool
}

// Clone returns a deep copy of the original AccessPath.
// Note that we rely on the Expression.Clone(), (*IndexInfo).Clone() and (*Range).Clone() in this method, so there are
// some fields like FieldType are not deep-copied.
func (path *AccessPath) Clone() *AccessPath {
	ret := &AccessPath{
		Index:                        path.Index.Clone(),
		FullIdxCols:                  CloneCols(path.FullIdxCols),
		FullIdxColLens:               slices.Clone(path.FullIdxColLens),
		IdxCols:                      CloneCols(path.IdxCols),
		IdxColLens:                   slices.Clone(path.IdxColLens),
		ConstCols:                    slices.Clone(path.ConstCols),
		Ranges:                       CloneRanges(path.Ranges),
		CountAfterAccess:             path.CountAfterAccess,
		CorrCountAfterAccess:         path.CorrCountAfterAccess,
		CountAfterIndex:              path.CountAfterIndex,
		AccessConds:                  CloneExprs(path.AccessConds),
		EqCondCount:                  path.EqCondCount,
		EqOrInCondCount:              path.EqOrInCondCount,
		IndexFilters:                 CloneExprs(path.IndexFilters),
		TableFilters:                 CloneExprs(path.TableFilters),
		IndexMergeIsIntersection:     path.IndexMergeIsIntersection,
		PartialIndexPaths:            nil,
		StoreType:                    path.StoreType,
		IsDNFCond:                    path.IsDNFCond,
		MinAccessCondsForDNFCond:     path.MinAccessCondsForDNFCond,
		IsIntHandlePath:              path.IsIntHandlePath,
		IsCommonHandlePath:           path.IsCommonHandlePath,
		Forced:                       path.Forced,
		ForceKeepOrder:               path.ForceKeepOrder,
		ForceNoKeepOrder:             path.ForceNoKeepOrder,
		IsSingleScan:                 path.IsSingleScan,
		IsUkShardIndexPath:           path.IsUkShardIndexPath,
		KeepIndexMergeORSourceFilter: path.KeepIndexMergeORSourceFilter,
	}
	if path.IndexMergeORSourceFilter != nil {
		ret.IndexMergeORSourceFilter = path.IndexMergeORSourceFilter.Clone()
	}
	ret.PartialIndexPaths = SliceDeepClone(path.PartialIndexPaths)
	ret.PartialAlternativeIndexPaths = make([][][]*AccessPath, 0, len(path.PartialAlternativeIndexPaths))
	for _, oneORBranch := range path.PartialAlternativeIndexPaths {
		clonedORBranch := make([][]*AccessPath, 0, len(oneORBranch))
		for _, oneAlternative := range oneORBranch {
			clonedOneAlternative := SliceDeepClone(oneAlternative)
			clonedORBranch = append(clonedORBranch, clonedOneAlternative)
		}
		ret.PartialAlternativeIndexPaths = append(ret.PartialAlternativeIndexPaths, clonedORBranch)
	}
	return ret
}

// IsTablePath returns true if it's IntHandlePath or CommonHandlePath. Including tiflash table scan.
func (path *AccessPath) IsTablePath() bool {
	return path.IsIntHandlePath || path.IsCommonHandlePath || (path.Index != nil && path.StoreType == kv.TiFlash)
}

// IsTiKVTablePath returns true if it's IntHandlePath or CommonHandlePath. And the store type is TiKV.
func (path *AccessPath) IsTiKVTablePath() bool {
	return (path.IsIntHandlePath || path.IsCommonHandlePath) && path.StoreType == kv.TiKV
}

// IsTiFlashSimpleTablePath returns true if it's a TiFlash path and will not use any special indexes like vector index.
func (path *AccessPath) IsTiFlashSimpleTablePath() bool {
	return path.StoreType == kv.TiFlash && path.Index == nil
}

// SplitCorColAccessCondFromFilters move the necessary filter in the form of index_col = corrlated_col to access conditions.
// The function consider the `idx_col_1 = const and index_col_2 = cor_col and index_col_3 = const` case.
// It enables more index columns to be considered. The range will be rebuilt in 'ResolveCorrelatedColumns'.
func (path *AccessPath) SplitCorColAccessCondFromFilters(ctx planctx.PlanContext, eqOrInCount int) (access, remained []expression.Expression) {
	// The plan cache do not support subquery now. So we skip this function when
	// 'MaybeOverOptimized4PlanCache' function return true .
	if expression.MaybeOverOptimized4PlanCache(ctx.GetExprCtx(), path.TableFilters...) {
		return nil, path.TableFilters
	}
	access = make([]expression.Expression, len(path.IdxCols)-eqOrInCount)
	used := make([]bool, len(path.TableFilters))
	usedCnt := 0
	for i := eqOrInCount; i < len(path.IdxCols); i++ {
		matched := false
		for j, filter := range path.TableFilters {
			if used[j] {
				continue
			}
			colEqConstant := isColEqConstant(filter, path.IdxCols[i])
			if i == eqOrInCount && colEqConstant {
				// If there is a col-eq-constant condition for path.IdxCols[eqOrInCount], it means that range fallback happens
				// in DetachCondAndBuildRangeForIndex. In this case we don't consider adding access conditions. Besides, the IF
				// branch also ensures that there must be some col-eq-corcol condition in access if len(access) > 0, which is
				// important. If there is no col-eq-corcol condition in access, we would not rebuild ranges, which brings the
				// correctness issue.
				return nil, path.TableFilters
			}
			colEqCorCol := isColEqCorCol(filter, path.IdxCols[i])
			if !colEqConstant && !colEqCorCol {
				continue
			}
			matched = true
			access[i-eqOrInCount] = filter
			if path.IdxColLens[i] == types.UnspecifiedLength {
				used[j] = true
				usedCnt++
			}
			break
		}
		if !matched {
			access = access[:i-eqOrInCount]
			break
		}
	}
	remained = make([]expression.Expression, 0, len(used)-usedCnt)
	for i, ok := range used {
		if !ok {
			remained = append(remained, path.TableFilters[i]) // nozero
		}
	}
	return access, remained
}

// isColEqConstant checks if the expression is eq function that one side is column and the other side is constant.
func isColEqConstant(expr expression.Expression, col *expression.Column) bool {
	isConstant := func(arg expression.Expression) bool {
		_, ok := arg.(*expression.Constant)
		return ok
	}
	return isColEqExpr(expr, col, isConstant)
}

// isColEqCorCol checks if the expression is eq function that one side is column and the other side is correlated column.
func isColEqCorCol(expr expression.Expression, col *expression.Column) bool {
	isCorCol := func(arg expression.Expression) bool {
		_, ok := arg.(*expression.CorrelatedColumn)
		return ok
	}
	return isColEqExpr(expr, col, isCorCol)
}

// isColEqExpr checks if the expression is eq function that one side is column and the other side passes checkFn.
func isColEqExpr(expr expression.Expression, col *expression.Column, checkFn func(expression.Expression) bool) bool {
	f, ok := expr.(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.EQ {
		return false
	}
	_, collation := f.CharsetAndCollation()
	if c, ok := f.GetArgs()[0].(*expression.Column); ok {
		if c.RetType.EvalType() == types.ETString && !collate.CompatibleCollate(collation, c.RetType.GetCollate()) {
			return false
		}
		if checkFn(f.GetArgs()[1]) {
			if col.EqualColumn(c) {
				return true
			}
		}
	}
	if c, ok := f.GetArgs()[1].(*expression.Column); ok {
		if c.RetType.EvalType() == types.ETString && !collate.CompatibleCollate(collation, c.RetType.GetCollate()) {
			return false
		}
		if checkFn(f.GetArgs()[0]) {
			if col.EqualColumn(c) {
				return true
			}
		}
	}
	return false
}

// OnlyPointRange checks whether each range is a point(no interval range exists).
func (path *AccessPath) OnlyPointRange(tc types.Context) bool {
	if path.IsIntHandlePath {
		for _, ran := range path.Ranges {
			if !ran.IsPointNullable(tc) {
				return false
			}
		}
		return true
	}
	for _, ran := range path.Ranges {
		// Not point or the not full matched.
		if !ran.IsPointNonNullable(tc) || len(ran.HighVal) != len(path.Index.Columns) {
			return false
		}
	}
	return true
}

// Col2Len maps expression.Column.UniqueID to column length
type Col2Len map[int64]int

// ExtractCol2Len collects index/table columns with lengths from expressions. If idxCols and idxColLens are not nil, it collects index columns with lengths(maybe prefix lengths).
// Otherwise it collects table columns with full lengths.
func ExtractCol2Len(ctx expression.EvalContext, exprs []expression.Expression, idxCols []*expression.Column, idxColLens []int) Col2Len {
	col2len := make(Col2Len, len(idxCols))
	for _, expr := range exprs {
		extractCol2LenFromExpr(ctx, expr, idxCols, idxColLens, col2len)
	}
	return col2len
}

func extractCol2LenFromExpr(ctx expression.EvalContext, expr expression.Expression, idxCols []*expression.Column, idxColLens []int, col2Len Col2Len) {
	switch v := expr.(type) {
	case *expression.Column:
		if idxCols == nil {
			col2Len[v.UniqueID] = types.UnspecifiedLength
		} else {
			for i, col := range idxCols {
				if col != nil && v.EqualByExprAndID(ctx, col) {
					col2Len[v.UniqueID] = idxColLens[i]
					break
				}
			}
		}
	case *expression.ScalarFunction:
		for _, arg := range v.GetArgs() {
			extractCol2LenFromExpr(ctx, arg, idxCols, idxColLens, col2Len)
		}
	}
}

// compareLength will compare the two column lengths. The return value:
// (1) -1 means that l is shorter than r;
// (2) 0 means that l equals to r;
// (3) 1 means that l is longer than r;
func compareLength(l, r int) int {
	if l == r {
		return 0
	}
	if l == types.UnspecifiedLength {
		return 1
	}
	if r == types.UnspecifiedLength {
		return -1
	}
	if l > r {
		return 1
	}
	return -1
}

// dominate return true if each column of c2 exists in c1 and c2's column length is no longer than c1's column length.
func (c1 Col2Len) dominate(c2 Col2Len) bool {
	if len(c2) > len(c1) {
		return false
	}
	for colID, len2 := range c2 {
		len1, ok := c1[colID]
		if !ok || compareLength(len2, len1) == 1 {
			return false
		}
	}
	return true
}

// CompareCol2Len will compare the two Col2Len maps. The last return value is used to indicate whether they are comparable.
// When the second return value is true, the first return value:
// (1) -1 means that c1 is worse than c2;
// (2) 0 means that c1 equals to c2;
// (3) 1 means that c1 is better than c2;
func CompareCol2Len(c1, c2 Col2Len) (int, bool) {
	l1, l2 := len(c1), len(c2)
	if l1 > l2 {
		if c1.dominate(c2) {
			return 1, true
		}
		return 0, false
	}
	if l1 < l2 {
		if c2.dominate(c1) {
			return -1, true
		}
		return 0, false
	}
	// If c1 and c2 have the same columns but have different lengths on some column, we regard c1 and c2 incomparable.
	for colID, colLen2 := range c2 {
		colLen1, ok := c1[colID]
		if !ok || colLen1 != colLen2 {
			return 0, false
		}
	}
	return 0, true
}

// GetCol2LenFromAccessConds returns columns with lengths from path.AccessConds.
func (path *AccessPath) GetCol2LenFromAccessConds(ctx planctx.PlanContext) Col2Len {
	if path.IsTablePath() {
		return ExtractCol2Len(ctx.GetExprCtx().GetEvalCtx(), path.AccessConds, nil, nil)
	}
	return ExtractCol2Len(ctx.GetExprCtx().GetEvalCtx(), path.AccessConds, path.IdxCols, path.IdxColLens)
}

// IsFullScanRange checks that a table scan does not have any filtering such that it can limit the range of
// the table scan.
func (path *AccessPath) IsFullScanRange(tableInfo *model.TableInfo) bool {
	var unsignedIntHandle bool
	if path.IsIntHandlePath && tableInfo.PKIsHandle {
		if pkColInfo := tableInfo.GetPkColInfo(); pkColInfo != nil {
			unsignedIntHandle = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
		}
	}
	if ranger.HasFullRange(path.Ranges, unsignedIntHandle) {
		return true
	}
	return false
}
