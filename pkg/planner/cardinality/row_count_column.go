// Copyright 2023 PingCAP, Inc.
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

package cardinality

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func init() {
	statistics.GetRowCountByColumnRanges = GetRowCountByColumnRanges
	statistics.GetRowCountByIndexRanges = GetRowCountByIndexRanges
}

// colEstimateCacheKey fully identifies a column estimate lookup, including the
// serialized ranges and row-count snapshot, so it can be used directly as a map
// key for O(1) lookup without a secondary linear scan or range cloning.
type colEstimateCacheKey struct {
	physicalID    int64
	colInfoID     int64
	pkIsHandle    bool
	realtimeCount int64
	modifyCount   int64
	rangesKey     string // serialized form of the range slice
}

// colEstimateCacheMap is the concrete type stored in StmtCtx.ColEstimateCache.
// Each distinct (column, ranges, row-count snapshot) tuple maps directly to its
// cached result, giving O(1) lookup and storage with no per-key linear scan.
type colEstimateCacheMap map[colEstimateCacheKey]statistics.RowEstimate

// buildColEstimateCacheKey constructs the full cache key for a column estimate.
// Ranges are serialized to a string so the key is comparable without storing or
// scanning the range slice.
func buildColEstimateCacheKey(physicalID, colInfoID int64, pkIsHandle bool, ranges []*ranger.Range, realtimeCount, modifyCount int64) colEstimateCacheKey {
	var b strings.Builder
	for i, r := range ranges {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(r.Redact(errors.RedactLogDisable))
	}
	return colEstimateCacheKey{
		physicalID:    physicalID,
		colInfoID:     colInfoID,
		pkIsHandle:    pkIsHandle,
		realtimeCount: realtimeCount,
		modifyCount:   modifyCount,
		rangesKey:     b.String(),
	}
}

// GetRowCountByColumnRanges estimates the row count by a slice of Range.
// PKIsHandle indicates whether the column is the single primary key column.
// Results from valid (non-pseudo) column stats are cached on the statement context
// so that subsequent calls with the same column and ranges — including calls from
// different plan candidates exploring the same physical table — can skip recomputation.
func GetRowCountByColumnRanges(sctx planctx.PlanContext, coll *statistics.HistColl, colUniqueID int64, colRanges []*ranger.Range, pkIsHandle bool) (result statistics.RowEstimate, err error) {
	sc := sctx.GetSessionVars().StmtCtx
	c := coll.GetCol(colUniqueID)
	colInfoID := colUniqueID
	if len(coll.UniqueID2colInfoID) > 0 {
		colInfoID = coll.UniqueID2colInfoID[colUniqueID]
	}
	recordUsedItemStatsStatus(sctx, c, coll.PhysicalID, colInfoID)
	if statistics.ColumnStatsIsInvalid(c, sctx, coll, colUniqueID) {
		// Do not cache pseudo/invalid results — they should not be reused
		// by index estimation paths that require real column stats.
		var pseudoResult float64
		if pkIsHandle {
			if len(colRanges) == 0 {
				return statistics.DefaultRowEst(0), nil
			}
			if colRanges[0].LowVal[0].Kind() == types.KindInt64 {
				pseudoResult = getPseudoRowCountBySignedIntRanges(colRanges, float64(coll.RealtimeCount))
			} else {
				pseudoResult = getPseudoRowCountByUnsignedIntRanges(colRanges, float64(coll.RealtimeCount))
			}
		} else {
			pseudoResult, err = getPseudoRowCountByColumnRanges(sc.TypeCtx(), float64(coll.RealtimeCount), colRanges, 0)
			if err != nil {
				return statistics.DefaultRowEst(0), err
			}
		}
		return statistics.DefaultRowEst(pseudoResult), nil
	}

	// Check the statement-scoped cache before computing.
	key := buildColEstimateCacheKey(coll.PhysicalID, colInfoID, pkIsHandle, colRanges, coll.RealtimeCount, coll.ModifyCount)
	cache, _ := sc.ColEstimateCache.(colEstimateCacheMap)
	if cache != nil {
		if cached, ok := cache[key]; ok {
			return cached, nil
		}
	}

	result, err = getColumnRowCount(sctx, c, colRanges, coll.RealtimeCount, coll.ModifyCount, pkIsHandle)
	if err != nil {
		return statistics.DefaultRowEst(0), errors.Trace(err)
	}

	// Store the result in the statement-scoped cache.
	if cache == nil {
		cache = make(colEstimateCacheMap)
		sc.ColEstimateCache = cache
	}
	cache[key] = result
	return result, nil
}

// tryColumnEstimateForSingleColRanges checks whether column statistics can be
// used instead of index statistics for the given single-column ranges. This is
// preferred over index histogram estimation because column histograms retain
// original data types, avoiding the lossy string encoding that index histograms
// use.
//
// Returns (result, true) if column stats are valid and the estimate was
// produced. Returns (zero, false) if column stats are unavailable, the ranges
// are not single-column, or the index uses a prefix length on the column, in
// which case the caller should fall back to index-based estimation.
func tryColumnEstimateForSingleColRanges(
	sctx planctx.PlanContext,
	coll *statistics.HistColl,
	idx *statistics.Index,
	indexRanges []*ranger.Range,
) (statistics.RowEstimate, bool) {
	if coll == nil || len(indexRanges) == 0 {
		return statistics.RowEstimate{}, false
	}
	// All ranges must be single-column.
	for _, r := range indexRanges {
		if len(r.LowVal) != 1 {
			return statistics.RowEstimate{}, false
		}
	}
	// Not applicable for prefix indexes — ranges are truncated to the prefix
	// length, so column-level ranges would not match.
	if idx.Info.Columns[0].Length != types.UnspecifiedLength {
		return statistics.RowEstimate{}, false
	}
	// Not applicable for partial indexes (ConditionExprString != "") — column
	// stats cover all rows, but a partial index only covers rows satisfying its
	// predicate, so the estimates would not match.
	// Not applicable for MV indexes — a single row can produce multiple index
	// entries, so column-level cardinality does not reflect index row counts.
	if idx.Info.ConditionExprString != "" || idx.Info.MVIndex {
		return statistics.RowEstimate{}, false
	}
	colIDs := coll.Idx2ColUniqueIDs[idx.Histogram.ID]
	if len(colIDs) == 0 {
		return statistics.RowEstimate{}, false
	}
	colID := colIDs[0]
	// Check column stats validity — do not use pseudo estimates here.
	c := coll.GetCol(colID)
	if statistics.ColumnStatsIsInvalid(c, sctx, coll, colID) {
		return statistics.RowEstimate{}, false
	}
	// For a single-column unique non-nullable index where all ranges are
	// non-null point probes, the index-based path returns exactly 1 per range,
	// which is more accurate than histogram estimation. Bail out so the caller
	// can use that path instead. Multi-column unique indexes are not eligible
	// because single-column ranges don't cover the full index, so the "exactly 1"
	// guarantee does not apply.
	if len(idx.Info.Columns) == 1 && idx.Info.Unique && mysql.HasNotNullFlag(c.Info.GetFlag()) {
		tc := sctx.GetSessionVars().StmtCtx.TypeCtx()
		allPoints := true
		for _, r := range indexRanges {
			if !r.IsPointNonNullable(tc) {
				allPoints = false
				break
			}
		}
		if allPoints {
			return statistics.RowEstimate{}, false
		}
	}
	// Compute or retrieve from cache.
	result, err := GetRowCountByColumnRanges(sctx, coll, colID, indexRanges, false)
	if err != nil {
		return statistics.RowEstimate{}, false
	}
	return result, true
}

// equalRowCountOnColumn estimates the row count by a slice of Range and a Datum.
func equalRowCountOnColumn(sctx planctx.PlanContext, c *statistics.Column, val types.Datum, encodedVal []byte, realtimeRowCount, modifyCount int64) (result statistics.RowEstimate, err error) {
	if val.IsNull() {
		return statistics.DefaultRowEst(float64(c.NullCount)), nil
	}
	if c.StatsVer < statistics.Version2 {
		// All the values are null.
		if c.Histogram.Bounds.NumRows() == 0 {
			return statistics.DefaultRowEst(0.0), nil
		}
		if c.Histogram.NDV > 0 && c.OutOfRange(val) {
			outOfRangeCnt := outOfRangeEQSelectivity(sctx, c.Histogram.NDV, realtimeRowCount, int64(c.TotalRowCount())) * c.TotalRowCount()
			return statistics.DefaultRowEst(outOfRangeCnt), nil
		}
		if c.CMSketch != nil {
			count, err := statistics.QueryValue(sctx, c.CMSketch, c.TopN, val)
			return statistics.DefaultRowEst(float64(count)), errors.Trace(err)
		}
		histRowCount, _ := c.Histogram.EqualRowCount(sctx, val, false)
		return statistics.DefaultRowEst(histRowCount), nil
	}

	// Stats version == 2
	// All the values are null.
	if c.Histogram.Bounds.NumRows() == 0 && c.TopN.Num() == 0 {
		return statistics.DefaultRowEst(0), nil
	}
	// 1. try to find this value in TopN
	if c.TopN != nil {
		rowcount, ok := c.TopN.QueryTopN(sctx, encodedVal)
		if ok {
			return statistics.DefaultRowEst(float64(rowcount)), nil
		}
	}
	// 2. try to find this value in bucket.Repeat(the last value in every bucket)
	histCnt, matched := c.Histogram.EqualRowCount(sctx, val, true)
	// Calculate histNDV here as it's needed for both the underrepresented check and later calculations
	histNDV := float64(c.Histogram.NDV - int64(c.TopN.Num()))
	// also check if this last bucket end value is underrepresented
	if matched && !IsLastBucketEndValueUnderrepresented(sctx,
		&c.Histogram, val, histCnt, histNDV, realtimeRowCount, modifyCount) {
		return statistics.DefaultRowEst(histCnt), nil
	}
	// 3. use uniform distribution assumption for the rest, and address special cases for out of range
	// or all values assumed to be contained within TopN.
	rowEstimate := estimateRowCountWithUniformDistribution(sctx, c, realtimeRowCount, modifyCount)
	return rowEstimate, nil
}

// getColumnRowCount estimates the row count by a slice of Range.
func getColumnRowCount(sctx planctx.PlanContext, c *statistics.Column, ranges []*ranger.Range, realtimeRowCount, modifyCount int64, pkIsHandle bool) (statistics.RowEstimate, error) {
	sc := sctx.GetSessionVars().StmtCtx
	var totalCount statistics.RowEstimate
	for _, rg := range ranges {
		highVal := *rg.HighVal[0].Clone()
		lowVal := *rg.LowVal[0].Clone()
		if highVal.Kind() == types.KindString {
			highVal.SetBytes(collate.GetCollator(highVal.Collation()).Key(highVal.GetString()))
		}
		if lowVal.Kind() == types.KindString {
			lowVal.SetBytes(collate.GetCollator(lowVal.Collation()).Key(lowVal.GetString()))
		}
		cmp, err := lowVal.Compare(sc.TypeCtx(), &highVal, collate.GetBinaryCollator())
		if err != nil {
			return statistics.DefaultRowEst(0), errors.Trace(err)
		}
		lowEncoded, err := codec.EncodeKey(sc.TimeZone(), nil, lowVal)
		err = sc.HandleError(err)
		if err != nil {
			return statistics.DefaultRowEst(0), err
		}
		highEncoded, err := codec.EncodeKey(sc.TimeZone(), nil, highVal)
		err = sc.HandleError(err)
		if err != nil {
			return statistics.DefaultRowEst(0), err
		}
		if cmp == 0 {
			// case 1: it's a point
			if !rg.LowExclude && !rg.HighExclude {
				// In this case, the row count is at most 1.
				if pkIsHandle {
					totalCount.AddAll(1)
					continue
				}
				var cnt statistics.RowEstimate
				cnt, err = equalRowCountOnColumn(sctx, c, lowVal, lowEncoded, realtimeRowCount, modifyCount)
				if err != nil {
					return statistics.DefaultRowEst(0), errors.Trace(err)
				}
				// If the current table row count has changed, we should scale the row count accordingly.
				cnt.MultiplyAll(c.GetIncreaseFactor(realtimeRowCount))
				totalCount.Add(cnt)
			}
			continue
		}
		// In stats ver 1, we use CM Sketch to estimate row count for point condition, which is more accurate.
		// So for the small range, we convert it to points.
		if c.StatsVer < 2 {
			rangeVals := statistics.EnumRangeValues(lowVal, highVal, rg.LowExclude, rg.HighExclude)

			// case 2: it's a small range && using ver1 stats
			if rangeVals != nil {
				for _, val := range rangeVals {
					cnt, err := equalRowCountOnColumn(sctx, c, val, lowEncoded, realtimeRowCount, modifyCount)
					if err != nil {
						return statistics.DefaultRowEst(0), err
					}
					// If the current table row count has changed, we should scale the row count accordingly.
					cnt.MultiplyAll(c.GetIncreaseFactor(realtimeRowCount))
					totalCount.Add(cnt)
				}

				continue
			}
		}

		// case 3: it's an interval
		cnt := betweenRowCountOnColumn(sctx, c, lowVal, highVal, lowEncoded, highEncoded)
		// `betweenRowCount` returns count for [l, h) range, we adjust cnt for boundaries here.
		// Note that, `cnt` does not include null values, we need specially handle cases
		//   where null is the lower bound.
		// And because we use (2, MaxValue] to represent expressions like a > 2 and use [MinNotNull, 3) to represent
		//   expressions like b < 3, we need to exclude the special values.
		if rg.LowExclude && !lowVal.IsNull() && lowVal.Kind() != types.KindMaxValue && lowVal.Kind() != types.KindMinNotNull {
			lowCnt, err := equalRowCountOnColumn(sctx, c, lowVal, lowEncoded, realtimeRowCount, modifyCount)
			if err != nil {
				return statistics.DefaultRowEst(0), errors.Trace(err)
			}
			cnt.Subtract(lowCnt)
			cnt.Clamp(0, c.NotNullCount())
		}
		if !rg.LowExclude && lowVal.IsNull() {
			cnt.AddAll(float64(c.NullCount))
		}
		if !rg.HighExclude && highVal.Kind() != types.KindMaxValue && highVal.Kind() != types.KindMinNotNull {
			highCnt, err := equalRowCountOnColumn(sctx, c, highVal, highEncoded, realtimeRowCount, modifyCount)
			if err != nil {
				return statistics.DefaultRowEst(0), errors.Trace(err)
			}
			cnt.Add(highCnt)
		}
		// Clamp all 3 fields of RowEstimate to [0, realtimeRowCount]
		cnt.Clamp(0, float64(realtimeRowCount))

		// If the current table row count has changed, we should scale the row count accordingly.
		increaseFactor := c.GetIncreaseFactor(realtimeRowCount)
		cnt.MultiplyAll(increaseFactor)

		// Calculate if the estimate already covers the full range of realtimeRowCount.
		// Use a tolerance factor to avoid precision issues.
		atFullRange := cnt.Est >= float64(realtimeRowCount)*(1-cost.ToleranceFactor)
		// handling the out-of-range part if the estimate does not cover the full range.
		if !atFullRange && ((c.OutOfRange(lowVal) && !lowVal.IsNull()) || c.OutOfRange(highVal)) {
			histNDV := c.NDV
			// Exclude the TopN
			if c.StatsVer == statistics.Version2 {
				histNDV -= int64(c.TopN.Num())
			}
			var count statistics.RowEstimate
			count.Add(c.Histogram.OutOfRangeRowCount(sctx, &lowVal, &highVal, realtimeRowCount, modifyCount, histNDV))
			cnt.Add(count)
		}

		totalCount.Add(cnt)
	}
	totalCount.Clamp(1.0, float64(realtimeRowCount))
	return totalCount, nil
}

// betweenRowCountOnColumn estimates the row count for interval [l, r).
func betweenRowCountOnColumn(sctx planctx.PlanContext, c *statistics.Column, l, r types.Datum, lowEncoded, highEncoded []byte) statistics.RowEstimate {
	// TODO: Track min/max range for column estimates, currently only used for indexes.
	histBetweenCnt := c.Histogram.BetweenRowCount(sctx, l, r)
	if c.StatsVer <= statistics.Version1 {
		return histBetweenCnt
	}
	topNCnt := float64(c.TopN.BetweenCount(sctx, lowEncoded, highEncoded))
	// Only add TopN count to the main estimate, keep min/max estimates from histogram
	histBetweenCnt.Est += topNCnt
	return histBetweenCnt
}

// getPseudoRowCountWithPartialStats calculates the row count if there are no statistics on the index, but there are column stats available.
func getPseudoRowCountWithPartialStats(sctx planctx.PlanContext, coll *statistics.HistColl, indexRanges []*ranger.Range,
	tableRowCount float64, idxCols []*expression.Column) (totalCount float64, maxCount float64, err error) {
	if tableRowCount == 0 {
		return 0, 0, nil
	}
	// If it is a single column index, directly use column estimation instead.
	if len(idxCols) == 1 {
		var countEst statistics.RowEstimate
		countEst, err = GetRowCountByColumnRanges(sctx, coll, idxCols[0].UniqueID, indexRanges, false)
		if err != nil {
			return 0, 0, err
		}
		return countEst.Est, 0, nil
	}
	tmpRan := []*ranger.Range{
		{
			LowVal:    make([]types.Datum, 1),
			HighVal:   make([]types.Datum, 1),
			Collators: make([]collate.Collator, 1),
		},
	}
	var (
		count float64
		colID int64
	)
	totalCount = float64(0)
	maxCount = float64(0)
	for _, indexRange := range indexRanges {
		selectivity := float64(1.0)
		corrSelectivity := float64(1.0)
		for i := range indexRange.LowVal {
			tmpRan[0].LowVal[0] = indexRange.LowVal[i]
			tmpRan[0].HighVal[0] = indexRange.HighVal[i]
			tmpRan[0].Collators[0] = indexRange.Collators[0]
			if i == len(indexRange.LowVal)-1 {
				tmpRan[0].LowExclude = indexRange.LowExclude
				tmpRan[0].HighExclude = indexRange.HighExclude
			}
			colID = idxCols[i].UniqueID
			// GetRowCountByColumnRanges handles invalid stats internally by using pseudo estimation
			var countEst statistics.RowEstimate
			countEst, err = GetRowCountByColumnRanges(sctx, coll, colID, tmpRan, false)
			if err != nil {
				return 0, 0, errors.Trace(err)
			}
			count = countEst.Est
			tempSelectivity := count / tableRowCount
			selectivity *= tempSelectivity
			corrSelectivity = min(corrSelectivity, tempSelectivity)
		}
		totalCount += selectivity * tableRowCount
		maxCount += corrSelectivity * tableRowCount
	}
	totalCount = mathutil.Clamp(totalCount, 1, tableRowCount)
	return totalCount, maxCount, nil
}
