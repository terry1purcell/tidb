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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// convertMatchAgainstToLike converts a MATCH...AGAINST expression to ILIKE
// predicates. It is a thin wrapper around expression.BuildFTSToILikeExpression;
// the conversion logic lives in pkg/expression so the same translation can be
// shared with cardinality-based selectivity estimation (which substitutes the
// equivalent ILIKE form for the opaque FTSMysqlMatchAgainst builtin).
//
// This is a fallback rewrite since TiDB does not natively support full-text
// search outside the TiFlash FTS path. The planner only invokes it in
// predicate clauses (WHERE / HAVING / JOIN ON) — scoring contexts
// (SELECT field list, ORDER BY) keep the native FTSMysqlMatchAgainst builtin
// so the result is a float relevance score rather than 0/1, even though the
// native path then requires TiFlash at execution time. The semantic
// differences below therefore apply to predicate use only:
//
//  1. No relevance scoring — the synthesized ILIKE predicate produces a 0/1
//     boolean filter result, which is the only thing a WHERE/HAVING/JOIN ON
//     clause consumes. Relevance ranking (ORDER BY MATCH(...) DESC) and
//     scalar SELECT MATCH(...) are intentionally NOT routed through this
//     fallback for that reason; substituting 0/1 there would silently
//     corrupt the sort or the projected score.
//  2. No stop word filtering - searches for all words regardless of length or commonness
//  3. No word length limits - MySQL ignores words shorter than ft_min_word_len (default 4)
//  4. No word boundaries - LIKE %term% matches substrings anywhere, not just complete words
//     - Simple terms: "cat" matches "concatenate", "category", "application"
//     (MySQL FTS only matches "cat" as a standalone word)
//     - Prefix wildcard: "Optim*" matches "reOptimizing", "Optimizing"
//     (MySQL FTS only matches words starting with "Optim" like "Optimizing", not "reOptimizing")
//     - Phrase matching: "quick brown" matches "aquick brownie"
//     (MySQL FTS only matches the exact phrase with word boundaries)
//     This limitation exists because LIKE cannot enforce word boundaries without REGEXP
//
// 5. Performance - LIKE predicates cannot use full-text indexes (much slower on large datasets)
//
// Supported Boolean mode operators: + (required), - (excluded), * (prefix wildcard), "..." (phrase)
// Partially supported: ~ (treated as optional, ranking effect ignored), > < (treated as optional)
// Unsupported: WITH QUERY EXPANSION (returns an error), () sub-expression grouping (stripped)
func (er *expressionRewriter) convertMatchAgainstToLike(
	columns []expression.Expression,
	searchText string,
	modifier ast.FulltextSearchModifier,
) (expression.Expression, error) {
	return expression.BuildFTSToILikeExpression(er.sctx, columns, searchText, modifier)
}
