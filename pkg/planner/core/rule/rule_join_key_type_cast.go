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

package rule

import (
	"context"
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// JoinKeyTypeCastRewriter rewrites implicit type cast predicates to enable
// index usage.
//
// When a query contains WHERE varchar_col = 123, type coercion wraps the
// column in CAST(varchar_col AS DOUBLE), which prevents the ranger from
// building index ranges. This rule detects the pattern and adds first-
// character prefix predicates (OR of LIKE patterns) that the ranger CAN
// use for index range building. The original CAST equality remains as a
// filter for correctness.
//
// For integer 123, the valid first characters of strings that CAST to
// 123.0 as DOUBLE are: whitespace (trimmed by TrimSpace), '+', '.',
// '0' (leading zero), and '1' (first significant digit). The rule adds:
//
//	(col LIKE ' %' OR col LIKE '\t%' OR ... OR col LIKE '1%')
//
// The ranger converts each LIKE to an index range. The original
// CAST(varchar_col AS DOUBLE) = 123.0 stays as a table filter to remove
// false positives, guaranteeing correctness.
//
// This rule also handles join key type cast rewriting (future).
type JoinKeyTypeCastRewriter struct{}

// Optimize implements base.LogicalOptRule.
func (*JoinKeyTypeCastRewriter) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	changed := rewriteImplicitCasts(p)
	return p, changed, nil
}

// Name implements base.LogicalOptRule.
func (*JoinKeyTypeCastRewriter) Name() string {
	return "join_key_type_cast"
}

// rewriteImplicitCasts recursively walks the plan tree.
func rewriteImplicitCasts(p base.LogicalPlan) bool {
	changed := false
	for _, child := range p.Children() {
		if rewriteImplicitCasts(child) {
			changed = true
		}
	}

	if ds, ok := p.(*logicalop.DataSource); ok {
		if rewriteDataSourceCastPredicates(ds) {
			changed = true
		}
	}

	// TODO: handle LogicalJoin for join key type cast rewriting.

	return changed
}

// rewriteDataSourceCastPredicates scans DataSource.PushedDownConds for
// CAST(varchar_col AS DOUBLE) = int_const patterns and adds OR-of-LIKE
// prefix predicates that the ranger can use for index range building.
func rewriteDataSourceCastPredicates(ds *logicalop.DataSource) bool {
	if len(ds.PushedDownConds) == 0 {
		return false
	}

	ctx := ds.SCtx()
	var newConds []expression.Expression

	for _, cond := range ds.PushedDownConds {
		col, intVal, ok := extractCastEqPattern(ctx, cond)
		if !ok {
			continue
		}
		likePred := buildPrefixLikePredicate(ctx, col, intVal)
		if likePred != nil {
			newConds = append(newConds, likePred)
		}
	}

	if len(newConds) == 0 {
		return false
	}

	ds.PushedDownConds = append(ds.PushedDownConds, newConds...)
	ds.AllConds = append(ds.AllConds, newConds...)
	return true
}

// extractCastEqPattern detects the pattern EQ(CAST(varchar_col AS DOUBLE), const)
// where the constant is an exact integer value. Returns the original column
// and integer value if matched.
func extractCastEqPattern(sctx base.PlanContext, cond expression.Expression) (*expression.Column, int64, bool) {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		return nil, 0, false
	}
	args := sf.GetArgs()
	if len(args) != 2 {
		return nil, 0, false
	}

	evalCtx := sctx.GetExprCtx().GetEvalCtx()

	// Try both orderings: CAST(col) = const and const = CAST(col).
	for i := 0; i < 2; i++ {
		castArg, constArg := args[i], args[1-i]

		castSF, ok1 := castArg.(*expression.ScalarFunction)
		constExpr, ok2 := constArg.(*expression.Constant)
		if !ok1 || !ok2 {
			continue
		}

		// The CAST must target DOUBLE.
		if castSF.FuncName.L != ast.Cast {
			continue
		}
		if castSF.GetType(evalCtx).EvalType() != types.ETReal {
			continue
		}

		// Inner of CAST must be a Column with string type.
		castArgs := castSF.GetArgs()
		if len(castArgs) != 1 {
			continue
		}
		col, ok3 := castArgs[0].(*expression.Column)
		if !ok3 || !col.GetType(evalCtx).EvalType().IsStringKind() {
			continue
		}

		// Constant must be DOUBLE with an exact integer value.
		if constExpr.GetType(evalCtx).EvalType() != types.ETReal {
			continue
		}
		val, isNull, err := constExpr.EvalReal(evalCtx, chunk.Row{})
		if err != nil || isNull {
			continue
		}
		if math.IsInf(val, 0) || math.IsNaN(val) {
			continue
		}
		if math.Floor(val) != val {
			continue
		}
		if val > float64(math.MaxInt64) || val < float64(math.MinInt64) {
			continue
		}
		// Beyond 2^53, DOUBLE cannot exactly represent every integer.
		// Multiple distinct integers map to the same DOUBLE value, and
		// their string representations may start with different digits,
		// so our first-character prefix ranges would be incomplete.
		const maxExactInt = 1 << 53
		if val > maxExactInt || val < -maxExactInt {
			continue
		}
		return col, int64(val), true
	}
	return nil, 0, false
}

// validFirstChars returns the set of byte values that can appear as the
// first character of a string whose CAST to DOUBLE equals intVal.
//
// TiDB's StrToFloat (pkg/types/convert.go) calls strings.TrimSpace first,
// then getValidFloatPrefix accepts: +, -, ., 0-9 as valid first characters.
func validFirstChars(intVal int64) []byte {
	// All characters stripped by strings.TrimSpace that could precede
	// the numeric content in the stored string value.
	wsChars := []byte{' ', '\t', '\n', '\r', '\x0b', '\x0c'}

	switch {
	case intVal > 0:
		d := firstSignificantDigit(intVal)
		return append(wsChars, '+', '.', '0', d)
	case intVal < 0:
		return append(wsChars, '-')
	default: // intVal == 0
		return append(wsChars, '+', '-', '.', '0')
	}
}

// firstSignificantDigit returns the leading digit of |n| as a byte.
// For example, 123 → '1', 9 → '9', 50 → '5'.
func firstSignificantDigit(n int64) byte {
	if n < 0 {
		n = -n
	}
	for n >= 10 {
		n /= 10
	}
	return byte('0' + n)
}

// buildPrefixLikePredicate constructs an OR of LIKE 'char%' predicates for
// each valid first character, using col as the match target.
func buildPrefixLikePredicate(sctx base.PlanContext, col *expression.Column, intVal int64) expression.Expression {
	chars := validFirstChars(intVal)
	if len(chars) == 0 {
		return nil
	}

	exprCtx := sctx.GetExprCtx()
	evalCtx := exprCtx.GetEvalCtx()
	retType := types.NewFieldType(mysql.TypeLonglong)
	escapeConst := &expression.Constant{
		Value:   types.NewIntDatum(int64('\\')),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	likes := make([]expression.Expression, 0, len(chars))
	for _, ch := range chars {
		pattern := string([]byte{ch, '%'})
		patternConst := &expression.Constant{
			Value:   types.NewStringDatum(pattern),
			RetType: col.GetType(evalCtx).Clone(),
		}
		like := expression.NewFunctionInternal(exprCtx, ast.Like, retType, col.Clone(), patternConst, escapeConst)
		if like != nil {
			likes = append(likes, like)
		}
	}

	if len(likes) == 0 {
		return nil
	}

	// Chain with OR: likes[0] OR likes[1] OR ... OR likes[n-1].
	result := likes[0]
	for i := 1; i < len(likes); i++ {
		result = expression.NewFunctionInternal(exprCtx, ast.LogicOr, retType, result, likes[i])
	}
	return result
}
