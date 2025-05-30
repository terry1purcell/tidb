// Copyright 2017 PingCAP, Inc.
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

package expression

import (
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Vectorizable checks whether a list of expressions can employ vectorized execution.
func Vectorizable(exprs []Expression) bool {
	if slices.ContainsFunc(exprs, HasGetSetVarFunc) {
		return false
	}
	return checkSequenceFunction(exprs)
}

// checkSequenceFunction indicates whether the exprs can be evaluated as a vector.
// When two or more of this three(nextval, lastval, setval) exists in exprs list and one of them is nextval, it should be eval row by row.
func checkSequenceFunction(exprs []Expression) bool {
	var (
		nextval int
		lastval int
		setval  int
	)
	for _, expr := range exprs {
		scalaFunc, ok := expr.(*ScalarFunction)
		if !ok {
			continue
		}
		switch scalaFunc.FuncName.L {
		case ast.NextVal:
			nextval++
		case ast.LastVal:
			lastval++
		case ast.SetVal:
			setval++
		}
	}
	// case1: nextval && other sequence function.
	// case2: more than one nextval.
	if (nextval > 0 && (lastval > 0 || setval > 0)) || (nextval > 1) {
		return false
	}
	return true
}

// HasGetSetVarFunc checks whether an expression contains SetVar/GetVar function.
func HasGetSetVarFunc(expr Expression) bool {
	scalaFunc, ok := expr.(*ScalarFunction)
	if !ok {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar {
		return true
	}
	if scalaFunc.FuncName.L == ast.GetVar {
		return true
	}
	return slices.ContainsFunc(scalaFunc.GetArgs(), HasGetSetVarFunc)
}

// HasAssignSetVarFunc checks whether an expression contains SetVar function and assign a value
func HasAssignSetVarFunc(expr Expression) bool {
	scalaFunc, ok := expr.(*ScalarFunction)
	if !ok {
		return false
	}
	if scalaFunc.FuncName.L == ast.SetVar {
		for _, arg := range scalaFunc.GetArgs() {
			if _, ok := arg.(*ScalarFunction); ok {
				return true
			}
		}
	}
	return slices.ContainsFunc(scalaFunc.GetArgs(), HasAssignSetVarFunc)
}

// VectorizedExecute evaluates a list of expressions column by column and append their results to "output" Chunk.
func VectorizedExecute(ctx EvalContext, exprs []Expression, iterator *chunk.Iterator4Chunk, output *chunk.Chunk) error {
	for colID, expr := range exprs {
		err := evalOneColumn(ctx, expr, iterator, output, colID)
		if err != nil {
			return err
		}
	}
	return nil
}

func evalOneVec(ctx EvalContext, expr Expression, input *chunk.Chunk, output *chunk.Chunk, colIdx int) error {
	ft := expr.GetType(ctx)
	result := output.Column(colIdx)
	switch ft.EvalType() {
	case types.ETInt:
		if err := expr.VecEvalInt(ctx, input, result); err != nil {
			return err
		}
		if ft.GetType() == mysql.TypeBit {
			i64s := result.Int64s()
			buf := chunk.NewColumn(ft, input.NumRows())
			buf.ReserveBytes(input.NumRows())
			byteSize := (ft.GetFlen() + 7) >> 3
			for i := range i64s {
				if result.IsNull(i) {
					buf.AppendNull()
				} else {
					buf.AppendBytes(types.NewBinaryLiteralFromUint(uint64(i64s[i]), byteSize))
				}
			}
			// TODO: recycle all old Columns returned here.
			output.SetCol(colIdx, buf)
		} // else if mysql.HasUnsignedFlag(ft.flag) {
		// the underlying memory formats of int64 and uint64 are the same in Golang,
		// so we can do a no-op here.
		// }
	case types.ETReal:
		if err := expr.VecEvalReal(ctx, input, result); err != nil {
			return err
		}
		if ft.GetType() == mysql.TypeFloat {
			f64s := result.Float64s()
			n := input.NumRows()
			buf := chunk.NewColumn(ft, n)
			buf.ResizeFloat32(n, false)
			f32s := buf.Float32s()
			for i := range f64s {
				if result.IsNull(i) {
					buf.SetNull(i, true)
				} else {
					f32s[i] = float32(f64s[i])
				}
			}
			output.SetCol(colIdx, buf)
		}
	case types.ETDecimal:
		return expr.VecEvalDecimal(ctx, input, result)
	case types.ETDatetime, types.ETTimestamp:
		return expr.VecEvalTime(ctx, input, result)
	case types.ETDuration:
		return expr.VecEvalDuration(ctx, input, result)
	case types.ETJson:
		return expr.VecEvalJSON(ctx, input, result)
	case types.ETVectorFloat32:
		return expr.VecEvalVectorFloat32(ctx, input, result)
	case types.ETString:
		if err := expr.VecEvalString(ctx, input, result); err != nil {
			return err
		}
		if ft.GetType() == mysql.TypeEnum {
			n := input.NumRows()
			buf := chunk.NewColumn(ft, n)
			buf.ReserveEnum(n)
			for i := range n {
				if result.IsNull(i) {
					buf.AppendNull()
				} else {
					enum, err := types.ParseEnumName(ft.GetElems(), result.GetString(i), ft.GetCollate())
					if err != nil {
						logutil.BgLogger().Debug("Wrong enum name parsed during evaluation",
							zap.String("The name to be parsed in the ENUM", result.GetString(i)),
							zap.Strings("The valid names in the ENUM", ft.GetElems()),
							zap.Error(err),
						)
					}
					buf.AppendEnum(enum)
				}
			}
			output.SetCol(colIdx, buf)
		} else if ft.GetType() == mysql.TypeSet {
			n := input.NumRows()
			buf := chunk.NewColumn(ft, n)
			buf.ReserveSet(n)
			for i := range n {
				if result.IsNull(i) {
					buf.AppendNull()
				} else {
					set, err := types.ParseSetName(ft.GetElems(), result.GetString(i), ft.GetCollate())
					if err != nil {
						logutil.BgLogger().Debug("Wrong set name parsed during evaluation",
							zap.String("The name to be parsed in the SET", result.GetString(i)),
							zap.Strings("The valid names in the SET", ft.GetElems()),
							zap.Error(err),
						)
					}
					buf.AppendSet(set)
				}
			}
			output.SetCol(colIdx, buf)
		}
	default:
		return errors.Errorf("unsupported type %s during evaluation", ft.EvalType())
	}
	return nil
}

func evalOneColumn(ctx EvalContext, expr Expression, iterator *chunk.Iterator4Chunk, output *chunk.Chunk, colID int) (err error) {
	switch fieldType, evalType := expr.GetType(ctx), expr.GetType(ctx).EvalType(); evalType {
	case types.ETInt:
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			err = executeToInt(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETReal:
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			err = executeToReal(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETDecimal:
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			err = executeToDecimal(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETDatetime, types.ETTimestamp:
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			err = executeToDatetime(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETDuration:
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			err = executeToDuration(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETJson:
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			err = executeToJSON(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETVectorFloat32:
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			err = executeToVectorFloat32(ctx, expr, fieldType, row, output, colID)
		}
	case types.ETString:
		for row := iterator.Begin(); err == nil && row != iterator.End(); row = iterator.Next() {
			err = executeToString(ctx, expr, fieldType, row, output, colID)
		}
	default:
		return errors.Errorf("unsupported type %s during evaluation", evalType)
	}
	return err
}

func evalOneCell(ctx EvalContext, expr Expression, row chunk.Row, output *chunk.Chunk, colID int) (err error) {
	switch fieldType, evalType := expr.GetType(ctx), expr.GetType(ctx).EvalType(); evalType {
	case types.ETInt:
		err = executeToInt(ctx, expr, fieldType, row, output, colID)
	case types.ETReal:
		err = executeToReal(ctx, expr, fieldType, row, output, colID)
	case types.ETDecimal:
		err = executeToDecimal(ctx, expr, fieldType, row, output, colID)
	case types.ETDatetime, types.ETTimestamp:
		err = executeToDatetime(ctx, expr, fieldType, row, output, colID)
	case types.ETDuration:
		err = executeToDuration(ctx, expr, fieldType, row, output, colID)
	case types.ETJson:
		err = executeToJSON(ctx, expr, fieldType, row, output, colID)
	case types.ETVectorFloat32:
		err = executeToVectorFloat32(ctx, expr, fieldType, row, output, colID)
	case types.ETString:
		err = executeToString(ctx, expr, fieldType, row, output, colID)
	default:
		return errors.Errorf("unsupported type %s during evaluation", evalType)
	}
	return err
}

func executeToInt(ctx EvalContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalInt(ctx, row)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(colID)
		return nil
	}
	if fieldType.GetType() == mysql.TypeBit {
		byteSize := (fieldType.GetFlen() + 7) >> 3
		output.AppendBytes(colID, types.NewBinaryLiteralFromUint(uint64(res), byteSize))
		return nil
	}
	if fieldType.GetType() == mysql.TypeEnum {
		e, err := types.ParseEnumValue(fieldType.GetElems(), uint64(res))
		if err != nil {
			return err
		}
		output.AppendEnum(colID, e)
		return nil
	}
	if mysql.HasUnsignedFlag(fieldType.GetFlag()) {
		output.AppendUint64(colID, uint64(res))
		return nil
	}
	output.AppendInt64(colID, res)
	return nil
}

func executeToReal(ctx EvalContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalReal(ctx, row)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(colID)
		return nil
	}
	if fieldType.GetType() == mysql.TypeFloat {
		output.AppendFloat32(colID, float32(res))
		return nil
	}
	output.AppendFloat64(colID, res)
	return nil
}

func executeToDecimal(ctx EvalContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalDecimal(ctx, row)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(colID)
		return nil
	}
	output.AppendMyDecimal(colID, res)
	return nil
}

func executeToDatetime(ctx EvalContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalTime(ctx, row)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(colID)
	} else {
		output.AppendTime(colID, res)
	}
	return nil
}

func executeToDuration(ctx EvalContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalDuration(ctx, row)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(colID)
	} else {
		output.AppendDuration(colID, res)
	}
	return nil
}

func executeToJSON(ctx EvalContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalJSON(ctx, row)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(colID)
	} else {
		output.AppendJSON(colID, res)
	}
	return nil
}

func executeToVectorFloat32(ctx EvalContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalVectorFloat32(ctx, row)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(colID)
	} else {
		output.AppendVectorFloat32(colID, res)
	}
	return nil
}

func executeToString(ctx EvalContext, expr Expression, fieldType *types.FieldType, row chunk.Row, output *chunk.Chunk, colID int) error {
	res, isNull, err := expr.EvalString(ctx, row)
	if err != nil {
		return err
	}
	if isNull {
		output.AppendNull(colID)
	} else if fieldType.GetType() == mysql.TypeEnum {
		val := types.Enum{Value: uint64(0), Name: res}
		output.AppendEnum(colID, val)
	} else if fieldType.GetType() == mysql.TypeSet {
		val := types.Set{Value: uint64(0), Name: res}
		output.AppendSet(colID, val)
	} else {
		output.AppendString(colID, res)
	}
	return nil
}

// VectorizedFilter applies a list of filters to a Chunk and
// returns a bool slice, which indicates whether a row is passed the filters.
// Filters is executed vectorized.
func VectorizedFilter(ctx EvalContext, vecEnabled bool, filters []Expression, iterator *chunk.Iterator4Chunk, selected []bool) (_ []bool, err error) {
	selected, _, err = VectorizedFilterConsiderNull(ctx, vecEnabled, filters, iterator, selected, nil)
	return selected, err
}

// VectorizedFilterConsiderNull applies a list of filters to a Chunk and
// returns two bool slices, `selected` indicates whether a row passed the
// filters, `isNull` indicates whether the result of the filter is null.
// Filters is executed vectorized.
func VectorizedFilterConsiderNull(ctx EvalContext, vecEnabled bool, filters []Expression, iterator *chunk.Iterator4Chunk, selected []bool, isNull []bool) ([]bool, []bool, error) {
	// canVectorized used to check whether all of the filters can be vectorized evaluated
	canVectorized := true
	for _, filter := range filters {
		if !filter.Vectorized() {
			canVectorized = false
			break
		}
	}

	input := iterator.GetChunk()
	sel := input.Sel()
	var err error
	if canVectorized && vecEnabled {
		selected, isNull, err = vectorizedFilter(ctx, vecEnabled, filters, iterator, selected, isNull)
	} else {
		selected, isNull, err = rowBasedFilter(ctx, filters, iterator, selected, isNull)
	}
	if err != nil || sel == nil {
		return selected, isNull, err
	}

	// When the input.Sel() != nil, we need to handle the selected slice and input.Sel()
	// Get the index which is not appeared in input.Sel() and set the selected[index] = false
	selectedLength := len(selected)
	unselected := allocZeroSlice(selectedLength)
	defer deallocateZeroSlice(unselected)
	// unselected[i] == 1 means that the i-th row is not selected
	for i := range selectedLength {
		unselected[i] = 1
	}
	for _, ind := range sel {
		unselected[ind] = 0
	}
	for i := range selectedLength {
		if selected[i] && unselected[i] == 1 {
			selected[i] = false
		}
	}
	return selected, isNull, err
}

// rowBasedFilter filters by row.
func rowBasedFilter(ctx EvalContext, filters []Expression, iterator *chunk.Iterator4Chunk, selected []bool, isNull []bool) ([]bool, []bool, error) {
	// If input.Sel() != nil, we will call input.SetSel(nil) to clear the sel slice in input chunk.
	// After the function finished, then we reset the sel in input chunk.
	// Then the caller will handle the input.sel and selected slices.
	input := iterator.GetChunk()
	if input.Sel() != nil {
		defer input.SetSel(input.Sel())
		input.SetSel(nil)
		iterator = chunk.NewIterator4Chunk(input)
	}

	selected = selected[:0]
	for i, numRows := 0, iterator.Len(); i < numRows; i++ {
		selected = append(selected, true)
	}
	if isNull != nil {
		isNull = isNull[:0]
		for i, numRows := 0, iterator.Len(); i < numRows; i++ {
			isNull = append(isNull, false)
		}
	}
	var (
		filterResult       int64
		bVal, isNullResult bool
		err                error
	)
	for _, filter := range filters {
		isIntType := true
		if filter.GetType(ctx).EvalType() != types.ETInt {
			isIntType = false
		}
		for row := iterator.Begin(); row != iterator.End(); row = iterator.Next() {
			if !selected[row.Idx()] {
				continue
			}
			if isIntType {
				filterResult, isNullResult, err = filter.EvalInt(ctx, row)
				if err != nil {
					return nil, nil, err
				}
				selected[row.Idx()] = selected[row.Idx()] && !isNullResult && (filterResult != 0)
			} else {
				// TODO: should rewrite the filter to `cast(expr as SIGNED) != 0` and always use `EvalInt`.
				bVal, isNullResult, err = EvalBool(ctx, []Expression{filter}, row)
				if err != nil {
					return nil, nil, err
				}
				selected[row.Idx()] = selected[row.Idx()] && bVal
			}
			if isNull != nil {
				isNull[row.Idx()] = isNull[row.Idx()] || isNullResult
			}
		}
	}
	return selected, isNull, nil
}

// vectorizedFilter filters by vector.
func vectorizedFilter(ctx EvalContext, vecEnabled bool, filters []Expression, iterator *chunk.Iterator4Chunk, selected []bool, isNull []bool) ([]bool, []bool, error) {
	selected, isNull, err := VecEvalBool(ctx, vecEnabled, filters, iterator.GetChunk(), selected, isNull)
	if err != nil {
		return nil, nil, err
	}

	return selected, isNull, nil
}
