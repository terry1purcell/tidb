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

package expression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/vitess"
)

func (b *builtinInetNtoaSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf.Int64s()
	ip := make(net.IP, net.IPv4len)
	for i := range n {
		val := i64s[i]
		if buf.IsNull(i) || val < 0 || uint64(val) > math.MaxUint32 {
			result.AppendNull()
			continue
		}
		binary.BigEndian.PutUint32(ip, uint32(val))
		ipv4 := ip.To4()
		if ipv4 == nil {
			// Not a valid ipv4 address.
			result.AppendNull()
			continue
		}
		result.AppendString(ipv4.String())
	}
	return nil
}

func (b *builtinInetNtoaSig) vectorized() bool {
	return true
}

func (b *builtinIsIPv4Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if buf.IsNull(i) {
			continue
		}
		if isIPv4(buf.GetString(i)) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinIsIPv4Sig) vectorized() bool {
	return true
}
func (b *builtinJSONAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinJSONAnyValueSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalJSON(ctx, input, result)
}

func (b *builtinRealAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinRealAnyValueSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalReal(ctx, input, result)
}

func (b *builtinStringAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinStringAnyValueSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalString(ctx, input, result)
}

func (b *builtinIsIPv6Sig) vectorized() bool {
	return true
}

func (b *builtinIsIPv6Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if buf.IsNull(i) {
			continue
		}
		ipStr := buf.GetString(i)
		if ip := net.ParseIP(ipStr); ip != nil && !isIPv4(ipStr) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinIsUUIDSig) vectorized() bool {
	return true
}

func (b *builtinIsUUIDSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	result.MergeNulls(buf)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		val := buf.GetString(i)
		// MySQL's IS_UUID is strict and doesn't trim spaces, unlike Go's uuid.Parse
		// We need to check if the string has leading/trailing spaces before parsing
		if strings.TrimSpace(val) != val {
			i64s[i] = 0
		} else if _, err = uuid.Parse(val); err != nil {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinNameConstStringSig) vectorized() bool {
	return true
}

func (b *builtinNameConstStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalString(ctx, input, result)
}

func (b *builtinDecimalAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinDecimalAnyValueSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalDecimal(ctx, input, result)
}

func (b *builtinUUIDSig) vectorized() bool {
	return true
}

func (b *builtinUUIDSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)
	var id uuid.UUID
	var err error
	for range n {
		id, err = uuid.NewUUID()
		if err != nil {
			return err
		}
		result.AppendString(id.String())
	}
	return nil
}

func (b *builtinNameConstDurationSig) vectorized() bool {
	return true
}

func (b *builtinNameConstDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalDuration(ctx, input, result)
}

func (b *builtinDurationAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinDurationAnyValueSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalDuration(ctx, input, result)
}

func (b *builtinIntAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinIntAnyValueSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalInt(ctx, input, result)
}

func (b *builtinIsIPv4CompatSig) vectorized() bool {
	return true
}

func (b *builtinIsIPv4CompatSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	prefixCompat := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	for i := range n {
		if buf.IsNull(i) {
			continue
		}
		// Note that the input should be IP address in byte format.
		// For IPv4, it should be byte slice with 4 bytes.
		// For IPv6, it should be byte slice with 16 bytes.
		// See example https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4-compat
		ipAddress := buf.GetBytes(i)
		if len(ipAddress) != net.IPv6len || !bytes.HasPrefix(ipAddress, prefixCompat) {
			// Not an IPv6 address, return false
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinNameConstIntSig) vectorized() bool {
	return true
}

func (b *builtinNameConstIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalInt(ctx, input, result)
}

func (b *builtinNameConstTimeSig) vectorized() bool {
	return true
}

func (b *builtinNameConstTimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalTime(ctx, input, result)
}

func (b *builtinSleepSig) vectorized() bool {
	return true
}

// vecEvalInt evals a builtinSleepSig in a vectorized manner.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_sleep
func (b *builtinSleepSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	err = b.args[0].VecEvalReal(ctx, input, buf)
	if err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	i64s := result.Int64s()

	ec := errCtx(ctx)
	for i := range n {
		isNull := buf.IsNull(i)
		val := buf.GetFloat64(i)

		if isNull || val < 0 {
			// for insert ignore stmt, the StrictSQLMode and ignoreErr should both be considered.
			err := ec.HandleErrorWithAlias(errBadNull,
				errIncorrectArgs.GenWithStackByArgs("sleep"),
				errIncorrectArgs.FastGenByArgs("sleep"),
			)
			if err != nil {
				return err
			}
			continue
		}

		if val > math.MaxFloat64/float64(time.Second.Nanoseconds()) {
			return errIncorrectArgs.GenWithStackByArgs("sleep")
		}

		if isKilled := doSleep(val, vars); isKilled {
			for j := i; j < n; j++ {
				i64s[j] = 1
			}
			return nil
		}
	}

	return nil
}

func doSleep(secs float64, sessVars *variable.SessionVars) (isKilled bool) {
	if secs <= 0.0 {
		return false
	}
	dur := time.Duration(secs * float64(time.Second.Nanoseconds()))
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(dur)
	for {
		select {
		case <-ticker.C:
			// MySQL 8.0 sleep: https://dev.mysql.com/doc/refman/8.0/en/miscellaneous-functions.html#function_sleep
			// Regular kill or Killed because of max execution time
			if err := sessVars.SQLKiller.HandleSignal(); err != nil {
				if len(sessVars.StmtCtx.TableIDs) == 0 {
					sessVars.SQLKiller.Reset()
				}
				timer.Stop()
				return true
			}
		case <-timer.C:
			return false
		}
	}
}

func (b *builtinIsIPv4MappedSig) vectorized() bool {
	return true
}

func (b *builtinIsIPv4MappedSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	prefixMapped := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff}
	for i := range n {
		if buf.IsNull(i) {
			continue
		}
		// Note that the input should be IP address in byte format.
		// For IPv4, it should be byte slice with 4 bytes.
		// For IPv6, it should be byte slice with 16 bytes.
		// See example https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_is-ipv4-mapped
		ipAddress := buf.GetBytes(i)
		if len(ipAddress) != net.IPv6len || !bytes.HasPrefix(ipAddress, prefixMapped) {
			// Not an IPv6 address, return false
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinNameConstDecimalSig) vectorized() bool {
	return true
}

func (b *builtinNameConstDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalDecimal(ctx, input, result)
}

func (b *builtinNameConstJSONSig) vectorized() bool {
	return true
}

func (b *builtinNameConstJSONSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalJSON(ctx, input, result)
}

func (b *builtinInet6AtonSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinInet6AtonSig.
// See https://dev.mysql.com/doc/refman/5.7/en/miscellaneous-functions.html#function_inet6-aton
func (b *builtinInet6AtonSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	var (
		resv4 []byte
		resv6 []byte
		res   []byte
	)
	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		val := buf.GetString(i)
		if len(val) == 0 {
			result.AppendNull()
			continue
		}
		ip := net.ParseIP(val)
		if ip == nil {
			result.AppendNull()
			continue
		}
		var isMappedIpv6 bool
		ipTo4 := ip.To4()
		if ipTo4 != nil && strings.Contains(val, ":") {
			// mapped ipv6 address.
			isMappedIpv6 = true
		}

		if isMappedIpv6 || ipTo4 == nil {
			if resv6 == nil {
				resv6 = make([]byte, net.IPv6len)
			}
			res = resv6
		} else {
			if resv4 == nil {
				resv4 = make([]byte, net.IPv4len)
			}
			res = resv4
		}

		if isMappedIpv6 {
			copy(res[12:], ipTo4)
			res[11] = 0xff
			res[10] = 0xff
		} else if ipTo4 == nil {
			copy(res, ip.To16())
		} else {
			copy(res, ipTo4)
		}
		result.AppendBytes(res)
	}
	return nil
}

func (b *builtinTimeAnyValueSig) vectorized() bool {
	return true
}

func (b *builtinTimeAnyValueSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalTime(ctx, input, result)
}

func (b *builtinInetAtonSig) vectorized() bool {
	return true
}

func (b *builtinInetAtonSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	var (
		byteResult, res uint64
		dotCount        int
	)
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	result.MergeNulls(buf)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		ipAddr := buf.GetString(i)
		if len(ipAddr) == 0 || ipAddr[len(ipAddr)-1] == '.' {
			// ip address should not end with '.'.
			result.SetNull(i, true)
			continue
		}
		// reset
		byteResult = 0
		res = 0
		dotCount = 0
		for _, c := range ipAddr {
			if c >= '0' && c <= '9' {
				digit := uint64(c - '0')
				byteResult = byteResult*10 + digit
				if byteResult > 255 {
					result.SetNull(i, true)
					break
				}
			} else if c == '.' {
				dotCount++
				if dotCount > 3 {
					result.SetNull(i, true)
					break
				}
				res = (res << 8) + byteResult
				byteResult = 0
			} else {
				result.SetNull(i, true)
				break // illegal char (not number or .)
			}
		}
		// 127 		-> 0.0.0.127
		// 127.255 	-> 127.0.0.255
		// 127.256	-> NULL
		// 127.2.1	-> 127.2.0.1
		if !result.IsNull(i) {
			if dotCount == 1 {
				res <<= 16
			}
			if dotCount == 2 {
				res <<= 8
			}
			i64s[i] = int64((res << 8) + byteResult)
		}
	}
	return nil
}

func (b *builtinInet6NtoaSig) vectorized() bool {
	return true
}

func (b *builtinInet6NtoaSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	val, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(val)
	if err := b.args[0].VecEvalString(ctx, input, val); err != nil {
		return err
	}
	result.ReserveString(n)
	for i := range n {
		if val.IsNull(i) {
			result.AppendNull()
			continue
		}
		valI := val.GetString(i)
		ip := net.IP(valI).String()
		if len(valI) == net.IPv6len && !strings.Contains(ip, ":") {
			ip = fmt.Sprintf("::ffff:%s", ip)
		}
		if net.ParseIP(ip) == nil {
			result.AppendNull()
			continue
		}
		result.AppendString(ip)
	}
	return nil
}

func (b *builtinNameConstRealSig) vectorized() bool {
	return true
}

func (b *builtinNameConstRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[1].VecEvalReal(ctx, input, result)
}

func (b *builtinVitessHashSig) vectorized() bool {
	return true
}

func (b *builtinVitessHashSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	column, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(column)

	if err := b.args[0].VecEvalInt(ctx, input, column); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	r64s := result.Uint64s()
	result.MergeNulls(column)

	for i := range n {
		if column.IsNull(i) {
			continue
		}
		var uintKey = column.GetUint64(i)
		var hash uint64
		if hash, err = vitess.HashUint64(uintKey); err != nil {
			return err
		}
		r64s[i] = hash
	}

	return nil
}

func (b *builtinUUIDToBinSig) vectorized() bool {
	return true
}

// evalString evals UUID_TO_BIN(string_uuid, swap_flag).
// See https://dev.mysql.com/doc/refman/8.0/en/miscellaneous-functions.html#function_uuid-to-bin
func (b *builtinUUIDToBinSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	valBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(valBuf)
	if err := b.args[0].VecEvalString(ctx, input, valBuf); err != nil {
		return err
	}

	var flagBuf *chunk.Column
	i64s := make([]int64, n)
	if len(b.args) == 2 {
		flagBuf, err = b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(flagBuf)
		if err := b.args[1].VecEvalInt(ctx, input, flagBuf); err != nil {
			return err
		}
		i64s = flagBuf.Int64s()
	}
	result.ReserveString(n)
	for i := range n {
		if valBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		val := valBuf.GetString(i)
		// MySQL's UUID_TO_BIN is strict and doesn't trim spaces, unlike Go's uuid.Parse
		// We need to check if the string has leading/trailing spaces before parsing
		if strings.TrimSpace(val) != val {
			return errWrongValueForType.GenWithStackByArgs("string", val, "uuid_to_bin")
		}
		u, err := uuid.Parse(val)
		if err != nil {
			return errWrongValueForType.GenWithStackByArgs("string", val, "uuid_to_bin")
		}
		bin, err := u.MarshalBinary()
		if err != nil {
			return errWrongValueForType.GenWithStackByArgs("string", val, "uuid_to_bin")
		}
		if len(b.args) == 2 && flagBuf.IsNull(i) {
			result.AppendString(string(bin))
			continue
		}
		if i64s[i] != 0 {
			result.AppendString(swapBinaryUUID(bin))
		} else {
			result.AppendString(string(bin))
		}
	}
	return nil
}

func (b *builtinBinToUUIDSig) vectorized() bool {
	return true
}

// evalString evals BIN_TO_UUID(binary_uuid, swap_flag).
// See https://dev.mysql.com/doc/refman/8.0/en/miscellaneous-functions.html#function_bin-to-uuid
func (b *builtinBinToUUIDSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	valBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(valBuf)
	if err := b.args[0].VecEvalString(ctx, input, valBuf); err != nil {
		return err
	}

	var flagBuf *chunk.Column
	i64s := make([]int64, n)
	if len(b.args) == 2 {
		flagBuf, err = b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(flagBuf)
		if err := b.args[1].VecEvalInt(ctx, input, flagBuf); err != nil {
			return err
		}
		i64s = flagBuf.Int64s()
	}
	result.ReserveString(n)
	for i := range n {
		if valBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		val := valBuf.GetString(i)
		var u uuid.UUID
		err = u.UnmarshalBinary([]byte(val))
		if err != nil {
			return errWrongValueForType.GenWithStackByArgs("string", val, "bin_to_uuid")
		}
		str := u.String()
		if len(b.args) == 2 && flagBuf.IsNull(i) {
			result.AppendString(str)
			continue
		}
		if i64s[i] != 0 {
			result.AppendString(swapStringUUID(str))
		} else {
			result.AppendString(str)
		}
	}
	return nil
}
