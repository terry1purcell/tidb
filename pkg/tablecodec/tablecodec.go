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

package tablecodec

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/structure"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/tikv/client-go/v2/tikv"
)

var (
	errInvalidKey       = dbterror.ClassXEval.NewStd(errno.ErrInvalidKey)
	errInvalidRecordKey = dbterror.ClassXEval.NewStd(errno.ErrInvalidRecordKey)
	errInvalidIndexKey  = dbterror.ClassXEval.NewStd(errno.ErrInvalidIndexKey)
)

var (
	tablePrefix     = []byte{'t'}
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
	metaPrefix      = []byte{'m'}
)

const (
	idLen     = 8
	prefixLen = 1 + idLen /*tableID*/ + 2
	// RecordRowKeyLen is public for calculating average row size.
	RecordRowKeyLen       = prefixLen + idLen /*handle*/
	tablePrefixLength     = 1
	recordPrefixSepLength = 2
	metaPrefixLength      = 1
	// MaxOldEncodeValueLen is the maximum len of the old encoding of index value.
	MaxOldEncodeValueLen = 9

	// CommonHandleFlag is the flag used to decode the common handle in an unique index value.
	CommonHandleFlag byte = 127
	// PartitionIDFlag is the flag used to decode the partition ID in global index value.
	PartitionIDFlag byte = 126
	// IndexVersionFlag is the flag used to decode the index's version info.
	IndexVersionFlag byte = 125
	// RestoreDataFlag is the flag that RestoreData begin with.
	// See rowcodec.Encoder.Encode and rowcodec.row.toBytes
	RestoreDataFlag byte = rowcodec.CodecVer
)

// TableSplitKeyLen is the length of key 't{table_id}' which is used for table split.
const TableSplitKeyLen = 1 + idLen

func init() {
	// help kv package to refer the tablecodec package to resolve the kv.Key functions.
	kv.DecodeTableIDFunc = func(key kv.Key) int64 {
		//preCheck, avoid the noise error log.
		if hasTablePrefix(key) && len(key) >= TableSplitKeyLen {
			return DecodeTableID(key)
		}
		return 0
	}
}

// TablePrefix returns table's prefix 't'.
func TablePrefix() []byte {
	return tablePrefix
}

// MetaPrefix returns meta prefix 'm'.
func MetaPrefix() []byte {
	return metaPrefix
}

// EncodeRowKey encodes the table id and record handle into a kv.Key
func EncodeRowKey(tableID int64, encodedHandle []byte) kv.Key {
	buf := make([]byte, 0, prefixLen+len(encodedHandle))
	buf = appendTableRecordPrefix(buf, tableID)
	buf = append(buf, encodedHandle...)
	return buf
}

// EncodeRowKeyWithHandle encodes the table id, row handle into a kv.Key
func EncodeRowKeyWithHandle(tableID int64, handle kv.Handle) kv.Key {
	return EncodeRowKey(tableID, handle.Encoded())
}

// CutRowKeyPrefix cuts the row key prefix.
func CutRowKeyPrefix(key kv.Key) []byte {
	return key[prefixLen:]
}

// EncodeRecordKey encodes the recordPrefix, row handle into a kv.Key.
func EncodeRecordKey(recordPrefix kv.Key, h kv.Handle) kv.Key {
	buf := make([]byte, 0, len(recordPrefix)+h.Len())
	if ph, ok := h.(kv.PartitionHandle); ok {
		recordPrefix = GenTableRecordPrefix(ph.PartitionID)
	}
	buf = append(buf, recordPrefix...)
	buf = append(buf, h.Encoded()...)
	return buf
}

func hasTablePrefix(key kv.Key) bool {
	return key[0] == tablePrefix[0]
}

func hasRecordPrefixSep(key kv.Key) bool {
	return key[0] == recordPrefixSep[0] && key[1] == recordPrefixSep[1]
}

// DecodeRecordKey decodes the key and gets the tableID, handle.
func DecodeRecordKey(key kv.Key) (tableID int64, handle kv.Handle, err error) {
	if len(key) <= prefixLen {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", key)
	}

	k := key
	if !hasTablePrefix(key) {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", k)
	}

	key = key[tablePrefixLength:]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}

	if !hasRecordPrefixSep(key) {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", k)
	}

	key = key[recordPrefixSepLength:]
	if len(key) == 8 {
		var intHandle int64
		key, intHandle, err = codec.DecodeInt(key)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		return tableID, kv.IntHandle(intHandle), nil
	}
	h, err := kv.NewCommonHandle(key)
	if err != nil {
		return 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q %v", k, err)
	}
	return tableID, h, nil
}

// DecodeIndexKey decodes the key and gets the tableID, indexID, indexValues.
func DecodeIndexKey(key kv.Key) (tableID int64, indexID int64, indexValues []string, err error) {
	k := key

	tableID, indexID, isRecord, err := DecodeKeyHead(key)
	if err != nil {
		return 0, 0, nil, errors.Trace(err)
	}
	if isRecord {
		err = errInvalidIndexKey.GenWithStack("invalid index key - %q", k)
		return 0, 0, nil, err
	}
	indexKey := key[prefixLen+idLen:]
	indexValues, err = DecodeValuesBytesToStrings(indexKey)
	if err != nil {
		err = errInvalidIndexKey.GenWithStack("invalid index key - %q %v", k, err)
		return 0, 0, nil, err
	}
	return tableID, indexID, indexValues, nil
}

// DecodeValuesBytesToStrings decode the raw bytes to strings for each columns.
// FIXME: Without the schema information, we can only decode the raw kind of
// the column. For instance, MysqlTime is internally saved as uint64.
func DecodeValuesBytesToStrings(b []byte) ([]string, error) {
	var datumValues []string
	for len(b) > 0 {
		remain, d, e := codec.DecodeOne(b)
		if e != nil {
			return nil, e
		}
		str, e1 := d.ToString()
		if e1 != nil {
			return nil, e
		}
		datumValues = append(datumValues, str)
		b = remain
	}
	return datumValues, nil
}

// EncodeMetaKey encodes the key and field into meta key.
func EncodeMetaKey(key []byte, field []byte) kv.Key {
	ek := make([]byte, 0, len(metaPrefix)+codec.EncodedBytesLength(len(key))+8+codec.EncodedBytesLength(len(field)))
	ek = append(ek, metaPrefix...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(structure.HashData))
	ek = codec.EncodeBytes(ek, field)
	return ek
}

// EncodeMetaKeyPrefix encodes the key prefix into meta key
func EncodeMetaKeyPrefix(key []byte) kv.Key {
	ek := make([]byte, 0, len(metaPrefix)+codec.EncodedBytesLength(len(key))+8)
	ek = append(ek, metaPrefix...)
	ek = codec.EncodeBytes(ek, key)
	ek = codec.EncodeUint(ek, uint64(structure.HashData))
	return ek
}

// DecodeMetaKey decodes the key and get the meta key and meta field.
func DecodeMetaKey(ek kv.Key) (key []byte, field []byte, err error) {
	var tp uint64
	if !bytes.HasPrefix(ek, metaPrefix) {
		return nil, nil, errors.New("invalid encoded hash data key prefix")
	}
	ek = ek[metaPrefixLength:]
	ek, key, err = codec.DecodeBytes(ek, nil)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	ek, tp, err = codec.DecodeUint(ek)
	if err != nil {
		return nil, nil, errors.Trace(err)
	} else if structure.TypeFlag(tp) != structure.HashData {
		return nil, nil, errors.Errorf("invalid encoded hash data key flag %c", byte(tp))
	}
	_, field, err = codec.DecodeBytes(ek, nil)
	return key, field, errors.Trace(err)
}

// DecodeKeyHead decodes the key's head and gets the tableID, indexID. isRecordKey is true when is a record key.
func DecodeKeyHead(key kv.Key) (tableID int64, indexID int64, isRecordKey bool, err error) {
	isRecordKey = false
	k := key
	if !key.HasPrefix(tablePrefix) {
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
		return
	}

	key = key[len(tablePrefix):]
	key, tableID, err = codec.DecodeInt(key)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	if key.HasPrefix(recordPrefixSep) {
		isRecordKey = true
		return
	}
	if !key.HasPrefix(indexPrefixSep) {
		err = errInvalidKey.GenWithStack("invalid key - %q", k)
		return
	}

	key = key[len(indexPrefixSep):]

	key, indexID, err = codec.DecodeInt(key)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	return
}

// DecodeIndexID decodes indexID from the key.
// this method simply extract index id part, and no other checking.
// Caller should make sure the key is an index key.
func DecodeIndexID(key kv.Key) (int64, error) {
	key = key[len(tablePrefix)+8+len(indexPrefixSep):]

	_, indexID, err := codec.DecodeInt(key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return indexID, nil
}

// DecodeTableID decodes the table ID of the key, if the key is not table key, returns 0.
func DecodeTableID(key kv.Key) int64 {
	if !key.HasPrefix(tablePrefix) {
		// If the key is in API V2, then ignore the prefix
		_, k, err := tikv.DecodeKey(key, kvrpcpb.APIVersion_V2)
		if err != nil {
			terror.Log(errors.Trace(err))
			return 0
		}
		key = k
		if !key.HasPrefix(tablePrefix) {
			return 0
		}
	}
	key = key[len(tablePrefix):]
	_, tableID, err := codec.DecodeInt(key)
	// TODO: return error.
	terror.Log(errors.Trace(err))
	return tableID
}

// DecodeRowKey decodes the key and gets the handle.
func DecodeRowKey(key kv.Key) (kv.Handle, error) {
	// In the read path, remove the keyspace prefix
	// to ensure compatibility with the key parsing implemented in the mock.
	tempKey := rowcodec.RemoveKeyspacePrefix(key)

	if len(tempKey) < RecordRowKeyLen || !hasTablePrefix(tempKey) || !hasRecordPrefixSep(tempKey[prefixLen-2:]) {
		return kv.IntHandle(0), errInvalidKey.GenWithStack("invalid key - %q", tempKey)
	}
	if len(tempKey) == RecordRowKeyLen {
		u := binary.BigEndian.Uint64(tempKey[prefixLen:])
		return kv.IntHandle(codec.DecodeCmpUintToInt(u)), nil
	}
	return kv.NewCommonHandle(tempKey[prefixLen:])
}

// EncodeValue encodes a go value to bytes.
// This function may return both a valid encoded bytes and an error (actually `"pingcap/errors".ErrorGroup`). If the caller
// expects to handle these errors according to `SQL_MODE` or other configuration, please refer to `pkg/errctx`.
func EncodeValue(loc *time.Location, b []byte, raw types.Datum) ([]byte, error) {
	var v types.Datum
	err := flatten(loc, raw, &v)
	if err != nil {
		return nil, err
	}

	val, err := codec.EncodeValue(loc, b, v)

	return val, err
}

// EncodeRow encode row data and column ids into a slice of byte.
// valBuf and values pass by caller, for reducing EncodeRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeRow will allocate it.
// This function may return both a valid encoded bytes and an error (actually `"pingcap/errors".ErrorGroup`). If the caller
// expects to handle these errors according to `SQL_MODE` or other configuration, please refer to `pkg/errctx`.
func EncodeRow(loc *time.Location, row []types.Datum, colIDs []int64, valBuf []byte, values []types.Datum, checksum rowcodec.Checksum, e *rowcodec.Encoder) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	if e.Enable {
		valBuf = valBuf[:0]
		return e.Encode(loc, colIDs, row, checksum, valBuf)
	}
	return EncodeOldRow(loc, row, colIDs, valBuf, values)
}

// EncodeOldRow encode row data and column ids into a slice of byte.
// Row layout: colID1, value1, colID2, value2, .....
// valBuf and values pass by caller, for reducing EncodeOldRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeOldRow will allocate it.
func EncodeOldRow(loc *time.Location, row []types.Datum, colIDs []int64, valBuf []byte, values []types.Datum) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	valBuf = valBuf[:0]
	if values == nil {
		values = make([]types.Datum, len(row)*2)
	}
	for i, c := range row {
		id := colIDs[i]
		values[2*i].SetInt64(id)
		err := flatten(loc, c, &values[2*i+1])
		if err != nil {
			return valBuf, errors.Trace(err)
		}
	}
	if len(values) == 0 {
		// We could not set nil value into kv.
		return append(valBuf, codec.NilFlag), nil
	}
	return codec.EncodeValue(loc, valBuf, values...)
}

func flatten(loc *time.Location, data types.Datum, ret *types.Datum) error {
	switch data.Kind() {
	case types.KindMysqlTime:
		// for mysql datetime, timestamp and date type
		t := data.GetMysqlTime()
		if t.Type() == mysql.TypeTimestamp && loc != nil && loc != time.UTC {
			err := t.ConvertTimeZone(loc, time.UTC)
			if err != nil {
				return errors.Trace(err)
			}
		}
		v, err := t.ToPackedUint()
		ret.SetUint64(v)
		return errors.Trace(err)
	case types.KindMysqlDuration:
		// for mysql time type
		ret.SetInt64(int64(data.GetMysqlDuration().Duration))
		return nil
	case types.KindMysqlEnum:
		ret.SetUint64(data.GetMysqlEnum().Value)
		return nil
	case types.KindMysqlSet:
		ret.SetUint64(data.GetMysqlSet().Value)
		return nil
	case types.KindBinaryLiteral, types.KindMysqlBit:
		// We don't need to handle errors here since the literal is ensured to be able to store in uint64 in convertToMysqlBit.
		val, err := data.GetBinaryLiteral().ToInt(types.StrictContext)
		if err != nil {
			return errors.Trace(err)
		}
		ret.SetUint64(val)
		return nil
	default:
		*ret = data
		return nil
	}
}

// DecodeColumnValue decodes data to a Datum according to the column info.
func DecodeColumnValue(data []byte, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	_, d, err := codec.DecodeOne(data)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	colDatum, err := Unflatten(d, ft, loc)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return colDatum, nil
}

// DecodeColumnValueWithDatum decodes data to an existing Datum according to the column info.
func DecodeColumnValueWithDatum(data []byte, ft *types.FieldType, loc *time.Location, result *types.Datum) error {
	var err error
	_, *result, err = codec.DecodeOne(data)
	if err != nil {
		return errors.Trace(err)
	}
	*result, err = Unflatten(*result, ft, loc)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// DecodeRowWithMapNew decode a row to datum map.
func DecodeRowWithMapNew(b []byte, cols map[int64]*types.FieldType,
	loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if row == nil {
		row = make(map[int64]types.Datum, len(cols))
	}
	if b == nil {
		return row, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return row, nil
	}

	reqCols := make([]rowcodec.ColInfo, len(cols))
	var idx int
	for id, tp := range cols {
		reqCols[idx] = rowcodec.ColInfo{
			ID: id,
			Ft: tp,
		}
		idx++
	}
	rd := rowcodec.NewDatumMapDecoder(reqCols, loc)
	return rd.DecodeToDatumMap(b, row)
}

// DecodeRowWithMap decodes a byte slice into datums with an existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeRowWithMap(b []byte, cols map[int64]*types.FieldType, loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if row == nil {
		row = make(map[int64]types.Datum, len(cols))
	}
	if b == nil {
		return row, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return row, nil
	}
	cnt := 0
	var (
		data []byte
		err  error
	)
	for len(b) > 0 {
		// Get col id.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, cid, err := codec.DecodeOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Get col value.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		id := cid.GetInt64()
		ft, ok := cols[id]
		if ok {
			_, v, err := codec.DecodeOne(data)
			if err != nil {
				return nil, errors.Trace(err)
			}
			v, err = Unflatten(v, ft, loc)
			if err != nil {
				return nil, errors.Trace(err)
			}
			row[id] = v
			cnt++
			if cnt == len(cols) {
				// Get enough data.
				break
			}
		}
	}
	return row, nil
}

// DecodeRowToDatumMap decodes a byte slice into datums.
// Row layout: colID1, value1, colID2, value2, .....
// Default value columns, generated columns and handle columns are unprocessed.
func DecodeRowToDatumMap(b []byte, cols map[int64]*types.FieldType, loc *time.Location) (map[int64]types.Datum, error) {
	if !rowcodec.IsNewFormat(b) {
		return DecodeRowWithMap(b, cols, loc, nil)
	}
	return DecodeRowWithMapNew(b, cols, loc, nil)
}

// DecodeHandleToDatumMap decodes a handle into datum map.
func DecodeHandleToDatumMap(handle kv.Handle, handleColIDs []int64,
	cols map[int64]*types.FieldType, loc *time.Location, row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if handle == nil || len(handleColIDs) == 0 {
		return row, nil
	}
	if row == nil {
		row = make(map[int64]types.Datum, len(cols))
	}
	for idx, id := range handleColIDs {
		ft, ok := cols[id]
		if !ok {
			continue
		}
		if types.NeedRestoredData(ft) {
			continue
		}
		d, err := decodeHandleToDatum(handle, ft, idx)
		if err != nil {
			return row, err
		}
		d, err = Unflatten(d, ft, loc)
		if err != nil {
			return row, err
		}
		if _, exists := row[id]; !exists {
			row[id] = d
		}
	}
	return row, nil
}

// decodeHandleToDatum decodes a handle to a specific column datum.
func decodeHandleToDatum(handle kv.Handle, ft *types.FieldType, idx int) (types.Datum, error) {
	var d types.Datum
	var err error
	if handle.IsInt() {
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			d = types.NewUintDatum(uint64(handle.IntValue()))
		} else {
			d = types.NewIntDatum(handle.IntValue())
		}
		return d, nil
	}
	// Decode common handle to Datum.
	_, d, err = codec.DecodeOne(handle.EncodedCol(idx))
	return d, err
}

// CutRowNew cuts encoded row into byte slices and return columns' byte slice.
// Row layout: colID1, value1, colID2, value2, .....
func CutRowNew(data []byte, colIDs map[int64]int) ([][]byte, error) {
	if data == nil {
		return nil, nil
	}
	if len(data) == 1 && data[0] == codec.NilFlag {
		return nil, nil
	}

	var (
		cnt int
		b   []byte
		err error
		cid int64
	)
	row := make([][]byte, len(colIDs))
	for len(data) > 0 && cnt < len(colIDs) {
		// Get col id.
		data, cid, err = codec.CutColumnID(data)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// Get col value.
		b, data, err = codec.CutOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}

		offset, ok := colIDs[cid]
		if ok {
			row[offset] = b
			cnt++
		}
	}
	return row, nil
}

// UnflattenDatums converts raw datums to column datums.
func UnflattenDatums(datums []types.Datum, fts []*types.FieldType, loc *time.Location) ([]types.Datum, error) {
	for i, datum := range datums {
		ft := fts[i]
		uDatum, err := Unflatten(datum, ft, loc)
		if err != nil {
			return datums, errors.Trace(err)
		}
		datums[i] = uDatum
	}
	return datums, nil
}

// Unflatten converts a raw datum to a column datum.
func Unflatten(datum types.Datum, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	if datum.IsNull() {
		return datum, nil
	}
	switch ft.GetType() {
	case mysql.TypeFloat:
		datum.SetFloat32(float32(datum.GetFloat64()))
		return datum, nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		datum.SetString(datum.GetString(), ft.GetCollate())
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeYear, mysql.TypeInt24,
		mysql.TypeLong, mysql.TypeLonglong, mysql.TypeDouble:
		return datum, nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		t := types.NewTime(types.ZeroCoreTime, ft.GetType(), ft.GetDecimal())
		var err error
		err = t.FromPackedUint(datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		if ft.GetType() == mysql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, loc)
			if err != nil {
				return datum, errors.Trace(err)
			}
		}
		datum.SetUint64(0)
		datum.SetMysqlTime(t)
		return datum, nil
	case mysql.TypeDuration: // duration should read fsp from column meta data
		dur := types.Duration{Duration: time.Duration(datum.GetInt64()), Fsp: ft.GetDecimal()}
		datum.SetMysqlDuration(dur)
		return datum, nil
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(ft.GetElems(), datum.GetUint64())
		if err != nil {
			enum = types.Enum{}
		}
		datum.SetMysqlEnum(enum, ft.GetCollate())
		return datum, nil
	case mysql.TypeSet:
		set, err := types.ParseSetValue(ft.GetElems(), datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetMysqlSet(set, ft.GetCollate())
		return datum, nil
	case mysql.TypeBit:
		val := datum.GetUint64()
		byteSize := (ft.GetFlen() + 7) >> 3
		datum.SetUint64(0)
		datum.SetMysqlBit(types.NewBinaryLiteralFromUint(val, byteSize))
	}
	return datum, nil
}

// EncodeIndexSeekKey encodes an index value to kv.Key.
func EncodeIndexSeekKey(tableID int64, idxID int64, encodedValue []byte) kv.Key {
	key := make([]byte, 0, RecordRowKeyLen+len(encodedValue))
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	key = append(key, encodedValue...)
	return key
}

// CutIndexKey cuts encoded index key into colIDs to bytes slices map.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKey(key kv.Key, colIDs []int64) (values map[int64][]byte, b []byte, err error) {
	b = key[prefixLen+idLen:]
	values = make(map[int64][]byte, len(colIDs))
	for _, id := range colIDs {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values[id] = val
	}
	return
}

// CutIndexPrefix cuts the index prefix.
func CutIndexPrefix(key kv.Key) []byte {
	return key[prefixLen+idLen:]
}

// CutIndexKeyTo cuts encoded index key into colIDs to bytes slices.
// The caller should prepare the memory of the result values.
func CutIndexKeyTo(key kv.Key, values [][]byte) (b []byte, err error) {
	b = key[prefixLen+idLen:]
	length := len(values)
	for i := range length {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		values[i] = val
	}
	return
}

// CutIndexKeyNew cuts encoded index key into colIDs to bytes slices.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutIndexKeyNew(key kv.Key, length int) (values [][]byte, b []byte, err error) {
	values = make([][]byte, length)
	b, err = CutIndexKeyTo(key, values)
	return
}

// CutCommonHandle cuts encoded common handle key into colIDs to bytes slices.
// The returned value b is the remaining bytes of the key which would be empty if it is unique index or handle data
// if it is non-unique index.
func CutCommonHandle(key kv.Key, length int) (values [][]byte, b []byte, err error) {
	b = key[prefixLen:]
	values = make([][]byte, 0, length)
	for range length {
		var val []byte
		val, b, err = codec.CutOne(b)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		values = append(values, val)
	}
	return
}

// HandleStatus is the handle status in index.
type HandleStatus int

const (
	// HandleDefault means decode handle value as int64 or bytes when DecodeIndexKV.
	HandleDefault HandleStatus = iota
	// HandleIsUnsigned means decode handle value as uint64 when DecodeIndexKV.
	HandleIsUnsigned
	// HandleNotNeeded means no need to decode handle value when DecodeIndexKV.
	HandleNotNeeded
)

// reEncodeHandle encodes the handle as a Datum so it can be properly decoded later.
// If it is common handle, it returns the encoded column values.
// If it is int handle, it is encoded as int Datum or uint Datum decided by the unsigned.
func reEncodeHandle(handle kv.Handle, unsigned bool) ([][]byte, error) {
	handleColLen := 1
	if !handle.IsInt() {
		handleColLen = handle.NumCols()
	}
	result := make([][]byte, 0, handleColLen)
	return reEncodeHandleTo(handle, unsigned, nil, result)
}

func reEncodeHandleTo(handle kv.Handle, unsigned bool, buf []byte, result [][]byte) ([][]byte, error) {
	if !handle.IsInt() {
		handleColLen := handle.NumCols()
		for i := range handleColLen {
			result = append(result, handle.EncodedCol(i))
		}
		return result, nil
	}
	handleDatum := types.NewIntDatum(handle.IntValue())
	if unsigned {
		handleDatum.SetUint64(handleDatum.GetUint64())
	}
	intHandleBytes, err := codec.EncodeValue(time.UTC, buf, handleDatum)
	result = append(result, intHandleBytes)
	return result, err
}

// reEncodeHandleConsiderNewCollation encodes the handle as a Datum so it can be properly decoded later.
func reEncodeHandleConsiderNewCollation(handle kv.Handle, columns []rowcodec.ColInfo, restoreData []byte) ([][]byte, error) {
	handleColLen := handle.NumCols()
	cHandleBytes := make([][]byte, 0, handleColLen)
	for i := range handleColLen {
		cHandleBytes = append(cHandleBytes, handle.EncodedCol(i))
	}
	if len(restoreData) == 0 {
		return cHandleBytes, nil
	}
	// Remove some extra columns(ID < 0), such like `model.ExtraPhysTblID`.
	// They are not belong to common handle and no need to restore data.
	idx := len(columns)
	for idx > 0 && columns[idx-1].ID < 0 {
		idx--
	}
	return decodeRestoredValuesV5(columns[:idx], cHandleBytes, restoreData)
}

func decodeRestoredValues(columns []rowcodec.ColInfo, restoredVal []byte) ([][]byte, error) {
	colIDs := make(map[int64]int, len(columns))
	for i, col := range columns {
		colIDs[col.ID] = i
	}
	// We don't need to decode handle here, and colIDs >= 0 always.
	rd := rowcodec.NewByteDecoder(columns, []int64{-1}, nil, nil)
	resultValues, err := rd.DecodeToBytesNoHandle(colIDs, restoredVal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resultValues, nil
}

// decodeRestoredValuesV5 decodes index values whose format is introduced in TiDB 5.0.
// Unlike the format in TiDB 4.0, the new format is optimized for storage space:
// 1. If the index is a composed index, only the non-binary string column's value need to write to value, not all.
// 2. If a string column's collation is _bin, then we only write the number of the truncated spaces to value.
// 3. If a string column is char, not varchar, then we use the sortKey directly.
func decodeRestoredValuesV5(columns []rowcodec.ColInfo, results [][]byte, restoredVal []byte) ([][]byte, error) {
	colIDOffsets := buildColumnIDOffsets(columns)
	colInfosNeedRestore := buildRestoredColumn(columns)
	rd := rowcodec.NewByteDecoder(colInfosNeedRestore, nil, nil, nil)
	newResults, err := rd.DecodeToBytesNoHandle(colIDOffsets, restoredVal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i := range newResults {
		noRestoreData := len(newResults[i]) == 0
		if noRestoreData {
			newResults[i] = results[i]
			continue
		}
		if collate.IsBinCollation(columns[i].Ft.GetCollate()) {
			noPaddingDatum, err := DecodeColumnValue(results[i], columns[i].Ft, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			paddingCountDatum, err := DecodeColumnValue(newResults[i], types.NewFieldType(mysql.TypeLonglong), nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			noPaddingStr, paddingCount := noPaddingDatum.GetString(), int(paddingCountDatum.GetInt64())
			// Skip if padding count is 0.
			if paddingCount == 0 {
				newResults[i] = results[i]
				continue
			}
			newDatum := &noPaddingDatum
			newDatum.SetString(noPaddingStr+strings.Repeat(" ", paddingCount), newDatum.Collation())
			newResults[i] = newResults[i][:0]
			newResults[i] = append(newResults[i], rowcodec.BytesFlag)
			newResults[i] = codec.EncodeBytes(newResults[i], newDatum.GetBytes())
		}
	}
	return newResults, nil
}

func buildColumnIDOffsets(allCols []rowcodec.ColInfo) map[int64]int {
	colIDOffsets := make(map[int64]int, len(allCols))
	for i, col := range allCols {
		colIDOffsets[col.ID] = i
	}
	return colIDOffsets
}

func buildRestoredColumn(allCols []rowcodec.ColInfo) []rowcodec.ColInfo {
	restoredColumns := make([]rowcodec.ColInfo, 0, len(allCols))
	for i, col := range allCols {
		if !types.NeedRestoredData(col.Ft) {
			continue
		}
		copyColInfo := rowcodec.ColInfo{
			ID: col.ID,
		}
		if collate.IsBinCollation(col.Ft.GetCollate()) {
			// Change the fieldType from string to uint since we store the number of the truncated spaces.
			// NOTE: the corresponding datum is generated as `types.NewUintDatum(paddingSize)`, and the raw data is
			// encoded via `encodeUint`. Thus we should mark the field type as unsigened here so that the BytesDecoder
			// can decode it correctly later. Otherwise there might be issues like #47115.
			copyColInfo.Ft = types.NewFieldType(mysql.TypeLonglong)
			copyColInfo.Ft.AddFlag(mysql.UnsignedFlag)
		} else {
			copyColInfo.Ft = allCols[i].Ft
		}
		restoredColumns = append(restoredColumns, copyColInfo)
	}
	return restoredColumns
}

func decodeIndexKvOldCollation(key, value []byte, hdStatus HandleStatus, buf []byte, resultValues [][]byte) ([][]byte, error) {
	b, err := CutIndexKeyTo(key, resultValues)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if hdStatus == HandleNotNeeded {
		return resultValues, nil
	}
	var handle kv.Handle
	if len(b) > 0 {
		// non-unique index
		handle, err = decodeHandleInIndexKey(b)
		if err != nil {
			return nil, err
		}
		resultValues, err = reEncodeHandleTo(handle, hdStatus == HandleIsUnsigned, buf, resultValues)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		// In unique int handle index.
		handle = DecodeIntHandleInIndexValue(value)
		resultValues, err = reEncodeHandleTo(handle, hdStatus == HandleIsUnsigned, buf, resultValues)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return resultValues, nil
}

func getIndexVersion(value []byte) int {
	if len(value) <= MaxOldEncodeValueLen {
		return 0
	}
	tailLen := int(value[0])
	if (tailLen == 0 || tailLen == 1) && value[1] == IndexVersionFlag {
		return int(value[2])
	}
	return 0
}

// DecodeIndexKVEx looks like DecodeIndexKV, the difference is that it tries to reduce allocations.
func DecodeIndexKVEx(key, value []byte, colsLen int, hdStatus HandleStatus, columns []rowcodec.ColInfo, buf []byte, preAlloc [][]byte) ([][]byte, error) {
	if len(value) <= MaxOldEncodeValueLen {
		return decodeIndexKvOldCollation(key, value, hdStatus, buf, preAlloc)
	}
	if getIndexVersion(value) == 1 {
		return decodeIndexKvForClusteredIndexVersion1(key, value, colsLen, hdStatus, columns)
	}
	return decodeIndexKvGeneral(key, value, colsLen, hdStatus, columns)
}

// DecodeIndexKV uses to decode index key values.
//
//	`colsLen` is expected to be index columns count.
//	`columns` is expected to be index columns + handle columns(if hdStatus is not HandleNotNeeded).
func DecodeIndexKV(key, value []byte, colsLen int, hdStatus HandleStatus, columns []rowcodec.ColInfo) ([][]byte, error) {
	if len(value) <= MaxOldEncodeValueLen {
		preAlloc := make([][]byte, colsLen, colsLen+len(columns))
		return decodeIndexKvOldCollation(key, value, hdStatus, nil, preAlloc)
	}
	if getIndexVersion(value) == 1 {
		return decodeIndexKvForClusteredIndexVersion1(key, value, colsLen, hdStatus, columns)
	}
	return decodeIndexKvGeneral(key, value, colsLen, hdStatus, columns)
}

// DecodeIndexHandle uses to decode the handle from index key/value.
func DecodeIndexHandle(key, value []byte, colsLen int) (kv.Handle, error) {
	var err error
	b := key[prefixLen+idLen:]
	for range colsLen {
		_, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if len(b) > 0 {
		handle, err := decodeHandleInIndexKey(b)
		if err != nil {
			return nil, err
		}
		// If len(value) >= 9, it may contains partition id.
		// We should decode it and return a partition handle.
		if len(value) >= 9 {
			seg := SplitIndexValue(value)
			if len(seg.PartitionID) != 0 {
				_, pid, err := codec.DecodeInt(seg.PartitionID)
				if err != nil {
					return nil, err
				}
				handle = kv.NewPartitionHandle(pid, handle)
			}
		}
		return handle, nil
	} else if len(value) >= 8 {
		return DecodeHandleInIndexValue(value)
	}
	// Should never execute to here.
	return nil, errors.Errorf("no handle in index key: %v, value: %v", key, value)
}

func decodeHandleInIndexKey(keySuffix []byte) (kv.Handle, error) {
	remain, d, err := codec.DecodeOne(keySuffix)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(remain) == 0 && d.Kind() == types.KindInt64 {
		return kv.IntHandle(d.GetInt64()), nil
	}
	return kv.NewCommonHandle(keySuffix)
}

// DecodeHandleInIndexValue decodes handle in unqiue index value.
func DecodeHandleInIndexValue(value []byte) (handle kv.Handle, err error) {
	if len(value) <= MaxOldEncodeValueLen {
		return DecodeIntHandleInIndexValue(value), nil
	}
	seg := SplitIndexValue(value)
	if len(seg.IntHandle) != 0 {
		handle = DecodeIntHandleInIndexValue(seg.IntHandle)
	}
	if len(seg.CommonHandle) != 0 {
		handle, err = kv.NewCommonHandle(seg.CommonHandle)
		if err != nil {
			return nil, err
		}
	}
	if len(seg.PartitionID) != 0 {
		_, pid, err := codec.DecodeInt(seg.PartitionID)
		if err != nil {
			return nil, err
		}
		handle = kv.NewPartitionHandle(pid, handle)
	}
	return handle, nil
}

// DecodeIntHandleInIndexValue uses to decode index value as int handle id.
func DecodeIntHandleInIndexValue(data []byte) kv.Handle {
	return kv.IntHandle(binary.BigEndian.Uint64(data))
}

// EncodeTableIndexPrefix encodes index prefix with tableID and idxID.
func EncodeTableIndexPrefix(tableID, idxID int64) kv.Key {
	key := make([]byte, 0, prefixLen+idLen)
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	return key
}

// EncodeTablePrefix encodes the table prefix to generate a key
func EncodeTablePrefix(tableID int64) kv.Key {
	key := make([]byte, 0, tablePrefixLength+idLen)
	key = append(key, tablePrefix...)
	key = codec.EncodeInt(key, tableID)
	return key
}

// appendTableRecordPrefix appends table record prefix  "t[tableID]_r".
func appendTableRecordPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, recordPrefixSep...)
	return buf
}

// appendTableIndexPrefix appends table index prefix  "t[tableID]_i".
func appendTableIndexPrefix(buf []byte, tableID int64) []byte {
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, indexPrefixSep...)
	return buf
}

// GenTableRecordPrefix composes record prefix with tableID: "t[tableID]_r".
func GenTableRecordPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8+len(recordPrefixSep))
	return appendTableRecordPrefix(buf, tableID)
}

// GenTableIndexPrefix composes index prefix with tableID: "t[tableID]_i".
func GenTableIndexPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8+len(indexPrefixSep))
	return appendTableIndexPrefix(buf, tableID)
}

// IsRecordKey is used to check whether the key is an record key.
func IsRecordKey(k []byte) bool {
	return len(k) > 11 && k[0] == 't' && k[10] == 'r'
}

// IsIndexKey is used to check whether the key is an index key.
func IsIndexKey(k []byte) bool {
	return len(k) > 11 && k[0] == 't' && k[10] == 'i'
}

// IsTableKey is used to check whether the key is a table key.
func IsTableKey(k []byte) bool {
	return len(k) == 9 && k[0] == 't'
}

// IsUntouchedIndexKValue uses to check whether the key is index key, and the value is untouched,
// since the untouched index key/value is no need to commit.
func IsUntouchedIndexKValue(k, v []byte) bool {
	if !IsIndexKey(k) {
		return false
	}
	vLen := len(v)
	if IsTempIndexKey(k) {
		return vLen > 0 && v[vLen-1] == kv.UnCommitIndexKVFlag
	}
	if vLen <= MaxOldEncodeValueLen {
		return (vLen == 1 || vLen == 9) && v[vLen-1] == kv.UnCommitIndexKVFlag
	}
	// New index value format
	tailLen := int(v[0])
	if tailLen < 8 {
		// Non-unique index.
		return tailLen >= 1 && v[vLen-1] == kv.UnCommitIndexKVFlag
	}
	// Unique index
	return tailLen == 9
}

// GenTablePrefix composes table record and index prefix: "t[tableID]".
func GenTablePrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(tablePrefix)+8)
	buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	return buf
}

// TruncateToRowKeyLen truncates the key to row key length if the key is longer than row key.
func TruncateToRowKeyLen(key kv.Key) kv.Key {
	if len(key) > RecordRowKeyLen {
		return key[:RecordRowKeyLen]
	}
	return key
}

// GetTableHandleKeyRange returns table handle's key range with tableID.
func GetTableHandleKeyRange(tableID int64) (startKey, endKey []byte) {
	startKey = EncodeRowKeyWithHandle(tableID, kv.IntHandle(math.MinInt64))
	endKey = EncodeRowKeyWithHandle(tableID, kv.IntHandle(math.MaxInt64))
	return
}

// GetTableIndexKeyRange returns table index's key range with tableID and indexID.
func GetTableIndexKeyRange(tableID, indexID int64) (startKey, endKey []byte) {
	startKey = EncodeIndexSeekKey(tableID, indexID, nil)
	endKey = EncodeIndexSeekKey(tableID, indexID, []byte{255})
	return
}

// GetIndexKeyBuf reuse or allocate buffer
func GetIndexKeyBuf(buf []byte, defaultCap int) []byte {
	if buf != nil {
		return buf[:0]
	}
	return make([]byte, 0, defaultCap)
}

// GenIndexKey generates index key using input physical table id
func GenIndexKey(loc *time.Location, tblInfo *model.TableInfo, idxInfo *model.IndexInfo,
	phyTblID int64, indexedValues []types.Datum, h kv.Handle, buf []byte) (key []byte, distinct bool, err error) {
	if idxInfo.Unique {
		// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
		// A UNIQUE index creates a constraint such that all values in the index must be distinct.
		// An error occurs if you try to add a new row with a key value that matches an existing row.
		// For all engines, a UNIQUE index permits multiple NULL values for columns that can contain NULL.
		distinct = true
		for _, cv := range indexedValues {
			if cv.IsNull() {
				distinct = false
				break
			}
		}
	}
	// For string columns, indexes can be created using only the leading part of column values,
	// using col_name(length) syntax to specify an index prefix length.
	TruncateIndexValues(tblInfo, idxInfo, indexedValues)
	key = GetIndexKeyBuf(buf, RecordRowKeyLen+len(indexedValues)*9+9)
	key = appendTableIndexPrefix(key, phyTblID)
	key = codec.EncodeInt(key, idxInfo.ID)
	key, err = codec.EncodeKey(loc, key, indexedValues...)
	if err != nil {
		return nil, false, err
	}
	if !distinct && h != nil {
		if h.IsInt() {
			// We choose the efficient path here instead of calling `codec.EncodeKey`
			// because the int handle must be an int64, and it must be comparable.
			// This remains correct until codec.encodeSignedInt is changed.
			key = append(key, codec.IntHandleFlag)
			key = codec.EncodeInt(key, h.IntValue())
		} else {
			key = append(key, h.Encoded()...)
		}
	}
	return
}

// TempIndexPrefix used to generate temporary index ID from index ID.
const TempIndexPrefix = 0x7fff000000000000

// IndexIDMask used to get index id from index ID/temp index ID.
const IndexIDMask = 0xffffffffffff

// IndexKey2TempIndexKey generates a temporary index key.
func IndexKey2TempIndexKey(key []byte) {
	idxIDBytes := key[prefixLen : prefixLen+idLen]
	idxID := codec.DecodeCmpUintToInt(binary.BigEndian.Uint64(idxIDBytes))
	eid := codec.EncodeIntToCmpUint(TempIndexPrefix | idxID)
	binary.BigEndian.PutUint64(key[prefixLen:], eid)
}

// TempIndexKey2IndexKey generates an index key from temporary index key.
func TempIndexKey2IndexKey(tempIdxKey []byte) {
	tmpIdxIDBytes := tempIdxKey[prefixLen : prefixLen+idLen]
	tempIdxID := codec.DecodeCmpUintToInt(binary.BigEndian.Uint64(tmpIdxIDBytes))
	eid := codec.EncodeIntToCmpUint(tempIdxID & IndexIDMask)
	binary.BigEndian.PutUint64(tempIdxKey[prefixLen:], eid)
}

// IsTempIndexKey checks whether the input key is for a temp index.
func IsTempIndexKey(indexKey []byte) (isTemp bool) {
	indexIDKey := indexKey[prefixLen : prefixLen+8]
	indexID := codec.DecodeCmpUintToInt(binary.BigEndian.Uint64(indexIDKey))
	tempIndexID := int64(TempIndexPrefix) | indexID
	return tempIndexID == indexID
}

// TempIndexValueFlag is the flag of temporary index value.
type TempIndexValueFlag byte

const (
	// TempIndexValueFlagNormal means the following value is a distinct the normal index value.
	TempIndexValueFlagNormal TempIndexValueFlag = iota
	// TempIndexValueFlagNonDistinctNormal means the following value is the non-distinct normal index value.
	TempIndexValueFlagNonDistinctNormal
	// TempIndexValueFlagDeleted means the following value is the distinct and deleted index value.
	TempIndexValueFlagDeleted
	// TempIndexValueFlagNonDistinctDeleted means the following value is the non-distinct deleted index value.
	TempIndexValueFlagNonDistinctDeleted
)

// TempIndexValue is the value of temporary index.
// It contains one or more element, each element represents a history index operations on the original index.
// A temp index value element is encoded as one of:
//   - [flag 1 byte][value_length 2 bytes ] [value value_len bytes]   [key_version 1 byte] {distinct normal}
//   - [flag 1 byte][value value_len bytes]                           [key_version 1 byte] {non-distinct normal}
//   - [flag 1 byte][handle_length 2 bytes] [handle handle_len bytes] [key_version 1 byte] {distinct deleted}
//   - [flag 1 byte]                                                  [key_version 1 byte] {non-distinct deleted}
//
// The temp index value is encoded as:
//   - [element 1][element 2]...[element n] {for distinct values}
//   - [element 1]                          {for non-distinct values}
type TempIndexValue []*TempIndexValueElem

// IsEmpty checks whether the value is empty.
func (v TempIndexValue) IsEmpty() bool {
	return len(v) == 0
}

// Current returns the current latest temp index value.
func (v TempIndexValue) Current() *TempIndexValueElem {
	return v[len(v)-1]
}

// FilterOverwritten is used by the temp index merge process to remove the overwritten index operations.
// For example, the value {temp_idx_key -> [h2, h2d, h3, h1d]} recorded four operations on the original index.
// Since 'h2d' overwrites 'h2', we can remove 'h2' from the value.
func (v TempIndexValue) FilterOverwritten() TempIndexValue {
	if len(v) <= 1 || !v[0].Distinct {
		return v
	}
	occurred := kv.NewHandleMap()
	for i := len(v) - 1; i >= 0; i-- {
		if _, ok := occurred.Get(v[i].Handle); !ok {
			occurred.Set(v[i].Handle, struct{}{})
		} else {
			v[i] = nil
		}
	}
	ret := v[:0]
	for _, elem := range v {
		if elem != nil {
			ret = append(ret, elem)
		}
	}
	return ret
}

// TempIndexValueElem represents a history index operations on the original index.
// A temp index value element is encoded as one of:
//   - [flag 1 byte][value_length 2 bytes ] [value value_len bytes]   [key_version 1 byte] {distinct normal}
//   - [flag 1 byte][value value_len bytes]                           [key_version 1 byte] {non-distinct normal}
//   - [flag 1 byte][handle_length 2 bytes] [handle handle_len bytes] [partitionIdFlag 1 byte] [partitionID 8 bytes] [key_version 1 byte] {distinct deleted}
//   - [flag 1 byte]                                                  [key_version 1 byte] {non-distinct deleted}
type TempIndexValueElem struct {
	Value    []byte
	Handle   kv.Handle
	KeyVer   byte
	Delete   bool
	Distinct bool

	// Global means it's a global Index, for partitioned tables. Currently only used in `distinct` + `deleted` scenarios.
	Global bool
}

const (
	// TempIndexKeyTypeNone means the key is not a temporary index key.
	TempIndexKeyTypeNone byte = 0
	// TempIndexKeyTypeDelete indicates this value is written in the delete-only stage.
	TempIndexKeyTypeDelete byte = 'd'
	// TempIndexKeyTypeBackfill indicates this value is written in the backfill stage.
	TempIndexKeyTypeBackfill byte = 'b'
	// TempIndexKeyTypeMerge indicates this value is written in the merge stage.
	TempIndexKeyTypeMerge byte = 'm'
	// TempIndexKeyTypePartitionIDFlag indicates the following value is partition id.
	TempIndexKeyTypePartitionIDFlag byte = 'p'
)

// Encode encodes the temp index value.
func (v *TempIndexValueElem) Encode(buf []byte) []byte {
	if v.Delete {
		if v.Distinct {
			handle := v.Handle
			var hEncoded []byte
			var hLen uint16
			if handle.IsInt() {
				hEncoded = codec.EncodeUint(hEncoded, uint64(handle.IntValue()))
				hLen = 8
			} else {
				hEncoded = handle.Encoded()
				hLen = uint16(len(hEncoded))
			}
			// flag + handle length + handle + [partition id] + temp key version
			if buf == nil {
				l := hLen + 4
				if v.Global {
					l += 9
				}
				buf = make([]byte, 0, l)
			}
			buf = append(buf, byte(TempIndexValueFlagDeleted))
			buf = append(buf, byte(hLen>>8), byte(hLen))
			buf = append(buf, hEncoded...)
			if v.Global {
				buf = append(buf, TempIndexKeyTypePartitionIDFlag)
				buf = append(buf, codec.EncodeInt(nil, v.Handle.(kv.PartitionHandle).PartitionID)...)
			}
			buf = append(buf, v.KeyVer)
			return buf
		}
		// flag + temp key version
		if buf == nil {
			buf = make([]byte, 0, 2)
		}
		buf = append(buf, byte(TempIndexValueFlagNonDistinctDeleted))
		buf = append(buf, v.KeyVer)
		return buf
	}
	if v.Distinct {
		// flag + value length + value + temp key version
		if buf == nil {
			buf = make([]byte, 0, len(v.Value)+4)
		}
		buf = append(buf, byte(TempIndexValueFlagNormal))
		vLen := uint16(len(v.Value))
		buf = append(buf, byte(vLen>>8), byte(vLen))
		buf = append(buf, v.Value...)
		buf = append(buf, v.KeyVer)
		return buf
	}
	// flag + value + temp key version
	if buf == nil {
		buf = make([]byte, 0, len(v.Value)+2)
	}
	buf = append(buf, byte(TempIndexValueFlagNonDistinctNormal))
	buf = append(buf, v.Value...)
	buf = append(buf, v.KeyVer)
	return buf
}

// DecodeTempIndexValue decodes the temp index value.
func DecodeTempIndexValue(value []byte) (TempIndexValue, error) {
	var (
		values []*TempIndexValueElem
		err    error
	)
	for len(value) > 0 {
		v := &TempIndexValueElem{}
		value, err = v.DecodeOne(value)
		if err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, nil
}

// DecodeOne decodes one temp index value element.
func (v *TempIndexValueElem) DecodeOne(b []byte) (remain []byte, err error) {
	flag := TempIndexValueFlag(b[0])
	b = b[1:]
	switch flag {
	case TempIndexValueFlagNormal:
		vLen := (uint16(b[0]) << 8) + uint16(b[1])
		b = b[2:]
		v.Value = b[:vLen]
		b = b[vLen:]
		v.KeyVer = b[0]
		b = b[1:]
		v.Distinct = true
		return b, err
	case TempIndexValueFlagNonDistinctNormal:
		v.Value = b[:len(b)-1]
		v.KeyVer = b[len(b)-1]
		return nil, nil
	case TempIndexValueFlagDeleted:
		hLen := (uint16(b[0]) << 8) + uint16(b[1])
		b = b[2:]
		if hLen == idLen {
			v.Handle = DecodeIntHandleInIndexValue(b[:idLen])
		} else {
			v.Handle, _ = kv.NewCommonHandle(b[:hLen])
		}
		b = b[hLen:]
		if b[0] == TempIndexKeyTypePartitionIDFlag {
			v.Global = true
			var pid int64
			_, pid, err = codec.DecodeInt(b[1:9])
			if err != nil {
				return nil, err
			}
			v.Handle = kv.NewPartitionHandle(pid, v.Handle)
			b = b[9:]
		}
		v.KeyVer = b[0]
		b = b[1:]
		v.Distinct = true
		v.Delete = true
		return b, nil
	case TempIndexValueFlagNonDistinctDeleted:
		v.KeyVer = b[0]
		b = b[1:]
		v.Delete = true
		return b, nil
	default:
		return nil, errors.New("invalid temp index value")
	}
}

// TempIndexValueIsUntouched returns true if the value is untouched.
// All the temp index value has the suffix of temp key version.
// All the temp key versions differ from the uncommitted KV flag.
func TempIndexValueIsUntouched(b []byte) bool {
	if len(b) > 0 && b[len(b)-1] == kv.UnCommitIndexKVFlag {
		return true
	}
	return false
}

// GenIndexValuePortal is the portal for generating index value.
// Value layout:
//
//	+-- IndexValueVersion0  (with restore data, or common handle, or index is global)
//	|
//	|  Layout: TailLen | Options      | Padding      | [IntHandle] | [UntouchedFlag]
//	|  Length:   1     | len(options) | len(padding) |    8        |     1
//	|
//	|  TailLen:       len(padding) + len(IntHandle) + len(UntouchedFlag)
//	|  Options:       Encode some value for new features, such as common handle, new collations or global index.
//	|                 See below for more information.
//	|  Padding:       Ensure length of value always >= 10. (or >= 11 if UntouchedFlag exists.)
//	|  IntHandle:     Only exists when table use int handles and index is unique.
//	|  UntouchedFlag: Only exists when index is untouched.
//	|
//	+-- Old Encoding (without restore data, integer handle, local)
//	|
//	|  Layout: [Handle] | [UntouchedFlag]
//	|  Length:   8      |     1
//	|
//	|  Handle:        Only exists in unique index.
//	|  UntouchedFlag: Only exists when index is untouched.
//	|
//	|  If neither Handle nor UntouchedFlag exists, value will be one single byte '0' (i.e. []byte{'0'}).
//	|  Length of value <= 9, use to distinguish from the new encoding.
//	|
//	+-- IndexValueForClusteredIndexVersion1
//	|
//	|  Layout: TailLen |    VersionFlag  |    Version     ｜ Options      |   [UntouchedFlag]
//	|  Length:   1     |        1        |      1         |  len(options) |         1
//	|
//	|  TailLen:       len(UntouchedFlag)
//	|  Options:       Encode some value for new features, such as common handle, new collations or global index.
//	|                 See below for more information.
//	|  UntouchedFlag: Only exists when index is untouched.
//	|
//	|  Layout of Options:
//	|
//	|     Segment:             Common Handle                 |     Global Index      |   New Collation
//	|     Layout:  CHandle flag | CHandle Len | CHandle      | PidFlag | PartitionID |    restoreData
//	|     Length:     1         | 2           | len(CHandle) |    1    |    8        |   len(restoreData)
//	|
//	|     Common Handle Segment: Exists when unique index used common handles.
//	|     Global Index Segment:  Exists when index is global.
//	|     New Collation Segment: Exists when new collation is used and index or handle contains non-binary string.
//	|     In v4.0, restored data contains all the index values. For example, (a int, b char(10)) and index (a, b).
//	|     The restored data contains both the values of a and b.
//	|     In v5.0, restored data contains only non-binary data(except for char and _bin). In the above example, the restored data contains only the value of b.
//	|     Besides, if the collation of b is _bin, then restored data is an integer indicate the spaces are truncated. Then we use sortKey
//	|     and the restored data together to restore original data.
func GenIndexValuePortal(loc *time.Location, tblInfo *model.TableInfo, idxInfo *model.IndexInfo,
	needRestoredData bool, distinct bool, untouched bool, indexedValues []types.Datum, h kv.Handle,
	partitionID int64, restoredData []types.Datum, buf []byte) ([]byte, error) {
	if tblInfo.IsCommonHandle && tblInfo.CommonHandleVersion == 1 {
		return GenIndexValueForClusteredIndexVersion1(loc, tblInfo, idxInfo, needRestoredData, distinct, untouched, indexedValues, h, partitionID, restoredData, buf)
	}
	return genIndexValueVersion0(loc, tblInfo, idxInfo, needRestoredData, distinct, untouched, indexedValues, h, partitionID, buf)
}

// TryGetCommonPkColumnRestoredIds get the IDs of primary key columns which need restored data if the table has common handle.
// Caller need to make sure the table has common handle.
func TryGetCommonPkColumnRestoredIds(tbl *model.TableInfo) []int64 {
	var pkColIDs []int64
	var pkIdx *model.IndexInfo
	for _, idx := range tbl.Indices {
		if idx.Primary {
			pkIdx = idx
			break
		}
	}
	if pkIdx == nil {
		return pkColIDs
	}
	for _, idxCol := range pkIdx.Columns {
		if types.NeedRestoredData(&tbl.Columns[idxCol.Offset].FieldType) {
			pkColIDs = append(pkColIDs, tbl.Columns[idxCol.Offset].ID)
		}
	}
	return pkColIDs
}

// GenIndexValueForClusteredIndexVersion1 generates the index value for the clustered index with version 1(New in v5.0.0).
func GenIndexValueForClusteredIndexVersion1(loc *time.Location, tblInfo *model.TableInfo, idxInfo *model.IndexInfo,
	idxValNeedRestoredData bool, distinct bool, untouched bool, indexedValues []types.Datum, h kv.Handle,
	partitionID int64, handleRestoredData []types.Datum, buf []byte) ([]byte, error) {
	var idxVal []byte
	if buf == nil {
		idxVal = make([]byte, 0)
	} else {
		idxVal = buf[:0]
	}
	idxVal = append(idxVal, 0)
	tailLen := 0
	// Version info.
	idxVal = append(idxVal, IndexVersionFlag)
	idxVal = append(idxVal, byte(1))

	if distinct {
		idxVal = encodeCommonHandle(idxVal, h)
	}
	if idxInfo.Global {
		idxVal = encodePartitionID(idxVal, partitionID)
	}
	if idxValNeedRestoredData || len(handleRestoredData) > 0 {
		colIds := make([]int64, 0, len(idxInfo.Columns))
		allRestoredData := make([]types.Datum, 0, len(handleRestoredData)+len(idxInfo.Columns))
		for i, idxCol := range idxInfo.Columns {
			col := tblInfo.Columns[idxCol.Offset]
			// If  the column is the primary key's column,
			// the restored data will be written later. Skip writing it here to avoid redundancy.
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				continue
			}
			if types.NeedRestoredData(&col.FieldType) {
				colIds = append(colIds, col.ID)
				if collate.IsBinCollation(col.GetCollate()) {
					allRestoredData = append(allRestoredData, types.NewUintDatum(uint64(stringutil.GetTailSpaceCount(indexedValues[i].GetString()))))
				} else {
					allRestoredData = append(allRestoredData, indexedValues[i])
				}
			}
		}

		if len(handleRestoredData) > 0 {
			pkColIDs := TryGetCommonPkColumnRestoredIds(tblInfo)
			colIds = append(colIds, pkColIDs...)
			allRestoredData = append(allRestoredData, handleRestoredData...)
		}

		rd := rowcodec.Encoder{Enable: true}
		var err error
		idxVal, err = rd.Encode(loc, colIds, allRestoredData, nil, idxVal)
		if err != nil {
			return nil, err
		}
	}

	if untouched {
		tailLen = 1
		idxVal = append(idxVal, kv.UnCommitIndexKVFlag)
	}
	idxVal[0] = byte(tailLen)

	return idxVal, nil
}

// genIndexValueVersion0 create index value for both local and global index.
func genIndexValueVersion0(loc *time.Location, tblInfo *model.TableInfo, idxInfo *model.IndexInfo,
	idxValNeedRestoredData bool, distinct bool, untouched bool, indexedValues []types.Datum, h kv.Handle,
	partitionID int64, buf []byte) ([]byte, error) {
	var idxVal []byte
	if buf == nil {
		idxVal = make([]byte, 0)
	} else {
		idxVal = buf[:0]
	}
	idxVal = append(idxVal, 0)
	newEncode := false
	tailLen := 0
	if !h.IsInt() && distinct {
		idxVal = encodeCommonHandle(idxVal, h)
		newEncode = true
	}
	if idxInfo.Global {
		idxVal = encodePartitionID(idxVal, partitionID)
		newEncode = true
	}
	if idxValNeedRestoredData {
		colIds := make([]int64, len(idxInfo.Columns))
		for i, col := range idxInfo.Columns {
			colIds[i] = tblInfo.Columns[col.Offset].ID
		}
		rd := rowcodec.Encoder{Enable: true}
		// Encode row restored value.
		var err error
		idxVal, err = rd.Encode(loc, colIds, indexedValues, nil, idxVal)
		if err != nil {
			return nil, err
		}
		newEncode = true
	}

	if newEncode {
		if h.IsInt() && distinct {
			// The len of the idxVal is always >= 10 since len (restoredValue) > 0.
			tailLen += 8
			idxVal = append(idxVal, EncodeHandleInUniqueIndexValue(h, false)...)
		} else if len(idxVal) < 10 {
			// Padding the len to 10
			paddingLen := 10 - len(idxVal)
			tailLen += paddingLen
			idxVal = append(idxVal, bytes.Repeat([]byte{0x0}, paddingLen)...)
		}
		if untouched {
			// If index is untouched and fetch here means the key is exists in TiKV, but not in txn mem-buffer,
			// then should also write the untouched index key/value to mem-buffer to make sure the data
			// is consistent with the index in txn mem-buffer.
			tailLen++
			idxVal = append(idxVal, kv.UnCommitIndexKVFlag)
		}
		idxVal[0] = byte(tailLen)
	} else {
		// Old index value encoding.
		if buf == nil {
			idxVal = make([]byte, 0)
		} else {
			idxVal = buf[:0]
		}
		if distinct {
			idxVal = EncodeHandleInUniqueIndexValue(h, untouched)
		}
		if untouched {
			// If index is untouched and fetch here means the key is exists in TiKV, but not in txn mem-buffer,
			// then should also write the untouched index key/value to mem-buffer to make sure the data
			// is consistent with the index in txn mem-buffer.
			idxVal = append(idxVal, kv.UnCommitIndexKVFlag)
		}
		if len(idxVal) == 0 {
			idxVal = append(idxVal, byte('0'))
		}
	}
	return idxVal, nil
}

// TruncateIndexValues truncates the index values created using only the leading part of column values.
func TruncateIndexValues(tblInfo *model.TableInfo, idxInfo *model.IndexInfo, indexedValues []types.Datum) {
	for i := range indexedValues {
		idxCol := idxInfo.Columns[i]
		tblCol := tblInfo.Columns[idxCol.Offset]
		TruncateIndexValue(&indexedValues[i], idxCol, tblCol)
	}
}

// TruncateIndexValue truncate one value in the index.
func TruncateIndexValue(v *types.Datum, idxCol *model.IndexColumn, tblCol *model.ColumnInfo) {
	noPrefixIndex := idxCol.Length == types.UnspecifiedLength
	if noPrefixIndex {
		return
	}
	notStringType := v.Kind() != types.KindString && v.Kind() != types.KindBytes
	if notStringType {
		return
	}
	colValue := v.GetBytes()
	if tblCol.GetCharset() == charset.CharsetBin || tblCol.GetCharset() == charset.CharsetASCII {
		// Count character length by bytes if charset is binary or ascii.
		if len(colValue) > idxCol.Length {
			// truncate value and limit its length
			if v.Kind() == types.KindBytes {
				v.SetBytes(colValue[:idxCol.Length])
			} else {
				v.SetString(v.GetString()[:idxCol.Length], tblCol.GetCollate())
			}
		}
	} else if utf8.RuneCount(colValue) > idxCol.Length {
		// Count character length by characters for other rune-based charsets, they are all internally encoded as UTF-8.
		rs := bytes.Runes(colValue)
		truncateStr := string(rs[:idxCol.Length])
		// truncate value and limit its length
		v.SetString(truncateStr, tblCol.GetCollate())
	}
}

// EncodeHandleInUniqueIndexValue encodes handle in data.
func EncodeHandleInUniqueIndexValue(h kv.Handle, isUntouched bool) []byte {
	if h.IsInt() {
		var data [8]byte
		binary.BigEndian.PutUint64(data[:], uint64(h.IntValue()))
		return data[:]
	}
	var untouchedFlag byte
	if isUntouched {
		untouchedFlag = 1
	}
	return encodeCommonHandle([]byte{untouchedFlag}, h)
}

func encodeCommonHandle(idxVal []byte, h kv.Handle) []byte {
	idxVal = append(idxVal, CommonHandleFlag)
	hLen := uint16(len(h.Encoded()))
	idxVal = append(idxVal, byte(hLen>>8), byte(hLen))
	idxVal = append(idxVal, h.Encoded()...)
	return idxVal
}

func encodePartitionID(idxVal []byte, partitionID int64) []byte {
	idxVal = append(idxVal, PartitionIDFlag)
	idxVal = codec.EncodeInt(idxVal, partitionID)
	return idxVal
}

// IndexValueSegments use to store result of SplitIndexValue.
type IndexValueSegments struct {
	CommonHandle   []byte
	PartitionID    []byte
	RestoredValues []byte
	IntHandle      []byte
}

// SplitIndexValue decodes segments in index value for both non-clustered and clustered table.
func SplitIndexValue(value []byte) (segs IndexValueSegments) {
	if getIndexVersion(value) == 0 {
		// For Old Encoding (IntHandle without any others options)
		if len(value) <= MaxOldEncodeValueLen {
			segs.IntHandle = value
			return segs
		}
		// For IndexValueVersion0
		return splitIndexValueForIndexValueVersion0(value)
	}
	// For IndexValueForClusteredIndexVersion1
	return splitIndexValueForClusteredIndexVersion1(value)
}

// splitIndexValueForIndexValueVersion0 splits index value into segments.
func splitIndexValueForIndexValueVersion0(value []byte) (segs IndexValueSegments) {
	tailLen := int(value[0])
	tail := value[len(value)-tailLen:]
	value = value[1 : len(value)-tailLen]
	if len(tail) >= 8 {
		segs.IntHandle = tail[:8]
	}
	if len(value) > 0 && value[0] == CommonHandleFlag {
		handleLen := uint16(value[1])<<8 + uint16(value[2])
		handleEndOff := 3 + handleLen
		segs.CommonHandle = value[3:handleEndOff]
		value = value[handleEndOff:]
	}
	if len(value) > 0 && value[0] == PartitionIDFlag {
		segs.PartitionID = value[1:9]
		value = value[9:]
	}
	if len(value) > 0 && value[0] == RestoreDataFlag {
		segs.RestoredValues = value
	}
	return
}

// splitIndexValueForClusteredIndexVersion1 splits index value into segments.
func splitIndexValueForClusteredIndexVersion1(value []byte) (segs IndexValueSegments) {
	tailLen := int(value[0])
	// Skip the tailLen and version info.
	value = value[3 : len(value)-tailLen]
	if len(value) > 0 && value[0] == CommonHandleFlag {
		handleLen := uint16(value[1])<<8 + uint16(value[2])
		handleEndOff := 3 + handleLen
		segs.CommonHandle = value[3:handleEndOff]
		value = value[handleEndOff:]
	}
	if len(value) > 0 && value[0] == PartitionIDFlag {
		segs.PartitionID = value[1:9]
		value = value[9:]
	}
	if len(value) > 0 && value[0] == RestoreDataFlag {
		segs.RestoredValues = value
	}
	return
}

func decodeIndexKvForClusteredIndexVersion1(key, value []byte, colsLen int, hdStatus HandleStatus, columns []rowcodec.ColInfo) ([][]byte, error) {
	var resultValues [][]byte
	var keySuffix []byte
	var handle kv.Handle
	var err error
	segs := splitIndexValueForClusteredIndexVersion1(value)
	resultValues, keySuffix, err = CutIndexKeyNew(key, colsLen)
	if err != nil {
		return nil, err
	}
	if segs.RestoredValues != nil {
		resultValues, err = decodeRestoredValuesV5(columns[:colsLen], resultValues, segs.RestoredValues)
		if err != nil {
			return nil, err
		}
	}
	if hdStatus == HandleNotNeeded {
		return resultValues, nil
	}
	if segs.CommonHandle != nil {
		// In unique common handle index.
		handle, err = kv.NewCommonHandle(segs.CommonHandle)
	} else {
		// In non-unique index, decode handle in keySuffix.
		handle, err = kv.NewCommonHandle(keySuffix)
	}
	if err != nil {
		return nil, err
	}
	handleBytes, err := reEncodeHandleConsiderNewCollation(handle, columns[colsLen:], segs.RestoredValues)
	if err != nil {
		return nil, err
	}
	resultValues = append(resultValues, handleBytes...)
	if segs.PartitionID != nil {
		_, pid, err := codec.DecodeInt(segs.PartitionID)
		if err != nil {
			return nil, err
		}
		datum := types.NewIntDatum(pid)
		pidBytes, err := codec.EncodeValue(time.UTC, nil, datum)
		if err != nil {
			return nil, err
		}
		resultValues = append(resultValues, pidBytes)
	}
	return resultValues, nil
}

// decodeIndexKvGeneral decodes index key value pair of new layout in an extensible way.
func decodeIndexKvGeneral(key, value []byte, colsLen int, hdStatus HandleStatus, columns []rowcodec.ColInfo) ([][]byte, error) {
	var resultValues [][]byte
	var keySuffix []byte
	var handle kv.Handle
	var err error
	segs := splitIndexValueForIndexValueVersion0(value)
	resultValues, keySuffix, err = CutIndexKeyNew(key, colsLen)
	if err != nil {
		return nil, err
	}
	if segs.RestoredValues != nil { // new collation
		resultValues, err = decodeRestoredValues(columns[:colsLen], segs.RestoredValues)
		if err != nil {
			return nil, err
		}
	}
	if hdStatus == HandleNotNeeded {
		return resultValues, nil
	}

	if segs.IntHandle != nil {
		// In unique int handle index.
		handle = DecodeIntHandleInIndexValue(segs.IntHandle)
	} else if segs.CommonHandle != nil {
		// In unique common handle index.
		handle, err = decodeHandleInIndexKey(segs.CommonHandle)
		if err != nil {
			return nil, err
		}
	} else {
		// In non-unique index, decode handle in keySuffix
		handle, err = decodeHandleInIndexKey(keySuffix)
		if err != nil {
			return nil, err
		}
	}
	handleBytes, err := reEncodeHandle(handle, hdStatus == HandleIsUnsigned)
	if err != nil {
		return nil, err
	}
	resultValues = append(resultValues, handleBytes...)
	if segs.PartitionID != nil {
		_, pid, err := codec.DecodeInt(segs.PartitionID)
		if err != nil {
			return nil, err
		}
		datum := types.NewIntDatum(pid)
		pidBytes, err := codec.EncodeValue(time.UTC, nil, datum)
		if err != nil {
			return nil, err
		}
		resultValues = append(resultValues, pidBytes)
	}
	return resultValues, nil
}

// IndexKVIsUnique uses to judge if an index is unique, it can handle the KV committed by txn already, it doesn't consider the untouched flag.
func IndexKVIsUnique(value []byte) bool {
	if len(value) <= MaxOldEncodeValueLen {
		return len(value) == 8
	}
	if getIndexVersion(value) == 1 {
		segs := splitIndexValueForClusteredIndexVersion1(value)
		return segs.CommonHandle != nil
	}
	segs := splitIndexValueForIndexValueVersion0(value)
	return segs.IntHandle != nil || segs.CommonHandle != nil
}

// VerifyTableIDForRanges verifies that all given ranges are valid to decode the table id.
func VerifyTableIDForRanges(keyRanges *kv.KeyRanges) ([]int64, error) {
	tids := make([]int64, 0, keyRanges.PartitionNum())
	collectFunc := func(ranges []kv.KeyRange, _ []int) error {
		if len(ranges) == 0 {
			return nil
		}
		tid := DecodeTableID(ranges[0].StartKey)
		if tid <= 0 {
			return errors.New("Incorrect keyRange is constrcuted")
		}
		tids = append(tids, tid)
		for i := 1; i < len(ranges); i++ {
			tmpTID := DecodeTableID(ranges[i].StartKey)
			if tmpTID <= 0 {
				return errors.New("Incorrect keyRange is constrcuted")
			}
			if tid != tmpTID {
				return errors.Errorf("Using multi partition's ranges as single table's")
			}
		}
		return nil
	}
	err := keyRanges.ForEachPartitionWithErr(collectFunc)
	return tids, err
}
