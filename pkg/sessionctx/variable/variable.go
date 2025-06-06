// Copyright 2021 PingCAP, Inc.
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

package variable

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"golang.org/x/exp/maps"
)

// SysVar is for system variable.
// All the fields of SysVar should be READ ONLY after created.
type SysVar struct {
	// Scope is for whether can be changed or not
	Scope vardef.ScopeFlag
	// Name is the variable name.
	Name string
	// Value is the variable value.
	Value string
	// Type is the MySQL type (optional)
	Type vardef.TypeFlag
	// MinValue will automatically be validated when specified (optional)
	MinValue int64
	// MaxValue will automatically be validated when specified (optional)
	MaxValue uint64
	// AutoConvertNegativeBool applies to boolean types (optional)
	AutoConvertNegativeBool bool
	// ReadOnly applies to all types
	ReadOnly bool
	// PossibleValues applies to ENUM type
	PossibleValues []string
	// AllowEmpty is a special TiDB behavior which means "read value from config" (do not use)
	AllowEmpty bool
	// AllowEmptyAll is a special behavior that only applies to TiDBCapturePlanBaseline, TiDBTxnMode (do not use)
	AllowEmptyAll bool
	// AllowAutoValue means that the special value "-1" is permitted, even when outside of range.
	AllowAutoValue bool
	// Validation is a callback after the type validation has been performed, but before the Set function
	Validation func(*SessionVars, string, string, vardef.ScopeFlag) (string, error)
	// SetSession is called after validation but before updating systems[]. It also doubles as an Init function
	// and will be called on all variables in builtinGlobalVariable, regardless of their scope.
	SetSession func(*SessionVars, string) error
	// SetGlobal is called after validation
	SetGlobal func(context.Context, *SessionVars, string) error
	// IsHintUpdatableVerified indicate whether we've confirmed that SET_VAR() hint is worked for this hint.
	IsHintUpdatableVerified bool
	// Deprecated: Hidden previously meant that the variable still responds to SET but doesn't show up in SHOW VARIABLES
	// However, this feature is no longer used. All variables are visible.
	Hidden bool
	// Aliases is a list of sysvars that should also be updated when this sysvar is updated.
	// Updating aliases calls the SET function of the aliases, but does not update their aliases (preventing SET recursion)
	Aliases []string
	// GetSession is a getter function for session scope.
	// It can be used by instance-scoped variables to overwrite the previously expected value.
	GetSession func(*SessionVars) (string, error)
	// GetGlobal is a getter function for global scope.
	GetGlobal func(context.Context, *SessionVars) (string, error)
	// GetStateValue gets the value for session states, which is used for migrating sessions.
	// We need a function to override GetSession sometimes, because GetSession may not return the real value.
	// The first return value must be a valid value for the variable, and the second return value must be
	// true if and only if the variable has been changed from the default.
	GetStateValue func(*SessionVars) (string, bool, error)
	// Depended indicates whether other variables depend on this one. That is, if this one is not correctly set,
	// another variable cannot be set either.
	// This flag is used to decide the order to replay session variables.
	Depended bool
	// skipInit defines if the sysvar should be loaded into the session on init.
	// This is only important to set for sysvars that include session scope,
	// since global scoped sysvars are not-applicable.
	skipInit bool
	// IsNoop defines if the sysvar is a noop included for MySQL compatibility
	IsNoop bool
	// GlobalConfigName is the global config name of this global variable.
	// If the global variable has the global config name,
	// it should store the global config into PD(etcd) too when set global variable.
	GlobalConfigName string
	// RequireDynamicPrivileges is a function to return a dynamic privilege list to check the set sysvar privilege
	RequireDynamicPrivileges func(isGlobal bool, sem bool) []string
}

// GetGlobalFromHook calls the GetSession func if it exists.
func (sv *SysVar) GetGlobalFromHook(ctx context.Context, s *SessionVars) (string, error) {
	// Call the Getter if there is one defined.
	if sv.GetGlobal != nil {
		val, err := sv.GetGlobal(ctx, s)
		if err != nil {
			return val, err
		}
		// Ensure that the results from the getter are validated
		// Since some are read directly from tables.
		return sv.ValidateWithRelaxedValidation(s, val, vardef.ScopeGlobal), nil
	}
	if sv.HasNoneScope() {
		return sv.Value, nil
	}
	return s.GlobalVarsAccessor.GetGlobalSysVar(sv.Name)
}

// GetSessionFromHook calls the GetSession func if it exists.
func (sv *SysVar) GetSessionFromHook(s *SessionVars) (string, error) {
	if sv.HasNoneScope() {
		return sv.Value, nil
	}
	// Call the Getter if there is one defined.
	if sv.GetSession != nil {
		val, err := sv.GetSession(s)
		if err != nil {
			return val, err
		}
		// Ensure that the results from the getter are validated
		// Since some are read directly from tables.
		return sv.ValidateWithRelaxedValidation(s, val, vardef.ScopeSession), nil
	}
	var (
		ok  bool
		val string
	)
	if val, ok = s.systems[sv.Name]; !ok {
		return val, errors.New("sysvar has not yet loaded")
	}
	return val, nil
}

// SetSessionFromHook calls the SetSession func if it exists.
func (sv *SysVar) SetSessionFromHook(s *SessionVars, val string) error {
	if sv.SetSession != nil {
		if err := sv.SetSession(s, val); err != nil {
			return err
		}
	}
	s.systems[sv.Name] = val

	// Call the Set function on all the aliases for this sysVar
	// Skipping the validation function, and not calling aliases of
	// aliases. By skipping the validation function it means that things
	// like duplicate warnings should not appear.

	if sv.Aliases != nil {
		for _, aliasName := range sv.Aliases {
			aliasSv := GetSysVar(aliasName)
			if aliasSv.SetSession != nil {
				if err := aliasSv.SetSession(s, val); err != nil {
					return err
				}
			}
			s.systems[aliasSv.Name] = val
		}
	}
	return nil
}

// SetGlobalFromHook calls the SetGlobal func if it exists.
func (sv *SysVar) SetGlobalFromHook(ctx context.Context, s *SessionVars, val string, skipAliases bool) error {
	if sv.SetGlobal != nil {
		return sv.SetGlobal(ctx, s, val)
	}

	// Call the SetGlobalSysVarOnly function on all the aliases for this sysVar
	// which skips the validation function and when SetGlobalFromHook is called again
	// it will be with skipAliases=true. This helps break recursion because
	// most aliases are reciprocal.

	if !skipAliases && sv.Aliases != nil {
		for _, aliasName := range sv.Aliases {
			if err := s.GlobalVarsAccessor.SetGlobalSysVarOnly(ctx, aliasName, val, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// HasNoneScope returns true if the scope for the sysVar is None.
func (sv *SysVar) HasNoneScope() bool {
	return sv.Scope == vardef.ScopeNone
}

// HasSessionScope returns true if the scope for the sysVar includes session.
func (sv *SysVar) HasSessionScope() bool {
	return sv.Scope&vardef.ScopeSession != 0
}

// HasGlobalScope returns true if the scope for the sysVar includes global.
func (sv *SysVar) HasGlobalScope() bool {
	return sv.Scope&vardef.ScopeGlobal != 0
}

// HasInstanceScope returns true if the scope for the sysVar includes instance
func (sv *SysVar) HasInstanceScope() bool {
	return sv.Scope&vardef.ScopeInstance != 0
}

// Validate checks if system variable satisfies specific restriction.
func (sv *SysVar) Validate(vars *SessionVars, value string, scope vardef.ScopeFlag) (string, error) {
	// Check that the scope is correct first.
	if err := sv.validateScope(scope); err != nil {
		return value, err
	}
	// Normalize the value and apply validation based on type.
	// i.e. TypeBool converts 1/on/ON to ON.
	normalizedValue, err := sv.ValidateFromType(vars, value, scope)
	if err != nil {
		return normalizedValue, err
	}
	// If type validation was successful, call the (optional) validation function
	if sv.Validation != nil {
		return sv.Validation(vars, normalizedValue, value, scope)
	}
	return normalizedValue, nil
}

// ValidateFromType provides automatic validation based on the SysVar's type
func (sv *SysVar) ValidateFromType(vars *SessionVars, value string, scope vardef.ScopeFlag) (string, error) {
	// Some sysvars in TiDB have a special behavior where the empty string means
	// "use the config file value". This needs to be cleaned up once the behavior
	// for instance variables is determined.
	if value == "" && ((sv.AllowEmpty && scope == vardef.ScopeSession) || sv.AllowEmptyAll) {
		return value, nil
	}
	// Provide validation using the SysVar struct
	switch sv.Type {
	case vardef.TypeUnsigned:
		return sv.checkUInt64SystemVar(value, vars)
	case vardef.TypeInt:
		return sv.checkInt64SystemVar(value, vars)
	case vardef.TypeBool:
		return sv.checkBoolSystemVar(value, vars)
	case vardef.TypeFloat:
		return sv.checkFloatSystemVar(value, vars)
	case vardef.TypeEnum:
		return sv.checkEnumSystemVar(value, vars)
	case vardef.TypeTime:
		return sv.checkTimeSystemVar(value, vars)
	case vardef.TypeDuration:
		return sv.checkDurationSystemVar(value, vars)
	}
	return value, nil // typeString
}

func (sv *SysVar) validateScope(scope vardef.ScopeFlag) error {
	if sv.ReadOnly || sv.Scope == vardef.ScopeNone {
		return ErrIncorrectScope.FastGenByArgs(sv.Name, "read only")
	}
	if scope == vardef.ScopeGlobal && !(sv.HasGlobalScope() || sv.HasInstanceScope()) {
		return errLocalVariable.FastGenByArgs(sv.Name)
	}
	if scope == vardef.ScopeSession && !sv.HasSessionScope() {
		return errGlobalVariable.FastGenByArgs(sv.Name)
	}
	return nil
}

// ValidateWithRelaxedValidation normalizes values but can not return errors.
// Normalization+validation needs to be applied when reading values because older versions of TiDB
// may be less sophisticated in normalizing values. But errors should be caught and handled,
// because otherwise there will be upgrade issues.
func (sv *SysVar) ValidateWithRelaxedValidation(vars *SessionVars, value string, scope vardef.ScopeFlag) string {
	warns := vars.StmtCtx.GetWarnings()
	defer func() {
		vars.StmtCtx.SetWarnings(warns) // RelaxedValidation = trim warnings too.
	}()
	normalizedValue, err := sv.ValidateFromType(vars, value, scope)
	if err != nil {
		return normalizedValue
	}
	if sv.Validation != nil {
		normalizedValue, err = sv.Validation(vars, normalizedValue, value, scope)
		if err != nil {
			return normalizedValue
		}
	}
	return normalizedValue
}

func (sv *SysVar) checkTimeSystemVar(value string, vars *SessionVars) (string, error) {
	var t time.Time
	var err error
	if len(value) <= len(vardef.LocalDayTimeFormat) {
		t, err = time.ParseInLocation(vardef.LocalDayTimeFormat, value, vars.Location())
	} else {
		t, err = time.ParseInLocation(vardef.FullDayTimeFormat, value, vars.Location())
	}
	if err != nil {
		return "", err
	}
	// Add a modern date to it, as the timezone shift can differ across the history
	// For example, the Asia/Shanghai refers to +08:05 before 1900
	now := time.Now()
	t = time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
	return t.Format(vardef.FullDayTimeFormat), nil
}

func (sv *SysVar) checkDurationSystemVar(value string, vars *SessionVars) (string, error) {
	d, err := time.ParseDuration(value)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	// Check for min/max violations
	if int64(d) < sv.MinValue {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(sv.Name, value))
		return time.Duration(sv.MinValue).String(), nil
	}
	if uint64(d) > sv.MaxValue {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(sv.Name, value))
		return time.Duration(sv.MaxValue).String(), nil
	}
	// return a string representation of the duration
	return d.String(), nil
}

func (sv *SysVar) checkUInt64SystemVar(value string, vars *SessionVars) (string, error) {
	if sv.AllowAutoValue && value == "-1" {
		return value, nil
	}
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if value[0] == '-' {
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
		}
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(sv.Name, value))
		return strconv.FormatInt(sv.MinValue, 10), nil
	}
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < uint64(sv.MinValue) {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(sv.Name, value))
		return strconv.FormatInt(sv.MinValue, 10), nil
	}
	if val > sv.MaxValue {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(sv.Name, value))
		return strconv.FormatUint(sv.MaxValue, 10), nil
	}
	return value, nil
}

func (sv *SysVar) checkInt64SystemVar(value string, vars *SessionVars) (string, error) {
	if sv.AllowAutoValue && value == "-1" {
		return value, nil
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < sv.MinValue {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(sv.Name, value))
		return strconv.FormatInt(sv.MinValue, 10), nil
	}
	if val > int64(sv.MaxValue) {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(sv.Name, value))
		return strconv.FormatUint(sv.MaxValue, 10), nil
	}
	return value, nil
}

func (sv *SysVar) checkEnumSystemVar(value string, vars *SessionVars) (string, error) {
	// The value could be either a string or the ordinal position in the PossibleValues.
	// This allows for the behavior 0 = OFF, 1 = ON, 2 = DEMAND etc.
	var iStr string
	for i, v := range sv.PossibleValues {
		iStr = strconv.Itoa(i)
		if strings.EqualFold(value, v) || strings.EqualFold(value, iStr) {
			return v, nil
		}
	}
	return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
}

func (sv *SysVar) checkFloatSystemVar(value string, vars *SessionVars) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < float64(sv.MinValue) {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(sv.Name, value))
		return strconv.FormatInt(sv.MinValue, 10), nil
	}
	if val > float64(sv.MaxValue) {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.FastGenByArgs(sv.Name, value))
		return strconv.FormatUint(sv.MaxValue, 10), nil
	}
	return value, nil
}

func (sv *SysVar) checkBoolSystemVar(value string, vars *SessionVars) (string, error) {
	if strings.EqualFold(value, "ON") {
		return vardef.On, nil
	} else if strings.EqualFold(value, "OFF") {
		return vardef.Off, nil
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		// There are two types of conversion rules for integer values.
		// The default only allows 0 || 1, but a subset of values convert any
		// negative integer to 1.
		if !sv.AutoConvertNegativeBool {
			if val == 0 {
				return vardef.Off, nil
			} else if val == 1 {
				return vardef.On, nil
			}
		} else {
			if val == 1 || val < 0 {
				return vardef.On, nil
			} else if val == 0 {
				return vardef.Off, nil
			}
		}
	}
	return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
}

// GetNativeValType attempts to convert the val to the approx MySQL non-string type
// TODO: only return 3 types now, support others like DOUBLE, TIME later
func (sv *SysVar) GetNativeValType(val string) (types.Datum, byte, uint) {
	switch sv.Type {
	case vardef.TypeUnsigned:
		u, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			u = 0
		}
		return types.NewUintDatum(u), mysql.TypeLonglong, mysql.UnsignedFlag | mysql.BinaryFlag
	case vardef.TypeBool:
		optVal := int64(0) // OFF
		if TiDBOptOn(val) {
			optVal = 1
		}
		return types.NewIntDatum(optVal), mysql.TypeLonglong, mysql.BinaryFlag
	}
	return types.NewStringDatum(val), mysql.TypeVarString, 0
}

// SkipInit returns true if when a new session is created we should "skip" copying
// an initial value to it (and call the SetSession func if it exists)
func (sv *SysVar) SkipInit() bool {
	if sv.skipInit || sv.IsNoop {
		return true
	}
	return !sv.HasSessionScope()
}

// SkipSysvarCache returns true if the sysvar should not re-execute on peers
// This doesn't make sense for the GC variables because they are based in tikv
// tables. We'd effectively be reading and writing to the same table, which
// could be in an unsafe manner. In future these variables might be converted
// to not use a different table internally, but to do that we need to first
// fix upgrade/downgrade so we know that older servers won't be in the cluster
// which update only these values.
func (sv *SysVar) SkipSysvarCache() bool {
	switch sv.Name {
	case vardef.TiDBGCEnable, vardef.TiDBGCRunInterval, vardef.TiDBGCLifetime,
		vardef.TiDBGCConcurrency, vardef.TiDBGCScanLockMode, vardef.TiDBExternalTS:
		return true
	}
	return false
}

var sysVars map[string]*SysVar
var sysVarsLock sync.RWMutex

// RegisterSysVar adds a sysvar to the SysVars list
func RegisterSysVar(sv *SysVar) {
	name := strings.ToLower(sv.Name)
	sysVarsLock.Lock()
	sysVars[name] = sv
	sysVarsLock.Unlock()
}

// UnregisterSysVar removes a sysvar from the SysVars list
// currently only used in tests.
func UnregisterSysVar(name string) {
	name = strings.ToLower(name)
	sysVarsLock.Lock()
	delete(sysVars, name)
	sysVarsLock.Unlock()
}

// GetSysVar returns sys var info for name as key.
func GetSysVar(name string) *SysVar {
	name = strings.ToLower(name)
	sysVarsLock.RLock()
	defer sysVarsLock.RUnlock()

	return sysVars[name]
}

// SetSysVar sets a sysvar. In fact, SysVar is immutable.
// SetSysVar is implemented by register a new SysVar with the same name again.
// This will not propagate to the cluster, so it should only be
// used for instance scoped AUTO variables such as system_time_zone.
func SetSysVar(name string, value string) {
	old := GetSysVar(name)
	tmp := *old
	tmp.Value = value
	RegisterSysVar(&tmp)
}

// GetSysVars deep copies the sysVars list under a RWLock
func GetSysVars() map[string]*SysVar {
	sysVarsLock.RLock()
	defer sysVarsLock.RUnlock()
	m := make(map[string]*SysVar, len(sysVars))
	for name, sv := range sysVars {
		tmp := *sv
		m[name] = &tmp
	}
	return m
}

// OrderByDependency orders the vars by dependency. The depended sys vars are in the front.
// Unknown sys vars are treated as not depended.
func OrderByDependency(names map[string]string) []string {
	depended, notDepended := make([]string, 0, len(names)), make([]string, 0, len(names))
	sysVarsLock.RLock()
	defer sysVarsLock.RUnlock()
	for name := range names {
		if sv, ok := sysVars[name]; ok && sv.Depended {
			depended = append(depended, name)
		} else {
			notDepended = append(notDepended, name)
		}
	}
	return append(depended, notDepended...)
}

func init() {
	sysVars = make(map[string]*SysVar)
	setHintUpdatable(defaultSysVars)
	// Destroy the map after init.
	maps.Clear(isHintUpdatableVerified)
	for _, v := range defaultSysVars {
		RegisterSysVar(v)
	}
	for _, v := range noopSysVars {
		v.IsNoop = true
		RegisterSysVar(v)
	}
}

// GlobalVarAccessor is the interface for accessing global scope system and status variables.
type GlobalVarAccessor interface {
	// GetGlobalSysVar gets the global system variable value for name.
	GetGlobalSysVar(name string) (string, error)
	// SetGlobalSysVar sets the global system variable name to value.
	SetGlobalSysVar(ctx context.Context, name string, value string) error
	// SetGlobalSysVarOnly sets the global system variable without calling the validation function or updating aliases.
	SetGlobalSysVarOnly(ctx context.Context, name string, value string, updateLocal bool) error
	// GetTiDBTableValue gets a value from mysql.tidb for the key 'name'
	GetTiDBTableValue(name string) (string, error)
	// SetTiDBTableValue sets a value+comment for the mysql.tidb key 'name'
	SetTiDBTableValue(name, value, comment string) error
}
