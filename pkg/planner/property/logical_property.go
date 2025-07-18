// Copyright 2018 PingCAP, Inc.
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

package property

import (
	"github.com/pingcap/tidb/pkg/expression"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
)

// LogicalProperty stands for logical properties such as schema of expression,
// or statistics of columns in schema for output of Group.
// All group expressions in a group share same logical property.
type LogicalProperty struct {
	Stats         *StatsInfo
	Schema        *expression.Schema
	FD            *fd.FDSet
	MaxOneRow     bool
	PossibleProps [][]*expression.Column
}

// NewLogicalProp returns a new empty LogicalProperty.
func NewLogicalProp() *LogicalProperty {
	return &LogicalProperty{}
}

// todo: ScalarProperty: usedColumns in current scalar expr, null reject, cor-related, subq contained and so on
