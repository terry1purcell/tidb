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

package core_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// firstRowPerRangeQueries compute MIN or MAX per namespace. The derived-table
// forms put the aggregation, as a root stream aggregation, a root hash
// aggregation or a pushed-down hash aggregation, on the inner side of an index
// join, which is the shape the rewrite handles. On these small tables the
// scalar subquery forms pull the aggregation above the join instead, so only
// their results are checked.
var firstRowPerRangeQueries = []struct {
	query      string
	isMax      bool
	aggIsInner bool
}{
	{"select /*+ inl_join(s@sel_2) */ t.ns_id, (select min(s.id) from %[1]s s where s.ns_id = t.ns_id) m from t where t.ns_id > 0 order by t.ns_id limit 100", false, false},
	{"select /*+ inl_join(s@sel_2) */ t.ns_id, (select max(s.id) from %[1]s s where s.ns_id = t.ns_id) m from t where t.ns_id > 0 order by t.ns_id limit 100", true, false},
	{"select /*+ inl_join(m) */ t.ns_id, m.m from t left join (select ns_id, min(id) m from %[1]s group by ns_id) m on t.ns_id = m.ns_id where t.ns_id > 0 order by t.ns_id limit 100", false, true},
	{"select /*+ inl_join(m) */ t.ns_id, m.m from t left join (select /*+ stream_agg() */ ns_id, min(id) m from %[1]s group by ns_id) m on t.ns_id = m.ns_id where t.ns_id > 0 order by t.ns_id limit 100", false, true},
	{"select /*+ inl_join(m) */ t.ns_id, m.m from t left join (select /*+ hash_agg() agg_to_cop() */ ns_id, max(id) m from %[1]s group by ns_id) m on t.ns_id = m.ns_id where t.ns_id > 0 order by t.ns_id limit 100", true, true},
}

func checkFirstRowPerRange(t *testing.T, tk *testkit.TestKit, table string, wantRewrite bool, minRows, maxRows [][]any) {
	for _, c := range firstRowPerRangeQueries {
		q := fmt.Sprintf(c.query, table)
		plan := tk.MustQuery("explain format='brief' " + q).Rows()
		var planText strings.Builder
		for _, r := range plan {
			planText.WriteString(fmt.Sprintln(r...))
		}
		require.Equal(t, wantRewrite && c.aggIsInner, strings.Contains(planText.String(), "first row per range"), "%s\n%s", q, planText.String())
		if c.aggIsInner {
			require.Contains(t, planText.String(), "IndexJoin", q)
		}
		want := minRows
		if c.isMax {
			want = maxRows
		}
		tk.MustQuery(q).Check(want)
	}
}

func TestIndexJoinMinFirstRowPerRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (ns_id bigint primary key, v int)")
	tk.MustExec("insert into t values (1,1),(2,2),(3,3),(4,4),(5,5)")

	// Small groups: reading each group whole is cheaper, so the rewrite is skipped.
	tk.MustExec("create table s (ns_id bigint not null, id bigint not null, a int, primary key (ns_id, id) clustered)")
	tk.MustExec("insert into s values (1,10,0),(1,5,0),(1,7,0),(2,3,0),(2,9,0),(4,1,0),(4,2,0)")

	// Large groups: 300 rows in each of namespaces 1, 2 and 4, ids k*1000+1 .. k*1000+300.
	tk.MustExec("create table big (ns_id bigint not null, id bigint not null, a int, primary key (ns_id, id) clustered)")
	tk.MustExec("insert into big with recursive r(n) as (select 1 union all select n+1 from r where n < 300) " +
		"select k.ns_id, k.ns_id*1000 + r.n, 0 from r join (select 1 ns_id union all select 2 union all select 4) k")
	tk.MustExec("analyze table t, s, big")
	// Split namespace 1 across regions so its range becomes two cop tasks.
	tk.MustQuery("split table s by (1, 7), (2, 0)").Check(testkit.Rows("2 1"))
	tk.MustQuery("split table big by (1, 1150), (2, 0)").Check(testkit.Rows("2 1"))

	checkFirstRowPerRange(t, tk, "s", false,
		testkit.Rows("1 5", "2 3", "3 <nil>", "4 1", "5 <nil>"),
		testkit.Rows("1 10", "2 9", "3 <nil>", "4 2", "5 <nil>"))
	checkFirstRowPerRange(t, tk, "big", true,
		testkit.Rows("1 1001", "2 2001", "3 <nil>", "4 4001", "5 <nil>"),
		testkit.Rows("1 1300", "2 2300", "3 <nil>", "4 4300", "5 <nil>"))
}
