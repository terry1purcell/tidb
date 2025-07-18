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

package executor_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestExplainAnalyzeMemory(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int, k int, key(k))")
	tk.MustExec("insert into t values (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)")

	checkMemoryInfo(t, tk, "explain analyze select * from t order by v")
	checkMemoryInfo(t, tk, "explain analyze select * from t order by v limit 5")
	checkMemoryInfo(t, tk, "explain analyze select /*+ HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.v = t2.v+1")
	checkMemoryInfo(t, tk, "explain analyze select /*+ MERGE_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k+1")
	checkMemoryInfo(t, tk, "explain analyze select /*+ INL_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	checkMemoryInfo(t, tk, "explain analyze select /*+ INL_HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	checkMemoryInfo(t, tk, "explain analyze select /*+ INL_MERGE_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	checkMemoryInfo(t, tk, "explain analyze select sum(k) from t group by v")
	checkMemoryInfo(t, tk, "explain analyze select sum(v) from t group by k")
	checkMemoryInfo(t, tk, "explain analyze select * from t")
	checkMemoryInfo(t, tk, "explain analyze select k from t use index(k)")
	checkMemoryInfo(t, tk, "explain analyze select * from t use index(k)")
	checkMemoryInfo(t, tk, "explain analyze select v+k from t")
}

func checkMemoryInfo(t *testing.T, tk *testkit.TestKit, sql string) {
	memCol := 6
	ops := []string{"Join", "Reader", "Top", "Sort", "LookUp", "Projection", "Selection", "Agg"}
	rows := tk.MustQuery(sql).Rows()
	for _, row := range rows {
		strs := make([]string, len(row))
		for i, c := range row {
			strs[i] = c.(string)
		}
		if strings.Contains(strs[3], "cop") {
			continue
		}

		shouldHasMem := false
		for _, op := range ops {
			if strings.Contains(strs[0], op) {
				shouldHasMem = true
				break
			}
		}

		if shouldHasMem {
			require.NotEqual(t, "N/A", strs[memCol])
		} else {
			require.Equal(t, "N/A", strs[memCol])
		}
	}
}

func TestMemoryAndDiskUsageAfterClose(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int, k int, key(k))")
	batch := 128
	limit := tk.Session().GetSessionVars().MaxChunkSize*2 + 10
	var buf bytes.Buffer
	for i := 0; i < limit; {
		buf.Reset()
		_, err := buf.WriteString("insert into t values ")
		require.NoError(t, err)
		for j := 0; j < batch && i < limit; i, j = i+1, j+1 {
			if j > 0 {
				_, err = buf.WriteString(", ")
				require.NoError(t, err)
			}
			_, err = buf.WriteString(fmt.Sprintf("(%v,%v)", i, i))
			require.NoError(t, err)
		}
		tk.MustExec(buf.String())
	}
	SQLs := []string{"select v+abs(k) from t",
		"select v from t where abs(v) > 0",
		"select v from t order by v",
		"select count(v) from t",            // StreamAgg
		"select count(v) from t group by v", // HashAgg
	}
	for _, sql := range SQLs {
		tk.MustQuery(sql)
		require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed())
		require.Greater(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), int64(0))
		require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.DiskTracker.BytesConsumed())
	}
}

func TestExplainAnalyzeExecutionInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v int, k int, key(k))")
	tk.MustExec("insert into t values (1, 1), (1, 1), (1, 1), (1, 1), (1, 1)")

	checkExecutionInfo(t, tk, "explain analyze select * from t order by v")
	checkExecutionInfo(t, tk, "explain analyze select * from t order by v limit 5")
	checkExecutionInfo(t, tk, "explain analyze select /*+ HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.v = t2.v+1")
	checkExecutionInfo(t, tk, "explain analyze select /*+ MERGE_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k+1")
	checkExecutionInfo(t, tk, "explain analyze select /*+ INL_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	checkExecutionInfo(t, tk, "explain analyze select /*+ INL_HASH_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	checkExecutionInfo(t, tk, "explain analyze select /*+ INL_MERGE_JOIN(t1, t2) */ t1.k from t t1, t t2 where t1.k = t2.k and t1.v=1")
	checkExecutionInfo(t, tk, "explain analyze select sum(k) from t group by v")
	checkExecutionInfo(t, tk, "explain analyze select sum(v) from t group by k")
	checkExecutionInfo(t, tk, "explain analyze select * from t")
	checkExecutionInfo(t, tk, "explain analyze select k from t use index(k)")
	checkExecutionInfo(t, tk, "explain analyze select * from t use index(k)")
	checkExecutionInfo(t, tk, "explain analyze with recursive cte(a) as (select 1 union select a + 1 from cte where a < 1000) select * from cte;")

	tk.MustExec("set @@foreign_key_checks=0")
	tk.MustExec("CREATE TABLE IF NOT EXISTS nation  ( N_NATIONKEY  BIGINT NOT NULL,N_NAME       CHAR(25) NOT NULL,N_REGIONKEY  BIGINT NOT NULL,N_COMMENT    VARCHAR(152),PRIMARY KEY (N_NATIONKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS part  ( P_PARTKEY     BIGINT NOT NULL,P_NAME        VARCHAR(55) NOT NULL,P_MFGR        CHAR(25) NOT NULL,P_BRAND       CHAR(10) NOT NULL,P_TYPE        VARCHAR(25) NOT NULL,P_SIZE        BIGINT NOT NULL,P_CONTAINER   CHAR(10) NOT NULL,P_RETAILPRICE DECIMAL(15,2) NOT NULL,P_COMMENT     VARCHAR(23) NOT NULL,PRIMARY KEY (P_PARTKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS supplier  ( S_SUPPKEY     BIGINT NOT NULL,S_NAME        CHAR(25) NOT NULL,S_ADDRESS     VARCHAR(40) NOT NULL,S_NATIONKEY   BIGINT NOT NULL,S_PHONE       CHAR(15) NOT NULL,S_ACCTBAL     DECIMAL(15,2) NOT NULL,S_COMMENT     VARCHAR(101) NOT NULL,PRIMARY KEY (S_SUPPKEY),CONSTRAINT FOREIGN KEY SUPPLIER_FK1 (S_NATIONKEY) references nation(N_NATIONKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS partsupp ( PS_PARTKEY     BIGINT NOT NULL,PS_SUPPKEY     BIGINT NOT NULL,PS_AVAILQTY    BIGINT NOT NULL,PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,PS_COMMENT     VARCHAR(199) NOT NULL,PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY),CONSTRAINT FOREIGN KEY PARTSUPP_FK1 (PS_SUPPKEY) references supplier(S_SUPPKEY),CONSTRAINT FOREIGN KEY PARTSUPP_FK2 (PS_PARTKEY) references part(P_PARTKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS orders  ( O_ORDERKEY       BIGINT NOT NULL,O_CUSTKEY        BIGINT NOT NULL,O_ORDERSTATUS    CHAR(1) NOT NULL,O_TOTALPRICE     DECIMAL(15,2) NOT NULL,O_ORDERDATE      DATE NOT NULL,O_ORDERPRIORITY  CHAR(15) NOT NULL,O_CLERK          CHAR(15) NOT NULL,O_SHIPPRIORITY   BIGINT NOT NULL,O_COMMENT        VARCHAR(79) NOT NULL,PRIMARY KEY (O_ORDERKEY),CONSTRAINT FOREIGN KEY ORDERS_FK1 (O_CUSTKEY) references customer(C_CUSTKEY));")
	tk.MustExec("CREATE TABLE IF NOT EXISTS lineitem ( L_ORDERKEY    BIGINT NOT NULL,L_PARTKEY     BIGINT NOT NULL,L_SUPPKEY     BIGINT NOT NULL,L_LINENUMBER  BIGINT NOT NULL,L_QUANTITY    DECIMAL(15,2) NOT NULL,L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,L_DISCOUNT    DECIMAL(15,2) NOT NULL,L_TAX         DECIMAL(15,2) NOT NULL,L_RETURNFLAG  CHAR(1) NOT NULL,L_LINESTATUS  CHAR(1) NOT NULL,L_SHIPDATE    DATE NOT NULL,L_COMMITDATE  DATE NOT NULL,L_RECEIPTDATE DATE NOT NULL,L_SHIPINSTRUCT CHAR(25) NOT NULL,L_SHIPMODE     CHAR(10) NOT NULL,L_COMMENT      VARCHAR(44) NOT NULL,PRIMARY KEY (L_ORDERKEY,L_LINENUMBER),CONSTRAINT FOREIGN KEY LINEITEM_FK1 (L_ORDERKEY)  references orders(O_ORDERKEY),CONSTRAINT FOREIGN KEY LINEITEM_FK2 (L_PARTKEY,L_SUPPKEY) references partsupp(PS_PARTKEY, PS_SUPPKEY));")

	checkExecutionInfo(t, tk, "select nation, o_year, sum(amount) as sum_profit from ( select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%dim%' ) as profit group by nation, o_year order by nation, o_year desc;")

	tk.MustExec("drop table if exists nation")
	tk.MustExec("drop table if exists part")
	tk.MustExec("drop table if exists supplier")
	tk.MustExec("drop table if exists partsupp")
	tk.MustExec("drop table if exists orders")
	tk.MustExec("drop table if exists lineitem")
}

func checkExecutionInfo(t *testing.T, tk *testkit.TestKit, sql string) {
	executionInfoCol := 4
	rows := tk.MustQuery(sql).Rows()
	for _, row := range rows {
		strs := make([]string, len(row))
		for i, c := range row {
			strs[i] = c.(string)
		}
		require.NotEqual(t, "time:0s, loops:0, rows:0", strs[executionInfoCol])
	}
}

func checkActRows(t *testing.T, tk *testkit.TestKit, sql string, expected []string) {
	actRowsCol := 2
	rows := tk.MustQuery("explain analyze " + sql).Rows()
	require.Equal(t, len(expected), len(rows))
	for id, row := range rows {
		strs := make([]string, len(row))
		for i, c := range row {
			strs[i] = c.(string)
		}

		require.Equal(t, expected[id], strs[actRowsCol], fmt.Sprintf("error comparing %s", sql))
	}
}

func TestCheckActRowsWithUnistore(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableCollectExecutionInfo = true
	})
	store := testkit.CreateMockStore(t)
	// testSuite1 use default mockstore which is unistore
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t_unistore_act_rows")
	tk.MustExec("create table t_unistore_act_rows(a int, b int, index(a, b))")
	tk.MustExec("insert into t_unistore_act_rows values (1, 0), (1, 0), (2, 0), (2, 1)")
	tk.MustExec("analyze table t_unistore_act_rows")
	tk.MustExec("set @@tidb_merge_join_concurrency= 5;")

	type testStruct struct {
		sql      string
		expected []string
	}

	tests := []testStruct{
		{
			sql:      "select * from t_unistore_act_rows",
			expected: []string{"4", "4"},
		},
		{
			sql:      "select * from t_unistore_act_rows where a > 1",
			expected: []string{"2", "2"},
		},
		{
			sql:      "select * from t_unistore_act_rows where a > 1 and b > 0",
			expected: []string{"1", "1", "2"},
		},
		{
			sql:      "select b from t_unistore_act_rows",
			expected: []string{"4", "4"},
		},
		{
			sql:      "select * from t_unistore_act_rows where b > 0",
			expected: []string{"1", "1", "4"},
		},
		{
			sql:      "select count(*) from t_unistore_act_rows",
			expected: []string{"1", "1", "1", "4"},
		},
		{
			sql:      "select count(*) from t_unistore_act_rows group by a",
			expected: []string{"2", "2", "2", "4"},
		},
		{
			sql:      "select count(*) from t_unistore_act_rows group by b",
			expected: []string{"2", "4", "4"},
		},
		{
			sql:      "with cte(a) as (select a from t_unistore_act_rows) select (select 1 from cte limit 1) from cte;",
			expected: []string{"4", "1", "1", "1", "4", "4", "4", "4", "4"},
		},
		{
			sql:      "select a, row_number() over (partition by b) from t_unistore_act_rows;",
			expected: []string{"4", "4", "4", "4", "4", "4", "4"},
		},
		{
			sql:      "select /*+ merge_join(t1, t2) */ * from t_unistore_act_rows t1 join t_unistore_act_rows t2 on t1.b = t2.b;",
			expected: []string{"10", "10", "4", "4", "4", "4", "4", "4", "4", "4", "4", "4"},
		},
	}

	// Default RPC encoding may cause statistics explain result differ and then the test unstable.
	tk.MustExec("set @@tidb_enable_chunk_rpc = on")

	for _, test := range tests {
		checkActRows(t, tk, test.sql, test.expected)
	}
}

func TestExplainAnalyzeCTEMemoryAndDiskInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t with recursive cte(a) as (select 1 union select a + 1 from cte where a < 1000) select * from cte;")

	rows := tk.MustQuery("explain analyze with recursive cte(a) as (select 1 union select a + 1 from cte where a < 1000)" +
		" select * from cte, t;").Rows()

	require.NotEqual(t, "N/A", rows[4][7].(string))
	require.Equal(t, "0 Bytes", rows[4][8].(string))

	tk.MustExec("set @@tidb_mem_quota_query=10240;")
	rows = tk.MustQuery("explain analyze with recursive cte(a) as (select 1 union select a + 1 from cte where a < 1000)" +
		" select * from cte, t;").Rows()

	require.NotEqual(t, "N/A", rows[4][7].(string))
	require.NotEqual(t, "N/A", rows[4][8].(string))
}

func TestIssue35296AndIssue43024(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int , c int, d int, e int, primary key(a), index ib(b), index ic(c), index idd(d), index ie(e));")

	rows := tk.MustQuery("explain analyze select * from t where a = 10 or b = 30 or c = 10 or d = 1 or e = 90;").Rows()

	require.Contains(t, rows[0][0], "IndexMerge")
	require.NotRegexp(t, "^time:0s", rows[1][5])
	require.NotRegexp(t, "^time:0s", rows[2][5])
	require.NotRegexp(t, "^time:0s", rows[3][5])
	require.NotRegexp(t, "^time:0s", rows[4][5])
	require.NotRegexp(t, "^time:0s", rows[5][5])
}

func TestIssue35911(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a int, b int);")
	tk.MustExec("create table t2(a int, b int, index ia(a));")
	tk.MustExec("insert into t1 value (1,1), (2,2), (3,3), (4,4), (5,5), (6,6);")
	tk.MustExec("insert into t2 value (1,1), (2,2), (3,3), (4,4), (5,5), (6,6);")
	tk.MustExec("set @@tidb_executor_concurrency = 5;")

	// case 1 of #35911
	tk.MustExec("set @@tidb_enable_parallel_apply = 0;")
	rows := tk.MustQuery("explain analyze select * from t1 where exists (select tt1.* from (select * from t2 where a = t1.b) as tt1 join (select * from t2 where a = t1.b) as tt2 on tt1.b = tt2.b);").Rows()

	extractTime, err := regexp.Compile("^time:(.*?),")
	require.NoError(t, err)
	timeStr1 := extractTime.FindStringSubmatch(rows[4][5].(string))[1]
	time1, err := time.ParseDuration(timeStr1)
	require.NoError(t, err)
	extractTime2, _ := regexp.Compile("^total_time:(.*?),")
	timeStr2 := extractTime2.FindStringSubmatch(rows[5][5].(string))[1]
	time2, err := time.ParseDuration(timeStr2)
	require.NoError(t, err)
	// The duration of IndexLookUp should be longer than its build side child
	require.LessOrEqual(t, time2, time1)

	// case 2 of #35911
	tk.MustExec("set @@tidb_enable_parallel_apply = 1;")
	rows = tk.MustQuery("explain analyze select * from t1 where exists (select tt1.* from (select * from t2 where a = t1.b) as tt1 join (select * from t2 where a = t1.b) as tt2 on tt1.b = tt2.b);").Rows()

	extractConcurrency, err := regexp.Compile(`table_task: [{].*concurrency: (\d+)[}]`)
	require.NoError(t, err)
	concurrencyStr := extractConcurrency.FindStringSubmatch(rows[4][5].(string))[1]
	concurrency, err := strconv.ParseInt(concurrencyStr, 10, 64)
	require.NoError(t, err)
	// To be consistent with other operators, we should not aggregate the concurrency in the runtime stats.
	require.EqualValues(t, 5, concurrency)
}

func TestTotalTimeCases(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 bigint, c2 int, c3 int, c4 int, primary key(c1, c2), index (c3));")
	lineNum := 1000
	for i := range lineNum {
		tk.MustExec(fmt.Sprintf("insert into t1 values(%d, %d, %d, %d);", i, i+1, i+2, i+3))
	}
	tk.MustExec("analyze table t1")
	tk.MustExec("set @@tidb_executor_concurrency = 5;")

	tk.MustExec("set @@tidb_enable_parallel_apply = 0;")
	rows := tk.MustQuery("explain analyze select (select /*+ NO_DECORRELATE() */ sum(c4) from t1 where t1.c3 = alias.c3) from t1 alias where alias.c1 = 1;").Rows()
	require.True(t, len(rows) == 11)

	// Line3 is tikv_task, others should be all walltime
	for i := range 11 {
		if i != 3 {
			require.True(t, strings.HasPrefix(rows[i][5].(string), "time:"))
		}
	}

	// use parallel_apply
	tk.MustExec("set @@tidb_enable_parallel_apply = 1;")
	rows = tk.MustQuery("explain analyze select (select /*+ NO_DECORRELATE() */ sum(c4) from t1 where t1.c3 = alias.c3) from t1 alias where alias.c1 = 1;").Rows()
	require.True(t, len(rows) == 11)
	// Line0-2 is walltime, Line3 is tikv_task, Line9 Line10 are special, they are total time in integration environment, while
	// walltime in uts due to only one IndexLookUp executor is actually open, others should be all total_time.
	for i := range 11 {
		if i == 9 || i == 10 {
			continue
		}
		if i < 3 {
			require.True(t, strings.HasPrefix(rows[i][5].(string), "time:"))
		} else if i > 3 {
			require.True(t, strings.HasPrefix(rows[i][5].(string), "total_time:"))
		}
	}
}

func flatJSONPlan(j *plannercore.ExplainInfoForEncode) (res []*plannercore.ExplainInfoForEncode) {
	if j == nil {
		return
	}
	res = append(res, j)
	for _, child := range j.SubOperators {
		res = append(res, flatJSONPlan(child)...)
	}
	return
}

func TestExplainJSON(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(id int, key(id))")
	tk.MustExec("create table t2(id int, key(id))")
	cases := []string{
		"select * from t1",
		"select count(*) from t2",
		"select * from t1, t2 where t1.id = t2.id",
		"select /*+ merge_join(t1, t2)*/ * from t1, t2 where t1.id = t2.id",
		"with top10 as ( select * from t1 order by id desc limit 10 ) select * from top10 where id in (1,2)",
		"insert into t1 values(1)",
		"delete from t2 where t2.id > 10",
		"update t2 set id = 1 where id =2",
		"select * from t1 where t1.id < (select sum(t2.id) from t2 where t2.id = t1.id)",
	}
	// test syntax
	tk.MustExec("explain format = 'tidb_json' select * from t1")
	tk.MustExec("explain format = tidb_json select * from t1")
	tk.MustExec("explain format = 'TIDB_JSON' select * from t1")
	tk.MustExec("explain format = TIDB_JSON select * from t1")
	tk.MustExec("explain analyze format = 'tidb_json' select * from t1")
	tk.MustExec("explain analyze format = tidb_json select * from t1")
	tk.MustExec("explain analyze format = 'TIDB_JSON' select * from t1")
	tk.MustExec("explain analyze format = TIDB_JSON select * from t1")

	// explain
	for _, sql := range cases {
		jsonForamt := "explain format = tidb_json " + sql
		rowForamt := "explain format = row " + sql
		resJSON := tk.MustQuery(jsonForamt).Rows()
		resRow := tk.MustQuery(rowForamt).Rows()

		j := new([]*plannercore.ExplainInfoForEncode)
		require.NoError(t, json.Unmarshal([]byte(resJSON[0][0].(string)), j))
		var flatJSONRows []*plannercore.ExplainInfoForEncode
		for _, row := range *j {
			flatJSONRows = append(flatJSONRows, flatJSONPlan(row)...)
		}
		require.Equal(t, len(flatJSONRows), len(resRow))

		for i, row := range resRow {
			require.Contains(t, row[0], flatJSONRows[i].ID)
			require.Equal(t, flatJSONRows[i].EstRows, row[1])
			require.Equal(t, flatJSONRows[i].TaskType, row[2])
			require.Equal(t, flatJSONRows[i].AccessObject, row[3])
			require.Equal(t, flatJSONRows[i].OperatorInfo, row[4])
		}
	}

	// explain analyze
	for _, sql := range cases {
		jsonForamt := "explain analyze format = tidb_json " + sql
		rowForamt := "explain analyze format = row " + sql
		resJSON := tk.MustQuery(jsonForamt).Rows()
		resRow := tk.MustQuery(rowForamt).Rows()

		j := new([]*plannercore.ExplainInfoForEncode)
		require.NoError(t, json.Unmarshal([]byte(resJSON[0][0].(string)), j))
		var flatJSONRows []*plannercore.ExplainInfoForEncode
		for _, row := range *j {
			flatJSONRows = append(flatJSONRows, flatJSONPlan(row)...)
		}
		require.Equal(t, len(flatJSONRows), len(resRow))

		for i, row := range resRow {
			require.Contains(t, row[0], flatJSONRows[i].ID)
			require.Equal(t, flatJSONRows[i].EstRows, row[1])
			require.Equal(t, flatJSONRows[i].ActRows, row[2])
			require.Equal(t, flatJSONRows[i].TaskType, row[3])
			require.Equal(t, flatJSONRows[i].AccessObject, row[4])
			require.Equal(t, flatJSONRows[i].OperatorInfo, row[6])
			// executeInfo, memory, disk maybe vary in multi execution
			require.NotEqual(t, flatJSONRows[i].ExecuteInfo, "")
			require.NotEqual(t, flatJSONRows[i].MemoryInfo, "")
			require.NotEqual(t, flatJSONRows[i].DiskInfo, "")
		}
	}
}

func TestExplainFormatInCtx(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("set @@session.tidb_enable_non_prepared_plan_cache = 1")

	explainFormats := []string{
		types.ExplainFormatBrief,
		types.ExplainFormatDOT,
		types.ExplainFormatHint,
		types.ExplainFormatROW,
		types.ExplainFormatVerbose,
		types.ExplainFormatTraditional,
		types.ExplainFormatBinary,
		types.ExplainFormatTiDBJSON,
		types.ExplainFormatCostTrace,
		types.ExplainFormatPlanCache,
	}

	tk.MustExec("select * from t")
	tk.MustExec("explain analyze select * from t")
	require.Equal(t, tk.Session().GetSessionVars().StmtCtx.InExplainStmt, true)
	require.Equal(t, tk.Session().GetSessionVars().StmtCtx.ExplainFormat, types.ExplainFormatROW)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	for _, format := range explainFormats {
		tk.MustExec(fmt.Sprintf("explain analyze format = '%v' select * from t", format))
		require.Equal(t, tk.Session().GetSessionVars().StmtCtx.InExplainStmt, true)
		require.Equal(t, tk.Session().GetSessionVars().StmtCtx.ExplainFormat, format)
		if format != types.ExplainFormatPlanCache {
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		} else {
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
		}
	}
}

func TestExplainImportFromSelect(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key)")
	tk.MustExec("create table t_o (a int primary key)")
	tk.MustExec("insert into t values (2)")
	rs := tk.MustQuery("explain import into t_o from select * from t").Rows()
	require.Contains(t, rs[0][0], "ImportInto")
	require.Contains(t, rs[1][0], "TableReader")
	require.Contains(t, rs[2][0], "TableFullScan")
}
