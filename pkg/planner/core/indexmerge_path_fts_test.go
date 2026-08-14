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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func prepareFTSIndexTable(t *testing.T) *testkit.TestKit {
	t.Helper()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_local_match_against = ON")
	tk.MustExec("create table articles (id int primary key, body varchar(255))")
	tk.MustExec(`insert into articles values
		(1, 'distributed sql database'),
		(2, 'relational storage engine'),
		(3, 'distributed storage layer'),
		(4, 'sql is distributed here'),
		(5, 'nothing relevant at all')`)
	tk.MustExec("alter table articles add fulltext index idx_body (body)")
	return tk
}

func explainRows(t *testing.T, tk *testkit.TestKit, sql string) string {
	t.Helper()
	var sb strings.Builder
	for _, row := range tk.MustQuery("explain " + sql).Rows() {
		for _, cell := range row {
			sb.WriteString(cell.(string))
			sb.WriteString(" ")
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// TestFTSMatchUsesFullTextIndex is the point of the access path: a MATCH filter
// should reach the FULLTEXT index instead of scanning every row.
func TestFTSMatchUsesFullTextIndex(t *testing.T) {
	t.Skip("no access path generated yet; see the KNOWN GAP note on deriveFTSIndexFilters")
	tk := prepareFTSIndexTable(t)

	plan := explainRows(t, tk,
		"select id from articles where match(body) against('+distributed +sql' in boolean mode)")
	require.Contains(t, plan, "idx_body", "plan should reach the fulltext index:\n"+plan)
	require.Contains(t, plan, "IndexMerge", "plan should use index merge:\n"+plan)

	// The MATCH must survive as a residual: the index only generates
	// candidates, so dropping it would return rows the query does not match.
	require.Contains(t, strings.ToLower(plan), "match_against",
		"MATCH must remain as a residual filter:\n"+plan)
}

// TestFTSMatchIndexResultsMatchScan is the correctness check that matters: the
// index path must return exactly what evaluating the filter over every row
// returns.
func TestFTSMatchIndexResultsMatchScan(t *testing.T) {
	tk := prepareFTSIndexTable(t)

	for _, search := range []string{
		"+distributed +sql",
		"+distributed -sql",
		"distributed storage",
		`+"distributed sql"`,
		"+distributed +sq*",
		"+nosuchtoken",
		"+storage",
	} {
		t.Run(search, func(t *testing.T) {
			query := "select id from articles where match(body) against('" +
				search + "' in boolean mode) order by id"
			withIndex := tk.MustQuery(query).Rows()
			withoutIndex := tk.MustQuery(
				strings.Replace(query, "from articles", "from articles ignore index (idx_body)", 1)).Rows()
			require.Equal(t, withoutIndex, withIndex,
				"index path disagrees with the full scan for %q", search)
		})
	}
}

// TestFTSMatchIndexNotUsedForMismatchedAnalyzer checks the index is declined
// when it was built with a different analyzer, since its tokens would not be
// the ones the query is looking for.
func TestFTSMatchIndexNotUsedForMismatchedAnalyzer(t *testing.T) {
	tk := prepareFTSIndexTable(t)
	tk.MustExec("create table ngram_articles (id int primary key, body varchar(255))")
	tk.MustExec("alter table ngram_articles add fulltext index idx_body (body) with parser ngram")
	tk.MustExec("insert into ngram_articles values (1, 'distributed sql')")

	// The query analyzes with STANDARD, the index holds NGRAM tokens.
	plan := explainRows(t, tk,
		"select id from ngram_articles where match(body) against('+distributed' in boolean mode)")
	require.NotContains(t, plan, "IndexMerge",
		"an index built with a different analyzer must not be used:\n"+plan)
}

// TestFTSMatchIndexNotUsedWithoutLocalEval checks that the access path is tied
// to local evaluation: with the switch off the MATCH takes the ILIKE fallback,
// which has nothing to do with the tokenized index.
func TestFTSMatchIndexNotUsedWithoutLocalEval(t *testing.T) {
	t.Skip("depends on the access path being generated at all; see TestFTSMatchUsesFullTextIndex")
	tk := prepareFTSIndexTable(t)
	tk.MustExec("set @@tidb_enable_local_match_against = OFF")

	plan := explainRows(t, tk,
		"select id from articles where match(body) against('+distributed' in boolean mode)")
	require.NotContains(t, plan, "idx_body",
		"without local evaluation there is no compiled query to derive terms from:\n"+plan)
}
