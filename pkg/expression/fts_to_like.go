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

package expression

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// ftsSearchTerm represents a single term in a Boolean fulltext search query.
type ftsSearchTerm struct {
	word       string
	isRequired bool // Has '+' prefix
	isExcluded bool // Has '-' prefix
	// Note: Phrases (wrapped in quotes) and prefix wildcards ('*' suffix) are parsed but not
	// treated differently from regular terms because LIKE %term% already matches the term anywhere.
	// Proper phrase/prefix matching would require REGEXP to enforce word boundaries, which we
	// avoid for simplicity.
}

// parseFTSBooleanSearchString parses a Boolean mode search string into individual terms.
func parseFTSBooleanSearchString(text string) []ftsSearchTerm {
	var terms []ftsSearchTerm
	var current strings.Builder
	inQuote := false
	phraseIsRequired := false
	phraseIsExcluded := false
	i := 0

	for i < len(text) {
		ch := text[i]

		switch ch {
		case '"':
			if inQuote {
				// End of phrase
				// NOTE: Phrase matching in MySQL full-text search finds the exact phrase as a sequence
				// of words (word boundaries are enforced). Using LIKE %phrase%, we cannot perfectly
				// enforce word boundaries without REGEXP. For example, "quick brown" would match
				// "aquick brownie" which MySQL full-text search would not match. This is an acceptable
				// limitation for a fallback implementation.
				phrase := current.String()
				if phrase != "" {
					terms = append(terms, ftsSearchTerm{
						word:       phrase,
						isRequired: phraseIsRequired,
						isExcluded: phraseIsExcluded,
					})
				}
				current.Reset()
				inQuote = false
				phraseIsRequired = false
				phraseIsExcluded = false
			} else {
				// Check for leading operator before the quote (e.g., +"phrase" or -"phrase")
				if current.Len() > 0 {
					prefix := current.String()
					// Only extract operator if prefix is exactly "+" or "-"
					// Otherwise, treat it as a regular word
					if prefix == "+" {
						phraseIsRequired = true
					} else if prefix == "-" {
						phraseIsExcluded = true
					} else {
						// Not an operator, parse as a regular word first
						terms = append(terms, parseFTSSearchTerm(prefix))
					}
					current.Reset()
				}
				// Start of phrase
				inQuote = true
			}
			i++
		case ' ', '\t', '\n', '\r':
			if inQuote {
				current.WriteByte(ch)
			} else if current.Len() > 0 {
				// End of word
				word := current.String()
				terms = append(terms, parseFTSSearchTerm(word))
				current.Reset()
			}
			i++
		default:
			current.WriteByte(ch)
			i++
		}
	}

	// Handle remaining content
	if current.Len() > 0 {
		if inQuote {
			// Unclosed quote, treat as phrase and preserve operator flags
			phrase := current.String()
			if phrase != "" {
				terms = append(terms, ftsSearchTerm{
					word:       phrase,
					isRequired: phraseIsRequired,
					isExcluded: phraseIsExcluded,
				})
			}
		} else {
			word := current.String()
			terms = append(terms, parseFTSSearchTerm(word))
		}
	}

	return terms
}

// parseFTSSearchTerm parses a single search term (not in quotes) and extracts operators.
func parseFTSSearchTerm(word string) ftsSearchTerm {
	if word == "" {
		return ftsSearchTerm{}
	}

	term := ftsSearchTerm{word: word}

	// Check for leading operators
	if word[0] == '+' {
		term.isRequired = true
		word = word[1:]
	} else if word[0] == '-' {
		term.isExcluded = true
		word = word[1:]
	}

	// Strip MySQL relevance modifiers >, <, ~ (treat term as optional in LIKE fallback).
	// ~ in MySQL Boolean FTS decreases the relevance of a term without excluding it;
	// >, < adjust the relevance contribution. All three map to "optional" here.
	if len(word) > 0 && (word[0] == '>' || word[0] == '<' || word[0] == '~') {
		word = word[1:]
	}

	// Strip grouping parentheses that MySQL uses for sub-expression grouping
	word = strings.Trim(word, "()")

	// Check for trailing wildcard and strip it (we don't use it differently, see struct comment)
	if len(word) > 0 && word[len(word)-1] == '*' {
		word = word[:len(word)-1]
	}

	term.word = word
	return term
}

// stripFTSTokenPunctuation removes leading and trailing non-word characters from a
// natural-language search token so that punctuation attached to a word by the
// tokenizer (e.g. "MySQL," → "MySQL") is not included in the LIKE pattern.
// Non-ASCII bytes (> 127) are treated as word characters so multi-byte UTF-8
// characters pass through unchanged.
func stripFTSTokenPunctuation(word string) string {
	start := 0
	for start < len(word) && !isFTSWordByte(word[start]) {
		start++
	}
	end := len(word)
	for end > start && !isFTSWordByte(word[end-1]) {
		end--
	}
	return word[start:end]
}

// isFTSWordByte returns true for alphanumeric ASCII and non-ASCII bytes.
// Punctuation including underscore is NOT a word character, consistent with
// MySQL's built-in FTS tokenizer which treats _ as a word separator.
func isFTSWordByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c > 127
}

// escapeFTSLikePattern escapes special LIKE characters (%, _, \) in the search term
// so they are treated as literal characters rather than wildcards.
func escapeFTSLikePattern(term string) string {
	// Count special characters to pre-allocate the exact buffer size needed
	escapeCount := 0
	for i := range len(term) {
		ch := term[i]
		if ch == '\\' || ch == '%' || ch == '_' {
			escapeCount++
		}
	}

	// Allocate exact size: original length + number of escape characters
	var result strings.Builder
	result.Grow(len(term) + escapeCount)
	for i := range len(term) {
		ch := term[i]
		if ch == '\\' || ch == '%' || ch == '_' {
			result.WriteByte('\\')
		}
		result.WriteByte(ch)
	}
	return result.String()
}

// BuildFTSToILikeExpression converts a MATCH...AGAINST input (a list of column
// expressions, the search-string literal, and the parsed modifier) into an
// equivalent ILIKE-based predicate expression.
//
// Two callers share this conversion:
//   - the planner's MATCH...AGAINST LIKE fallback rewrite, used as the
//     executable plan when the "fts-native" alternative round is not viable;
//   - selectivity estimation, which substitutes the same ILIKE form for the
//     opaque FTSMysqlMatchAgainst builtin so the two alternative rounds
//     compete on cost using the same column-stats-derived row estimate
//     (the native builtin cannot be evaluated in TiDB and would otherwise
//     fall through to a flat default selectivity that ignores the column's
//     histogram).
//
// Returns an integer (0/1) typed expression suitable for direct use as a
// filter predicate.
//
// Semantic differences from MySQL's full-text search are documented in detail
// at the planner-level call site; this helper preserves those approximations
// so both callers see the same translated expression.
func BuildFTSToILikeExpression(
	ctx BuildContext,
	columns []Expression,
	searchText string,
	modifier ast.FulltextSearchModifier,
) (Expression, error) {
	if len(columns) == 0 {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("MATCH...AGAINST with no columns")
	}

	// WITH QUERY EXPANSION requires a second FTS pass to find semantically related
	// terms; LIKE cannot approximate this. Error explicitly rather than silently
	// producing wrong results.
	if modifier.WithQueryExpansion() {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("MATCH...AGAINST WITH QUERY EXPANSION is not supported in the LIKE fallback")
	}

	zeroIntConst := func() Expression {
		return &Constant{
			Value:   types.NewIntDatum(0),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}
	}

	if searchText == "" {
		return zeroIntConst(), nil
	}

	if modifier.IsBooleanMode() {
		terms := parseFTSBooleanSearchString(searchText)
		if len(terms) == 0 {
			return zeroIntConst(), nil
		}

		var required, excluded, optional []ftsSearchTerm
		for _, term := range terms {
			if term.word == "" {
				continue
			}
			if term.isRequired {
				required = append(required, term)
			} else if term.isExcluded {
				excluded = append(excluded, term)
			} else {
				optional = append(optional, term)
			}
		}

		// MySQL Boolean mode: a query with only excluded terms ("-a -b") returns
		// an empty result set. The LIKE fallback must match this: when there are
		// no required and no optional terms, no row can possibly satisfy the
		// search, so return a constant FALSE immediately.
		if len(required) == 0 && len(optional) == 0 && len(excluded) > 0 {
			return zeroIntConst(), nil
		}

		var allPredicates []Expression

		// For each required term: (col1 ILIKE %term% OR col2 ILIKE %term% ...)
		for _, term := range required {
			var termColumnPreds []Expression
			for _, column := range columns {
				pred, err := buildFTSILikePredicate(ctx, column, term.word)
				if err != nil {
					return nil, err
				}
				termColumnPreds = append(termColumnPreds, pred)
			}
			if len(termColumnPreds) > 0 {
				allPredicates = append(allPredicates, ComposeDNFCondition(ctx, termColumnPreds...))
			}
		}

		// For each excluded term: NOT(col1 ILIKE %term% OR col2 ILIKE %term% ...)
		for _, term := range excluded {
			var termColumnPreds []Expression
			for _, column := range columns {
				pred, err := buildFTSILikePredicate(ctx, column, term.word)
				if err != nil {
					return nil, err
				}
				termColumnPreds = append(termColumnPreds, pred)
			}
			if len(termColumnPreds) > 0 {
				notPred, err := NewFunction(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny),
					ComposeDNFCondition(ctx, termColumnPreds...))
				if err != nil {
					return nil, err
				}
				allPredicates = append(allPredicates, notPred)
			}
		}

		// For optional terms: since LIKE cannot rank, treat optionals as a
		// positive filter when no required terms exist.
		// - required>0: ignore optionals (required terms already anchor the result)
		// - required==0, excluded==0: at least one optional must match (pure optional query)
		// - required==0, excluded>0: at least one optional must match AND excluded terms
		//   must be absent; AND the optional-DNF into allPredicates below
		if len(optional) > 0 && len(required) == 0 {
			var allOptionalPreds []Expression
			for _, term := range optional {
				for _, column := range columns {
					pred, err := buildFTSILikePredicate(ctx, column, term.word)
					if err != nil {
						return nil, err
					}
					allOptionalPreds = append(allOptionalPreds, pred)
				}
			}
			if len(allOptionalPreds) > 0 {
				optionalDNF := ComposeDNFCondition(ctx, allOptionalPreds...)
				if len(excluded) == 0 {
					return optionalDNF, nil
				}
				allPredicates = append(allPredicates, optionalDNF)
			}
		}

		if len(allPredicates) == 0 {
			return zeroIntConst(), nil
		}

		return ComposeCNFCondition(ctx, allPredicates...), nil
	}

	// Natural Language Mode: split into words and OR them together.
	words := strings.Fields(searchText)
	if len(words) == 0 {
		return zeroIntConst(), nil
	}

	var columnPredicates []Expression
	for _, column := range columns {
		var wordPredicates []Expression
		for _, word := range words {
			word = stripFTSTokenPunctuation(word)
			if word == "" {
				continue
			}
			pred, err := buildFTSILikePredicate(ctx, column, word)
			if err != nil {
				return nil, err
			}
			wordPredicates = append(wordPredicates, pred)
		}
		if len(wordPredicates) > 0 {
			columnPredicates = append(columnPredicates, ComposeDNFCondition(ctx, wordPredicates...))
		}
	}

	if len(columnPredicates) == 0 {
		return zeroIntConst(), nil
	}

	return ComposeDNFCondition(ctx, columnPredicates...), nil
}

// BuildFTSToILikeExpressionFromBuiltin pulls the search string and modifier
// out of a MATCH...AGAINST scalar function (FTSMysqlMatchAgainst) and
// delegates to BuildFTSToILikeExpression. It is the entry point for
// selectivity estimation, where the FTS scalar function is opaque to the
// stats engine; substituting an equivalent ILIKE expression lets the engine
// reuse its TopN/histogram-based estimation paths instead of falling back
// to a flat default that ignores column statistics.
func BuildFTSToILikeExpressionFromBuiltin(ctx BuildContext, fts *ScalarFunction) (Expression, error) {
	if fts == nil || fts.FuncName.L != ast.FTSMysqlMatchAgainst {
		return nil, errors.Errorf("expected %s, got %v", ast.FTSMysqlMatchAgainst, fts)
	}
	args := fts.GetArgs()
	if len(args) < 2 {
		return nil, errors.Errorf("%s expects at least 2 args, got %d", ast.FTSMysqlMatchAgainst, len(args))
	}
	againstConst, ok := args[0].(*Constant)
	if !ok {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("MATCH...AGAINST with non-constant search string")
	}
	if againstConst.Value.IsNull() {
		return &Constant{
			Value:   types.NewIntDatum(0),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}, nil
	}
	if againstConst.Value.Kind() != types.KindString {
		return nil, ErrNotSupportedYet.GenWithStackByArgs("MATCH...AGAINST with non-string search constant")
	}
	sig, ok := fts.Function.(*builtinFtsMysqlMatchAgainstSig)
	if !ok {
		return nil, errors.Errorf("unexpected builtin signature for %s: %T", ast.FTSMysqlMatchAgainst, fts.Function)
	}
	return BuildFTSToILikeExpression(ctx, args[1:], againstConst.Value.GetString(), sig.modifier)
}

// buildFTSILikePredicate builds a single ILIKE predicate for a column and search term,
// wrapped in IFNULL so that NULL columns are treated as not containing the term.
func buildFTSILikePredicate(ctx BuildContext, column Expression, term string) (Expression, error) {
	escapedTerm := escapeFTSLikePattern(term)

	// NOTE: Prefix matching (word*) in MySQL full-text search matches words that START with
	// the prefix, but the word can appear anywhere in the text. Using LIKE without REGEXP,
	// we cannot perfectly enforce word-start boundaries. We use %term% which may produce
	// false positives but avoids false negatives.
	pattern := "%" + escapedTerm + "%"

	patternConst := &Constant{
		Value:   types.NewStringDatum(pattern),
		RetType: types.NewFieldType(mysql.TypeVarchar),
	}

	// Backslash escape character (=92) for ILIKE.
	escapeConst := &Constant{
		Value:   types.NewIntDatum(92),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// MySQL full-text search is always case-insensitive regardless of column
	// collation, so ILIKE matches that semantic rather than plain LIKE which
	// would follow the column's collation.
	likeFunc, err := NewFunction(ctx, ast.Ilike, types.NewFieldType(mysql.TypeTiny), column, patternConst, escapeConst)
	if err != nil {
		return nil, err
	}

	// Wrap with IFNULL so a NULL column is treated as not containing the term
	// (consistent with MySQL FTS semantics where NULL columns are ignored).
	// Without this, NOT(NULL ILIKE %term%) = NOT(NULL) = NULL which incorrectly
	// filters rows that have a NULL column and don't contain the excluded term.
	zeroConst := &Constant{
		Value:   types.NewIntDatum(0),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
	return NewFunction(ctx, ast.Ifnull, types.NewFieldType(mysql.TypeTiny), likeFunc, zeroConst)
}
