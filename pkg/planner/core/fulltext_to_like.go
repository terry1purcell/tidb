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

package core

import (
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// searchTerm represents a single term in a Boolean fulltext search query
type searchTerm struct {
	word       string
	isRequired bool // Has '+' prefix
	isExcluded bool // Has '-' prefix
	// Note: Phrases (wrapped in quotes) and prefix wildcards ('*' suffix) are parsed but not
	// treated differently from regular terms because LIKE %term% already matches the term anywhere.
	// Proper phrase/prefix matching would require REGEXP to enforce word boundaries, which we
	// avoid for simplicity.
}

// parseBooleanSearchString parses a Boolean mode search string into individual terms
func parseBooleanSearchString(text string) []searchTerm {
	var terms []searchTerm
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
					terms = append(terms, searchTerm{
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
						terms = append(terms, parseSearchTerm(prefix))
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
				terms = append(terms, parseSearchTerm(word))
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
				terms = append(terms, searchTerm{
					word:       phrase,
					isRequired: phraseIsRequired,
					isExcluded: phraseIsExcluded,
				})
			}
		} else {
			word := current.String()
			terms = append(terms, parseSearchTerm(word))
		}
	}

	return terms
}

// parseSearchTerm parses a single search term (not in quotes) and extracts operators
func parseSearchTerm(word string) searchTerm {
	if word == "" {
		return searchTerm{}
	}

	term := searchTerm{word: word}

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

// stripTokenPunctuation removes leading and trailing non-word characters from a
// natural-language search token so that punctuation attached to a word by the
// tokenizer (e.g. "MySQL," → "MySQL") is not included in the LIKE pattern.
// Non-ASCII bytes (> 127) are treated as word characters so multi-byte UTF-8
// characters pass through unchanged.
func stripTokenPunctuation(word string) string {
	start := 0
	for start < len(word) && !isWordByte(word[start]) {
		start++
	}
	end := len(word)
	for end > start && !isWordByte(word[end-1]) {
		end--
	}
	return word[start:end]
}

// isWordByte returns true for alphanumeric ASCII and non-ASCII bytes.
// Punctuation including underscore is NOT a word character, consistent with
// MySQL's built-in FTS tokenizer which treats _ as a word separator.
func isWordByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c > 127
}

// convertMatchAgainstToLike converts a MATCH...AGAINST expression to LIKE predicates
//
// This is a fallback implementation since TiDB does not natively support full-text search.
// It provides basic text matching capabilities but has the following semantic differences
// from MySQL's full-text search:
//
// 1. No relevance scoring — returns 1 (match) or 0 (no match) in all expression contexts.
//    Queries using MATCH...AGAINST for relevance ranking (ORDER BY MATCH(...) DESC, or
//    scalar SELECT MATCH(...)) will get 0/1 integer results instead of float relevance scores.
//    This is a fundamental limitation of the LIKE-based approximation.
// 2. No stop word filtering - searches for all words regardless of length or commonness
// 3. No word length limits - MySQL ignores words shorter than ft_min_word_len (default 4)
// 4. No word boundaries - LIKE %term% matches substrings anywhere, not just complete words
//   - Simple terms: "cat" matches "concatenate", "category", "application"
//     (MySQL FTS only matches "cat" as a standalone word)
//   - Prefix wildcard: "Optim*" matches "reOptimizing", "Optimizing"
//     (MySQL FTS only matches words starting with "Optim" like "Optimizing", not "reOptimizing")
//   - Phrase matching: "quick brown" matches "aquick brownie"
//     (MySQL FTS only matches the exact phrase with word boundaries)
//     This limitation exists because LIKE cannot enforce word boundaries without REGEXP
//
// 5. Performance - LIKE predicates cannot use full-text indexes (much slower on large datasets)
//
// Supported Boolean mode operators: + (required), - (excluded), * (prefix wildcard), "..." (phrase)
// Partially supported: ~ (treated as optional, ranking effect ignored), > < (treated as optional)
// Unsupported: WITH QUERY EXPANSION (returns an error), () sub-expression grouping (stripped)
func (er *expressionRewriter) convertMatchAgainstToLike(
	columns []expression.Expression,
	searchText string,
	modifier ast.FulltextSearchModifier,
) (expression.Expression, error) {
	if len(columns) == 0 {
		return nil, expression.ErrNotSupportedYet.GenWithStackByArgs("MATCH...AGAINST with no columns")
	}

	// WITH QUERY EXPANSION requires a second FTS pass to find semantically related
	// terms; LIKE cannot approximate this. Error explicitly rather than silently
	// producing wrong results.
	if modifier.WithQueryExpansion() {
		return nil, expression.ErrNotSupportedYet.GenWithStackByArgs("MATCH...AGAINST WITH QUERY EXPANSION is not supported in the LIKE fallback")
	}

	if searchText == "" {
		// Empty search string matches nothing
		return &expression.Constant{
			Value:   types.NewIntDatum(0),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}, nil
	}

	var columnPredicates []expression.Expression

	if modifier.IsBooleanMode() {
		// Parse Boolean mode search string
		terms := parseBooleanSearchString(searchText)
		if len(terms) == 0 {
			return &expression.Constant{
				Value:   types.NewIntDatum(0),
				RetType: types.NewFieldType(mysql.TypeTiny),
			}, nil
		}

		// Group terms by type
		var required, excluded, optional []searchTerm
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
			return &expression.Constant{
				Value:   types.NewIntDatum(0),
				RetType: types.NewFieldType(mysql.TypeTiny),
			}, nil
		}

		// Build predicates with correct Boolean logic for multiple columns
		// In MySQL, MATCH(col1, col2) AGAINST('+word1 +word2') means:
		// - word1 must appear in (col1 OR col2)
		// - word2 must appear in (col1 OR col2)
		var allPredicates []expression.Expression

		// For each required term: (col1 LIKE %term% OR col2 LIKE %term%)
		for _, term := range required {
			var termColumnPreds []expression.Expression
			for _, column := range columns {
				pred, err := er.buildLikePredicate(column, term.word)
				if err != nil {
					return nil, err
				}
				termColumnPreds = append(termColumnPreds, pred)
			}
			// At least one column must match this required term
			if len(termColumnPreds) > 0 {
				allPredicates = append(allPredicates, expression.ComposeDNFCondition(er.sctx, termColumnPreds...))
			}
		}

		// For each excluded term: NOT(col1 LIKE %term% OR col2 LIKE %term%)
		for _, term := range excluded {
			var termColumnPreds []expression.Expression
			for _, column := range columns {
				pred, err := er.buildLikePredicate(column, term.word)
				if err != nil {
					return nil, err
				}
				termColumnPreds = append(termColumnPreds, pred)
			}
			// None of the columns should match this excluded term
			if len(termColumnPreds) > 0 {
				notPred, err := er.newFunction(ast.UnaryNot, types.NewFieldType(mysql.TypeTiny),
					expression.ComposeDNFCondition(er.sctx, termColumnPreds...))
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
			var allOptionalPreds []expression.Expression
			for _, term := range optional {
				for _, column := range columns {
					pred, err := er.buildLikePredicate(column, term.word)
					if err != nil {
						return nil, err
					}
					allOptionalPreds = append(allOptionalPreds, pred)
				}
			}
			if len(allOptionalPreds) > 0 {
				optionalDNF := expression.ComposeDNFCondition(er.sctx, allOptionalPreds...)
				if len(excluded) == 0 {
					// Pure optional query: return the DNF directly.
					return optionalDNF, nil
				}
				// Optional + excluded: fold optional requirement into allPredicates
				// so it is AND-ed with the NOT-exclusion predicates below.
				allPredicates = append(allPredicates, optionalDNF)
			}
		}

		// AND all required/excluded predicates together
		if len(allPredicates) == 0 {
			return &expression.Constant{
				Value:   types.NewIntDatum(0),
				RetType: types.NewFieldType(mysql.TypeTiny),
			}, nil
		}

		return expression.ComposeCNFCondition(er.sctx, allPredicates...), nil
	}

	// Natural Language Mode: split into words and OR them together
	words := strings.Fields(searchText)
	if len(words) == 0 {
		return &expression.Constant{
			Value:   types.NewIntDatum(0),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}, nil
	}

	for _, column := range columns {
		var wordPredicates []expression.Expression
		for _, word := range words {
			// Strip leading/trailing punctuation so "MySQL," becomes "MySQL"
			word = stripTokenPunctuation(word)
			if word == "" {
				continue
			}
			pred, err := er.buildLikePredicate(column, word)
			if err != nil {
				return nil, err
			}
			wordPredicates = append(wordPredicates, pred)
		}
		if len(wordPredicates) > 0 {
			columnPredicates = append(columnPredicates, expression.ComposeDNFCondition(er.sctx, wordPredicates...))
		}
	}

	// OR across all columns
	if len(columnPredicates) == 0 {
		return &expression.Constant{
			Value:   types.NewIntDatum(0),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}, nil
	}

	return expression.ComposeDNFCondition(er.sctx, columnPredicates...), nil
}

// escapeLikePattern escapes special LIKE characters (%, _, \) in the search term
// so they are treated as literal characters rather than wildcards
func escapeLikePattern(term string) string {
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

// buildLikePredicate builds a single LIKE predicate for a column and search term
func (er *expressionRewriter) buildLikePredicate(
	column expression.Expression,
	term string,
) (expression.Expression, error) {
	// Escape special LIKE characters in the search term
	escapedTerm := escapeLikePattern(term)

	// Build the pattern
	// NOTE: Prefix matching (word*) in MySQL full-text search matches words that START with
	// the prefix, but the word can appear anywhere in the text. For example, "Optim*" should
	// match "Optimizing MySQL" but NOT "reOptimizing". Using LIKE without REGEXP, we cannot
	// perfectly enforce word-start boundaries. We use %term% which may produce false positives
	// (matching mid-word like "reOptimizing"), but avoids false negatives. This is an acceptable
	// limitation for a fallback implementation.
	// Both prefix and general matches use %term% to find the term anywhere in text
	pattern := "%" + escapedTerm + "%"

	// Create constant for pattern
	patternConst := &expression.Constant{
		Value:   types.NewStringDatum(pattern),
		RetType: types.NewFieldType(mysql.TypeVarchar),
	}

	// Create escape constant (backslash = 92)
	escapeConst := &expression.Constant{
		Value:   types.NewIntDatum(92),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// Build ILIKE function (case-insensitive LIKE).
	// MySQL full-text search is always case-insensitive regardless of column
	// collation, so ILIKE matches that semantic rather than plain LIKE which
	// would follow the column's collation (often utf8mb4_bin = case-sensitive).
	likeFunc, err := er.newFunction(ast.Ilike, types.NewFieldType(mysql.TypeTiny), column, patternConst, escapeConst)
	if err != nil {
		return nil, err
	}

	// Wrap with IFNULL so that a NULL column is treated as not containing the term
	// (consistent with MySQL FTS semantics where NULL columns are ignored).
	// Without this, NOT(NULL LIKE %term%) = NOT(NULL) = NULL which incorrectly
	// filters rows that have a NULL column and don't contain the excluded term.
	zeroConst := &expression.Constant{
		Value:   types.NewIntDatum(0),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}
	nullSafeLike, err := er.newFunction(ast.Ifnull, types.NewFieldType(mysql.TypeTiny), likeFunc, zeroConst)
	if err != nil {
		return nil, err
	}

	return nullSafeLike, nil
}
