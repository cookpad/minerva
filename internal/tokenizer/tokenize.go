package internal

import (
	// "log"

	"regexp"
	"strings"
	"unicode"
)

// Token is a part of log message
type Token struct {
	Data    string
	IsDelim bool
	freeze  bool
}

// IsSpace checks all runes in string
func (x *Token) IsSpace() bool {
	for _, r := range x.Data {
		if !unicode.IsSpace(r) {
			return false
		}
	}

	return true
}

func newToken(d string) *Token {
	return &Token{Data: d}
}

// Tokenizer splits log message string
type Tokenizer interface {
	Split(msg string) []*Token
}

// SimpleTokenizer is one of implementation for Tokenizer
type SimpleTokenizer struct {
	delims    string
	regexList []*regexp.Regexp
	useRegex  bool
}

// NewSimpleTokenizer is a constructor of SimpleTokenizer
func NewSimpleTokenizer() *SimpleTokenizer {
	s := &SimpleTokenizer{}
	s.delims = " \t!,:;[]{}()<>=|\\*\"'/.@"
	s.useRegex = true

	heuristicsPatterns := []string{
		// IPv4 address
		`(\d{1,3}\.){3}\d{1,3}`,
	}

	s.regexList = make([]*regexp.Regexp, len(heuristicsPatterns))
	for idx, p := range heuristicsPatterns {
		s.regexList[idx] = regexp.MustCompile(p)
	}
	return s
}

// SetDelim is a function set characters as delimiter
func (x *SimpleTokenizer) SetDelim(d string) {
	x.delims = d
}

// EnableRegex is disabler of heuristics patterns
func (x *SimpleTokenizer) EnableRegex() {
	x.useRegex = true
}

// DisableRegex is disabler of heuristics patterns
func (x *SimpleTokenizer) DisableRegex() {
	x.useRegex = false
}

func (x *SimpleTokenizer) splitByRegex(chunk *Token) []*Token {
	tokens := []*Token{chunk}

	for _, rgx := range x.regexList {
		newTokens := []*Token{}
		for _, t := range tokens {
			matches := rgx.FindAllStringIndex(t.Data, -1)
			if len(matches) > 0 {
				last := 0
				for _, m := range matches {
					newTokens = append(newTokens, newToken(t.Data[last:m[0]]))
					tgt := newToken(t.Data[m[0]:m[1]])
					tgt.freeze = true
					newTokens = append(newTokens, tgt)
					last = m[1]
				}
				newTokens = append(newTokens, newToken(t.Data[last:]))
			} else {
				newTokens = append(newTokens, t)
			}
		}
		tokens = newTokens
	}

	return tokens
}

func (x *SimpleTokenizer) splitByDelimiter(chunk *Token) []*Token {
	var res []*Token
	msg := chunk.Data

	for {
		idx := strings.IndexAny(msg, x.delims)
		if idx < 0 {
			if len(msg) > 0 {
				res = append(res, newToken(msg))
			}
			break
		}

		// log.Print("index: ", idx)
		fwd := idx + 1

		s1 := msg[:idx]
		s2 := msg[idx:fwd]
		s3 := msg[fwd:]

		if len(s1) > 0 {
			// log.Print("add s1: ", s1)
			res = append(res, newToken(s1))
		}

		if len(s2) > 0 {
			t := newToken(s2)
			t.IsDelim = true
			res = append(res, t)
		}

		msg = s3
		// log.Print("remain: ", msg)
	}

	return res
}

// Split is a function to split log message.
func (x *SimpleTokenizer) Split(msg string) []*Token {
	token := newToken(msg)
	// chunks := []*Token{token}
	tokens := x.splitByRegex(token)

	var res []*Token
	for _, c := range tokens {
		if c.freeze {
			res = append(res, c)
		} else {
			res = append(res, x.splitByDelimiter(c)...)
		}
	}
	return res
}
