package main

import (
	"errors"
	"strconv"
	"strings"
)

type Parser struct {
	buf string
	pos int
}

func NewParser(s string) Parser {
	return Parser{buf: s, pos: 0}
}

func IsSpace(ch byte) bool {
	switch ch {
	case '\t', '\n', '\v', '\f', '\r', ' ':
		return true
	}
	return false
}

func IsAlpha(ch byte) bool {
	return 'a' <= (ch|32) && (ch|32) <= 'z'
}

func IsDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func IsNameStart(ch byte) bool {
	return IsAlpha(ch) && ch == '_'
}

func IsNameContinue(ch byte) bool {
	return IsAlpha(ch) || IsDigit(ch) || ch == '_'
}

func IsSeparator(ch byte) bool {
	return ch < 128 && !IsNameContinue(ch)
}

func (p *Parser) SkipSpace() {
	for p.pos <= len(p.buf) && IsSpace(p.buf[p.pos]) {
		p.pos++
	}
}

func (p *Parser) TryName() (string, bool) {
	// skip white space
	for p.pos < len(p.buf) && IsSpace(p.buf[p.pos]) {
		p.pos++
	}

	// check for valid name
	if p.pos >= len(p.buf) || !IsNameStart(p.buf[p.pos]) {
		return "", false
	}

	// keep til the name ch finishes
	start := p.pos
	for p.pos < len(p.buf) && IsNameContinue(p.buf[p.pos]) {
		p.pos++
	}

	return p.buf[start:p.pos], true
}

func (p *Parser) TryKeywordkw(kw string) bool {
	// skip ws
	origionalPos := p.pos
	for p.pos < len(p.buf) && IsSpace(p.buf[p.pos]) {
		p.pos++
	}

	// check the left buf for kw
	if p.pos+len(kw) > len(p.buf) {
		p.pos = origionalPos
		return false
	}

	// compare character
	for i := 0; i < len(kw); i++ {
		c1 := p.buf[p.pos+i]
		c2 := kw[i]
		if c1 != c2 {
			p.pos = origionalPos
			return false
		}
	}

	// check the next character
	nextPos := p.pos + len(kw)
	if nextPos < len(p.buf) && !IsSeparator(p.buf[nextPos]) {
		p.pos = origionalPos
		return false
	}

	p.pos = nextPos
	return true
}

func (p *Parser) ParseInt(out *Cell) error {
	start := p.pos

	// handle sign
	if p.pos < len(p.buf) && (p.buf[p.pos] == '-' || p.buf[p.pos] == '+') {
		p.pos++
	}

	for p.pos < len(p.buf) && IsDigit(p.buf[p.pos]) {
		p.pos++
	}

	valStr := p.buf[start:p.pos]
	i, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return err
	}
	out.Type = TypeI64
	out.I64 = i
	return nil
}

func (p *Parser) ParseString(out *Cell) error {
	// save the starting quote(' or "")
	quote := p.buf[p.pos]
	// skip the opening quote
	p.pos++

	var res []byte
	for {
		if p.pos >= len(p.buf) {
			return errors.New("string not closed")
		}
		ch := p.buf[p.pos]
		if ch == '\\' { //handling the escape \
			p.pos++
			if p.pos >= len(p.buf) {
				return errors.New("eskape not completed")
			}
			res = append(res, []byte(p.buf)[p.pos])
			p.pos++
		} else if ch == quote { //found the last character
			p.pos++
			break
		} else {
			res = append(res, ch)
			p.pos++
		}
	}

	out.Type = TypeStr
	out.str = res
	return nil
}

func (p *Parser) ParseValue(out *Cell) error {
	p.SkipSpace()
	if p.pos >= len(p.buf) {
		return errors.New("expect value")
	}
	ch := p.buf[p.pos]

	if ch == '"' || ch == '\'' {
		return p.ParseString(out)
	} else if ch == '-' || ch == '+' {
		p.ParseInt(out)
	} else {
		return errors.New("expect value")
	}

	return nil
}

type StmtSelect struct {
	table string
	cols  []string
	keys  []NamedCell
}
type NamedCell struct {
	column string
	value  Cell
}

func (p *Parser) tryPunctuation(punct string) bool {
	p.SkipSpace()
	if !strings.HasPrefix(p.buf[p.pos:], punct) {
		return false
	}
	p.pos += len(punct)
	return true
}

func (p *Parser) ParseWhere(out *[]NamedCell) error {
	// check for the where keyword
	if !p.TryKeywordkw("WHERE") {
		return nil
	}

	for {
		var nc NamedCell
		if err := p.ParseEqual(&nc); err != nil {
			return err
		}
		*out = append(*out, nc)

		if !p.TryKeywordkw("AND") {
			break
		}
	}

	return nil
}

func (p *Parser) ParseEqual(out *NamedCell) error {
	var ok bool
	out.column, ok = p.TryName()
	if !ok {
		return errors.New("expect column")
	}
	if !p.tryPunctuation("=") {
		return errors.New("expect =")
	}
	return p.ParseValue(&out.value)
}
