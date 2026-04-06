package main

import (
	"bytes"
	"encoding/json"
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

type StmntCreateTable struct {
	table string
	cols  []Column
	pkey  []string
}

type StmntInsert struct {
	table string
	val   []Cell
}

type StmntUpdate struct {
	table  string
	keys   []NamedCell
	values []NamedCell
}

type StmntDelete struct {
	table string
	keys  []NamedCell
}

func (p *Parser) ParseInsert(out *StmntInsert) error {
	// get table name
	var ok bool
	if out.table, ok = p.TryName(); !ok {
		return errors.New("expect table name")
	}

	// expect values and bracket
	if !p.TryKeywordkw("VALUES") || !p.tryPunctuation("()") {
		return errors.New("expect VALUES (")
	}

	// loop through values until it gets )
	for {
		var c Cell

		if err := p.ParseValue(&c); err != nil {
			return err
		}
		out.val = append(out.val, c)

		if p.tryPunctuation(")") {
			break
		}
		if !p.tryPunctuation(",") {
			return errors.New("expect , or )")
		}
	}

	return nil
}

type SQLResult struct {
	Updated int
	Header  []string
	Values  []Row
}

func pkeyColumnIndexes(cols []Column, pkeys []string) ([]int, error) {
	indexByName := make(map[string]int, len(cols))
	for i, c := range cols {
		indexByName[c.Name] = i
	}

	indexes := make([]int, 0, len(pkeys))
	for _, key := range pkeys {
		idx, ok := indexByName[key]
		if !ok {
			return nil, errors.New("unknown primary key column: " + key)
		}
		indexes = append(indexes, idx)
	}

	return indexes, nil
}

func (db *DB) execCreateTable(stmt *StmntCreateTable) error {
	pkeyIdx, err := pkeyColumnIndexes(stmt.cols, stmt.pkey)
	if err != nil {
		return err
	}

	// convert statement to schema struct
	schema := Schema{
		Table: stmt.table,
		Cols:  stmt.cols,
		PKey:  pkeyIdx,
	}

	data, err := json.Marshal(schema)
	if err != nil {
		return err
	}
	key := []byte("@schema_" + stmt.table)
	if _, err := db.KV.Set(key, data); err != nil {
		return err
	}

	return nil
}

func (db *DB) ExecStmt(stmt interface{}) (r SQLResult, err error) {
	switch ptr := stmt.(type) {
	case *StmntCreateTable:
		err = db.execCreateTable(ptr)
	default:
		err = errors.New("unsupported statement type")
	}

	return r, err
}

func (db *DB) GetSchema(table string) (Schema, error) {
	schema, ok := db.tables[table]
	if !ok {
		val, ok, err := db.KV.Get([]byte("@schema_" + table))
		if err == nil && ok {
			err = json.Unmarshal(val, &schema)
		}
		if err != nil {
			return Schema{}, err
		}
		if !ok {
			return Schema{}, errors.New("table not found")
		}
		db.tables[table] = schema
	}

	return schema, nil
}

func BinarySearch(keys [][]byte, target []byte) (int, bool) {
	low, high := 0, len(keys)-1
	for low <= high {
		mid := low + (high-low)/2
		cmp := bytes.Compare(keys[mid], target)
		if cmp == 0 {
			return mid, true // Found it!
		} else if cmp < 0 {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return low, false // Not found, but 'low' is the insertion index
}
