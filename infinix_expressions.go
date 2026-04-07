package main

import (
	"errors"
	"fmt"
)

type ExprBinOp struct {
	op    ExprOp
	left  interface{}
	right interface{}
}

func (p *Parser) ParseAtom() (interface{}, error) {
	// check for parenthesis
	if p.tryPunctuation("(") {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if !p.tryPunctuation(")") {
			return nil, errors.New("expecting )")
		}
		return expr, nil
	}
	name, ok := p.TryName()
	if ok {
		return name, nil
	}

	cell := &Cell{}
	if err := p.ParseValue(cell); err != nil {
		return nil, err
	}
	return cell, nil
}

func (p *Parser) ParseMul() (interface{}, error) {
	left, _ := p.ParseAtom()
	for {
		var op ExprOp
		if p.tryPunctuation("*") {
			op = OP_MUL
		} else if p.tryPunctuation("/") {
			op = OP_DIV
		} else {
			break
		}

		right, _ := p.ParseAtom()

		left = &ExprBinOp{
			op:    op,
			left:  left,
			right: right,
		}
	}

	return left, nil

}

func (p *Parser) ParseAdd() (interface{}, error) {
	// parse the 1st operand(left side)
	left, err := p.ParseMul()
	if err != nil {
		return nil, err
	}
	for {
		// check for an operator
		var op ExprOp
		if p.tryPunctuation("+") {
			op = OP_ADD
		} else if p.tryPunctuation("-") {
			op = OP_SUB
		} else {
			break
		}

		// parse the next oerand(right side)
		right, err := p.ParseMul()
		if err != nil {
			return nil, err
		}

		// write the existing tree as the left child of a new node
		left = &ExprBinOp{
			op:    op,
			left:  left,
			right: right,
		}
	}

	return left, nil
}

func (p *Parser) parseExpr() (interface{}, error) {
	return p.ParseAdd()
}

func evalExpr(schema *Schema, row Row, expr interface{}) (*Cell, error) {
	switch e := expr.(type) {
	case string:
		// find which index this column name corresponds to
		idx := -1
		for i, col := range schema.Cols {
			if col.Name == e {
				idx = i
				break
			}
		}
		if idx == -1 {
			return nil, fmt.Errorf("column %s not found", e)
		}
		// return the cell from the current row
		return &row[idx], nil
	case *Cell:
		return e, nil
	case *ExprBinOp:
		left, err := evalExpr(schema, row, e.left)
		if err != nil {
			return nil, err
		}
		right, err := evalExpr(schema, row, e.right)
		if err != nil {
			return nil, err
		}

		switch e.op {
		case OP_ADD:
			if left.Type == TypeI64 && right.Type == TypeI64 {
				return &Cell{Type: TypeI64, I64: left.I64 + right.I64}, nil
			}
		case OP_SUB:
			if left.Type == TypeI64 && right.Type == TypeI64 {
				return &Cell{Type: TypeI64, I64: left.I64 - right.I64}, nil
			}
		case OP_EQ:
			// comparisons return a boolean
			res := (left.I64 == right.I64)
			val := int64(0)
			if res {
				val = 1
			}
			return &Cell{Type: TypeI64, I64: val}, nil
		}
	default:
		panic("unreachable")
	}

	return &Cell{}, nil
}

func (p *Parser) parseOr() (interface{}, error) {
	return p.parseBinop([]string{"OR"}, []ExprOp{OP_OR}, p.parseAnd)
}
func (p *Parser) parseAnd() (interface{}, error) {
	return p.parseBinop([]string{"AND"}, []ExprOp{OP_AND}, p.parseNot)
}
func (p *Parser) parseNot() (expr interface{}, err error)
func (p *Parser) parseCmp() (interface{}, error) {
	return p.parseBinop(
		[]string{"=", "!=", "<>", "<=", ">=", "<", ">"},
		[]ExprOp{OP_EQ, OP_NE, OP_NE, OP_LE, OP_GE, OP_LT, OP_GT},
		p.parseAdd)
}
func (p *Parser) parseAdd() (interface{}, error) {
	return p.parseBinop([]string{"+", "-"}, []ExprOp{OP_ADD, OP_SUB}, p.parseMul)
}
func (p *Parser) parseMul() (interface{}, error) {
	return p.parseBinop([]string{"*", "/"}, []ExprOp{OP_MUL, OP_DIV}, p.parseNeg)
}
func (p *Parser) parseNeg() (expr interface{}, err error)

func (p *Parser) parseBinop(tokens []string, ops []ExprOp, inner func() (interface{}, error),
) (interface{}, error) {
	// get the lefet side
	left, _ := inner()

	for {
		found := -1
		for i, t := range tokens {
			if p.tryPunctuation(t) || p.TryKeywordkw(t) {
				found = i
				break
			}
		}
		if found == -1 {
			break
		}

		right, _ := inner()

		// wrap the result in a new node
		left = &ExprBinOp{
			op:    ops[found],
			left:  left,
			right: right,
		}
	}
	return left, nil
}

type ExprUnOp struct {
	op  ExprOp
	kid interface{}
}

type StmtSelectExpr struct {
	table string
	// cols  []string
	cols []interface{} // ExprUnOp | ExprBinOp | string | *Cell
	keys []NamedCell
}
type StmtUpdate struct {
	table string
	keys  []NamedCell
	// value []ExprEqual
	value []ExprAssign
}
type ExprAssign struct {
	column string
	expr   interface{} // ExprUnOp | ExprBinOp | string | *Cell
}

func (p *Parser) parseAssign(out *ExprAssign) (err error) {
	var ok bool
	out.column, ok = p.TryName()
	if !ok {
		return errors.New("expect column")
	}
	if !p.tryPunctuation("=") {
		return errors.New("expect =")
	}
	out.expr, err = p.parseExpr()
	return err
}

func (db *DB) execSelect(stmt *StmtSelect) ([]Row, error) {
	schema, ok := db.tables[stmt.table]
	if !ok {
		return nil, fmt.Errorf("table %s not found", stmt.table)
	}

	// Start from the first row key for this table.
	startRow := schema.NewRow()
	iter, err := db.Seek(&schema, startRow)
	if err != nil {
		return nil, err
	}

	var results []Row
	for iter.valid {
		oldRow := iter.row
		newRow := make(Row, len(stmt.cols))

		// 2. Evaluate each expression for this specific row
		for i, expr := range stmt.cols {
			cell, err := evalExpr(&schema, oldRow, expr)
			if err != nil {
				return nil, err
			}
			newRow[i] = *cell
		}
		results = append(results, newRow)

		if err := iter.iter.Next(); err != nil {
			return nil, err
		}
		iter.valid, err = DecodeKVIter(iter.schema, iter.iter, iter.row)
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (db *DB) execUpdate(stmt *StmtUpdate) (int, error) {
	schema, ok := db.tables[stmt.table]
	if !ok {
		return 0, fmt.Errorf("table %s not found", stmt.table)
	}

	// Build the target row from key predicates, then load current values.
	oldRow := schema.NewRow()
	for _, key := range stmt.keys {
		colIdx := -1
		for i, col := range schema.Cols {
			if col.Name == key.column {
				colIdx = i
				break
			}
		}
		if colIdx < 0 {
			return 0, fmt.Errorf("column %s not found", key.column)
		}
		oldRow[colIdx] = key.value
	}

	found, err := db.Select(&schema, oldRow)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}

	// For each assignment (e.g., SET a = a + 1):
	for _, assign := range stmt.value {
		// Evaluate the expression using the CURRENT values in the row
		newVal, err := evalExpr(&schema, oldRow, assign.expr)
		if err != nil {
			return 0, err
		}

		// Find the column index and update the row in-memory
		colIdx := -1
		for i, col := range schema.Cols {
			if col.Name == assign.column {
				colIdx = i
				break
			}
		}
		if colIdx < 0 {
			return 0, fmt.Errorf("column %s not found", assign.column)
		}
		oldRow[colIdx] = *newVal
	}
	// 3. Write the modified row back to the KV store
	updated, err := db.Update(&schema, oldRow)
	if err != nil {
		return 0, err
	}
	if !updated {
		return 0, nil
	}
	return 1, nil
}

type StmtSelectCond struct {
	table string
	cols  []interface{} // ExprUnOp | ExprBinOp | string | *Cell
	cond  interface{}
}

type StmtUpdateCond struct {
	table string
	cond  interface{}
	value []ExprAssign
}

type StmtDelete struct {
	table string
	cond  interface{}
}

type RangeReq struct {
	StartCmp ExprOp // <= >= < >
	StopCmp  ExprOp
	Start    []Cell
	Stop     []Cell
}

func (db *DB) Range(schema *Schema, req *RangeReq) (*RowIterator, error)

func (db *DB) execCond(schema *Schema, cond interface{}) (*RowIterator, error) {
	req, err := makeRange(schema, cond)
	if err != nil {
		return nil, err
	}
	return db.Range(schema, req)
}
func makeRange(schema *Schema, cond interface{}) (*RangeReq, error)

type ExprTuple struct {
	kids []interface{}
}

func (p *Parser) ParseTuple() (interface{}, error) {
	if !p.tryPunctuation("(") {
		return nil, errors.New("expect ()")
	}

	var kids []interface{}

	for {
		expr, _ := p.parseExpr()
		kids = append(kids, expr)

		if p.tryPunctuation(")") {
			break
		}
		if !p.tryPunctuation(",") {
			return nil, errors.New("expect , or )")
		}
	}

	if len(kids) == 1 {
		return kids[0], nil
	}
	return &ExprTuple{kids: kids}, nil
}
