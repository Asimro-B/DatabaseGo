package main

import (
	"encoding/binary"
	"errors"
	"io"
)

type CellType uint

const (
	TypeI64 CellType = 1
	TypeStr CellType = 2
)

type Cell struct {
	Type CellType
	I64  int64
	str  []byte
}

func (cell *Cell) Encode(toAppend []byte) []byte {
	// first append the type (1byte)
	toAppend = append(toAppend, uint8(cell.Type))

	switch cell.Type {
	case TypeI64:
		// create an 8 byte buffer for the int64
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(cell.I64))
		toAppend = append(toAppend, buf...)

	case TypeStr:
		// 4 bytes for length + the actual string bytes
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(len(cell.str)))
		toAppend = append(toAppend, buf...)
		toAppend = append(toAppend, cell.str...)
	}
	return toAppend
}

func (cell *Cell) Decode(data []byte) ([]byte, error) {
	if len(data) < 1 {
		return nil, io.ErrUnexpectedEOF
	}

	// read the type
	cell.Type = CellType(data[0])
	rest := data[1:]

	switch cell.Type {
	case TypeI64:
		if len(rest) < 8 {
			return nil, io.ErrUnexpectedEOF
		}
		// cast uint64 to int64 directly
		cell.I64 = int64(binary.LittleEndian.Uint64(rest[:8]))
		return rest[8:], nil

	case TypeStr:
		if len(rest) < 4 {
			return nil, io.ErrUnexpectedEOF
		}
		strLen := binary.LittleEndian.Uint32(rest[:4])
		rest = rest[4:]

		if len(rest) < int(strLen) {
			return nil, io.ErrUnexpectedEOF
		}
		cell.str = rest[:strLen]
		return rest[strLen:], nil

	default:
		return nil, errors.New("unknown cell type")
	}

}

type Schema struct {
	Table string
	Cols  []Column
	PKey  []int // which columns are the primary key?
}
type Column struct {
	Name string
	Type CellType
}

type Row []Cell

func (schema *Schema) NewRow() Row {
	return make(Row, len(schema.Cols))
}

func (row Row) EmcodeKey(schema *Schema) (key []byte) {
	// table name prefix
	key = append([]byte(schema.Table), 0*00)

	// add each pk col
	for _, colIdx := range schema.PKey {
		key = row[colIdx].Encode(key)
	}

	return key
}

func (row Row) EncodeVal(schema *Schema) (val []byte) {
	// look for pk
	isPk := make(map[int]bool)
	for _, idx := range schema.PKey {
		isPk[idx] = true
	}

	// append every column that is not pk
	for i := range schema.Cols {
		if !isPk[i] {
			val = row[i].Encode(val)
		}
	}

	return val
}

func (row Row) DecodeKey(schema *Schema, key []byte) error {
	// 1. Skip the table name and the 0x00 separator
	prefixLen := len(schema.Table) + 1
	if len(key) < prefixLen {
		return errors.New("key too short")
	}
	rest := key[prefixLen:]

	// 2. Decode each PK column in order
	var err error
	for _, colIdx := range schema.PKey {
		rest, err = row[colIdx].Decode(rest)
		if err != nil {
			return err
		}
	}
	return nil
}

func (row Row) DecodeVal(schema *Schema, val []byte) error {
	isPK := make(map[int]bool)
	for _, idx := range schema.PKey {
		isPK[idx] = true
	}

	// 1. Decode each non-PK column
	rest := val
	var err error
	for i := range schema.Cols {
		if !isPK[i] {
			rest, err = row[i].Decode(rest)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type UpdateMode int

const (
	ModeUpsert UpdateMode = 0
	ModeInsert UpdateMode = 1
	ModeUpdate UpdateMode = 2
)

func (kv *KV) SetEx(key []byte, val []byte, mode UpdateMode) (bool, error) {
	kStr := string(key)

	// check state of val
	_, exists := kv.mem[kStr]

	switch mode {
	case ModeInsert:
		if exists {
			return false, nil
		}
	case ModeUpdate:
		if !exists {
			return false, nil
		}
	}

	// update
	content := make([]byte, len(val))
	copy(content, val)
	kv.mem[kStr] = content

	return true, nil
}

type DB struct {
	KV KV
}

func (db *DB) Insert(schema *Schema, row Row) (bool, error) {
	key := row.EmcodeKey(schema)
	val := row.EncodeVal(schema)

	return db.KV.SetEx(key, val, ModeInsert)
}

func (db *DB) Update(schema *Schema, row Row) (bool, error) {
	key := row.EmcodeKey(schema)
	val := row.EncodeVal(schema)

	return db.KV.SetEx(key, val, ModeUpdate)
}

func (db *DB) Upseret(schema *Schema, row Row) (bool, error) {
	key := row.EmcodeKey(schema)
	val := row.EncodeVal(schema)

	return db.KV.SetEx(key, val, ModeUpsert)
}

func (db *DB) Select(schema *Schema, row Row) (bool, error) {
	key := row.EmcodeKey(schema)

	// fetch val from kv
	val, ok, err := db.KV.Get(key)
	if err != nil || !ok {
		return ok, err
	}

	err = row.DecodeVal(schema, val)
	return true, err
}

func (db *DB) Delete(schema *Schema, row Row) (bool, error) {
	key := row.EmcodeKey(schema)

	return db.KV.Del(key)
}
