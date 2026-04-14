package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
)

type KVIterator struct {
	keys [][]byte
	vals [][]byte
	pos  int // position
}

func (kv *KV) SeekArray(key []byte) (*KVIterator, error) {
	pos, _ := slices.BinarySearchFunc(kv.keys, key, bytes.Compare)
	return &KVIterator{keys: kv.keys, vals: kv.vals, pos: pos}, nil
}

func (iter *KVIterator) Valid() bool {
	return 0 <= iter.pos && iter.pos < len(iter.keys)
}

func (iter *KVIterator) Key() []byte {
	return iter.keys[iter.pos]
}
func (iter *KVIterator) Val() []byte {
	return iter.vals[iter.pos]
}

func (iter *KVIterator) Next() error {
	if iter.pos < len(iter.keys) {
		iter.pos++
	}
	return nil
}
func (iter *KVIterator) Prev() error {
	if iter.pos >= 0 {
		iter.pos--
	}
	return nil
}

func encodeStrKey(toAppend []byte, input []byte) []byte {
	for _, ch := range input {
		if ch == 0*00 || ch == 0*01 {
			toAppend = append(toAppend, 0*01, ch+1)
		} else {
			toAppend = append(toAppend, ch)
		}
	}

	return append(toAppend, 0*00)
}

func (cell *Cell) EncodeKey(out []byte) []byte {
	out = append(out, uint8(cell.Type))
	switch cell.Type {
	case TypeI64:
		// correct sort order= bigEndia+sign flip
		return binary.BigEndian.AppendUint64(out, uint64(cell.I64)^(1<<63))
	case TypeStr:
		return encodeStrKey(out, cell.str)
	}
	return out
}

func decodeStrKey(data []byte) ([]byte, []byte, error) {
	var out []byte
	for i := 0; i < len(data); i++ {
		if data[i] == 0x00 {
			return out, data[i+1:], nil // Found the end
		}
		if data[i] == 0x01 {
			i++ // Look at the next byte
			if i >= len(data) {
				return nil, nil, errors.New("bad escape")
			}
			if data[i] == 0x01 {
				out = append(out, 0x00)
			} else if data[i] == 0x02 {
				out = append(out, 0x01)
			} else {
				return nil, nil, errors.New("bad escape")
			}
		} else {
			out = append(out, data[i])
		}
	}
	return nil, nil, errors.New("missing null terminator")
}

type RowIterator struct {
	schema *Schema
	iter   *KVIterator
	valid  bool // decode result (err != ErrOutOfRange)
	row    Row  // decode result
}

func DecodeKVIter(schema *Schema, iter *KVIterator, row Row) (bool, error) {
	if !iter.Valid() {
		return false, nil
	}

	// decode the key
	err := row.DecodeKey(schema, iter.Key())
	if err == ErrOutOfRange {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// Decode the value
	err = row.DecodeVal(schema, iter.Val())
	if err != nil {
		return false, err
	}

	return true, nil
}

func (db *DB) Seek(schema *Schema, row Row) (*RowIterator, error) {
	// encode the key
	key := row.EmcodeKey(schema)

	// seek the underling kv store
	kvIter, err := db.KV.SeekArray(key)
	if err != nil {
		return nil, err
	}

	// create RowIterator wrapper
	iter := &RowIterator{
		schema: schema,
		iter:   kvIter,
		valid:  true,
		row:    row,
	}

	// prime the iterator by decoding the 1st match
	iter.valid, err = DecodeKVIter(schema, kvIter, row)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

type RangedKVIter struct {
	iter KVIterator
	stop []byte
	desc bool
}

func (iter *RangedKVIter) Key() []byte {
	return iter.iter.Key()
}

func (iter *RangedKVIter) Val() []byte {
	return iter.iter.Val()
}

func (iter *RangedKVIter) Valid() bool {
	if !iter.iter.Valid() {
		return false
	}

	r := bytes.Compare(iter.iter.Key(), iter.stop)
	if iter.desc && r < 0 {
		return false
	} else if !iter.desc && r > 0 {
		return false
	}
	return true
}

func (iter *RangedKVIter) Next() error {
	if !iter.Valid() {
		return nil
	}
	if iter.desc {
		return iter.iter.Prev()
	} else {
		return iter.iter.Next()
	}
}

func (kv *KV) Range(start, stop []byte, desc bool) (*RangedKVIter, error) {
	iter, err := kv.SeekArray(start)
	if err != nil {
		return nil, err
	}

	// if we are decs seek() goes to 1st key > start and we go to prev to get the range[start, stop]
	if desc {
		if !iter.Valid() || bytes.Compare(iter.Key(), start) > 0 {
			iter.Prev()
		}
	}

	return &RangedKVIter{
		iter: *iter,
		stop: stop,
		desc: true,
	}, nil
}

func (row Row) EncodeKey(schema *Schema) []byte {
	key := append([]byte(schema.Table), 0x00)
	for _, idx := range schema.PKey {
		cell := row[idx]
		key = append(key, byte(cell.Type)) // avoid 0xff
		key = cell.EncodeKey(key)
	}
	return append(key, 0x00) // > -infinity
}

func EncodeKeyPrefix(schema *Schema, prefix []Cell, positive bool) []byte {
	key := append([]byte(schema.Table), 0x00)
	for _, cell := range prefix {
		key = append(key, byte(cell.Type)) // avoid 0xff
		key = cell.EncodeKey(key)
	}
	if positive {
		key = append(key, 0xff) // +infinity
	} // -infinity
	return key
}

type ExprOp uint8

const (
	// Comparison Operators (from previous step)
	OP_EQ ExprOp = 1 // =
	OP_NE ExprOp = 2 // !=
	OP_LT ExprOp = 3 // <
	OP_LE ExprOp = 4 // <=
	OP_GT ExprOp = 5 // >
	OP_GE ExprOp = 6 // >=

	// Arithmetic Operators
	OP_ADD ExprOp = 10 // +
	OP_SUB ExprOp = 11 // -
	OP_MUL ExprOp = 12 // *
	OP_DIV ExprOp = 13 // /

	// Logical Operators
	OP_AND ExprOp = 20
	OP_OR  ExprOp = 21
	OP_NOT ExprOp = 22
)
