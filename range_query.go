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

func (kv *KV) Seek(key []byte) (*KVIterator, error) {
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
