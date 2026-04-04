package main

import (
	"encoding/binary"
	"errors"
	"io"
)

type cellType uint

const (
	TypeI64 cellType = 1
	TypeStr cellType = 2
)

type Cell struct {
	Type cellType
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
	cell.Type = cellType(data[0])
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
