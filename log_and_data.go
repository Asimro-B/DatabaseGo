package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"slices"
)

type SortedFile struct {
	FileName string
	fp       *os.File
	nkeys    int
}

type SortedKV interface {
	Size() int
	Iter() (SortedKVIter, error)
}

type SortedKVIter interface {
	Valid() bool
	Key() []byte
	Val() []byte
	Next() error
	Prev() error
}

func (file *SortedFile) CreateFromSorted(kv SortedKV) error {
	n := kv.Size()
	// calculate where the first kv pair will be written
	currentOffset := int64(8 + (n * 8))

	// write count 'n' at the very begining
	countBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(countBuf, uint64(n))
	if _, err := file.fp.WriteAt(countBuf, 0); err != nil {
		return err
	}

	iter, _ := kv.Iter()
	for i := 0; iter.Valid(); i++ {
		// write the pointer to the current kv pair in the offset array
		offsefBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(offsefBuf, uint64(currentOffset))

		// position-8 bytes for N+(i*8 bytes per offset)
		file.fp.WriteAt(offsefBuf, int64(8+(i*8)))

		// prepare the kv data
		key, val := iter.Key(), iter.Val()
		data := make([]byte, 8+len(key)+len(val))
		binary.LittleEndian.PutUint32(data[4:], uint32(len(key)))
		binary.LittleEndian.PutUint32(data[8:], uint32(len(val)))

		copy(data[8:], key)
		copy(data[8+len(key):], val)

		// write the data at the currentOffset
		file.fp.WriteAt(data, currentOffset)

		// advance the offset for the next KV pair
		currentOffset += int64(len(data))
		iter.Next()
	}
	return nil
}

func (file *SortedFile) index(pos int) (key, val []byte, err error) {
	var buf [8]byte

	if _, err := file.fp.ReadAt(buf[:], int64(8+8*pos)); err != nil {
		return nil, nil, err
	}

	// kv offset
	offset := int64(binary.LittleEndian.Uint64(buf[:]))
	if int64(8+8*file.nkeys) > offset {
		return nil, nil, errors.New("corrupted file")
	}

	// read kv
	if _, err := file.fp.ReadAt(buf[:], offset); err != nil {
		return nil, nil, err
	}

	keyLen := binary.LittleEndian.Uint32(buf[0:4])
	valLen := binary.LittleEndian.Uint32(buf[4:8])

	// read the actual kv data
	data := make([]byte, keyLen+valLen)
	if _, err := file.fp.ReadAt(data, offset+8); err != nil {
		return nil, nil, err
	}

	return data[:keyLen], data[keyLen:], nil
}

type SortedFileIter struct {
	file  *SortedFile
	pos   int
	key   []byte
	val   []byte
	valid bool
}

func (iter *SortedFileIter) loadCurrent() error {
	if iter.pos < 0 || iter.pos >= iter.file.nkeys {
		iter.key = nil
		iter.val = nil
		iter.valid = false
		return nil
	}

	key, val, err := iter.file.index(iter.pos)
	if err != nil {
		iter.key = nil
		iter.val = nil
		iter.valid = false
		return err
	}

	iter.key = key
	iter.val = val
	iter.valid = true
	return nil
}

func (iter *SortedFileIter) Valid() bool {
	return iter.valid
}

func (iter *SortedFileIter) Key() []byte {
	return iter.key
}

func (iter *SortedFileIter) Val() []byte {
	return iter.val
}

func (iter *SortedFileIter) Next() error {
	if iter.pos < iter.file.nkeys {
		iter.pos++
	}
	return iter.loadCurrent()
}

func (iter *SortedFileIter) Prev() error {
	if iter.pos >= 0 {
		iter.pos--
	}
	return iter.loadCurrent()
}

func (file *SortedFile) Seek(key []byte) (SortedKVIter, error) {
	low, high := 0, file.nkeys-1
	pos := file.nkeys

	for low <= high {
		mid := low + (high-low)/2
		midKey, _, err := file.index(mid)
		if err != nil {
			return nil, err
		}

		if bytes.Compare(midKey, key) >= 0 {
			pos = mid
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	iter := &SortedFileIter{file: file, pos: pos}
	if err := iter.loadCurrent(); err != nil {
		return nil, err
	}

	return iter, nil
}

type KVRefac struct {
	log Log
	mem SortedArray
}
type SortedArray struct {
	keys [][]byte
	vals [][]byte
}

type SortedArrayIter struct {
	array *SortedArray
	pos   int
}

func (array *SortedArray) Clear() {
	array.keys = array.keys[:0]
	array.vals = array.vals[:0]
}

func (array *SortedArray) Pop(key, val []byte) {
	array.keys = array.keys[:len(array.keys)-1]
	array.vals = array.vals[:len(array.vals)-1]
}

func (array *SortedArray) Push(key, val []byte) {
	array.keys = append(array.keys, key)
	array.vals = append(array.vals, val)
}

func (iter *SortedArrayIter) Valid() bool {
	return 0 <= iter.pos && iter.pos < len(iter.array.keys)
}

func (iter *SortedArrayIter) Key() []byte {
	return iter.array.keys[iter.pos]
}

func (iter *SortedArrayIter) Val() []byte {
	return iter.array.vals[iter.pos]
}

func (iter *SortedArrayIter) Next() error {
	if iter.pos < len(iter.array.keys) {
		iter.pos++
	}
	return nil
}

func (iter *SortedArrayIter) Prev() error {
	if iter.pos >= 0 {
		iter.pos--
	}
	return nil
}

func (array *SortedArray) Seek(key []byte) (SortedKVIter, error) {
	pos, _ := slices.BinarySearchFunc(array.keys, key, bytes.Compare)
	return &SortedArrayIter{array: array, pos: pos}, nil
}

func (kv *KVRefac) Seek(key []byte) (SortedKVIter, error) {
	return kv.mem.Seek(key)
}
