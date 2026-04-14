package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
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
	file.nkeys = n

	// Lazily open for writing; callers typically just set FileName.
	if file.fp == nil {
		fp, err := os.OpenFile(file.FileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		file.fp = fp
	}

	// calculate where the first kv pair will be written
	currentOffset := int64(8 + (n * 8))

	// write count 'n' at the very begining
	countBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(countBuf, uint64(n))
	if _, err := file.fp.WriteAt(countBuf, 0); err != nil {
		return err
	}

	iter, err := kv.Iter()
	if err != nil {
		return err
	}
	for i := 0; iter.Valid(); i++ {
		// write the pointer to the current kv pair in the offset array
		offsefBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(offsefBuf, uint64(currentOffset))

		// position-8 bytes for N+(i*8 bytes per offset)
		if _, err := file.fp.WriteAt(offsefBuf, int64(8+(i*8))); err != nil {
			return err
		}

		// prepare the kv data
		key, val := iter.Key(), iter.Val()
		data := make([]byte, 8+len(key)+len(val))
		binary.LittleEndian.PutUint32(data[4:], uint32(len(key)))
		binary.LittleEndian.PutUint32(data[8:], uint32(len(val)))

		copy(data[8:], key)
		copy(data[8+len(key):], val)

		// write the data at the currentOffset
		if _, err := file.fp.WriteAt(data, currentOffset); err != nil {
			return err
		}

		// advance the offset for the next KV pair
		currentOffset += int64(len(data))
		if err := iter.Next(); err != nil {
			return err
		}
	}

	if err := file.fp.Sync(); err != nil {
		return err
	}
	if err := syncDir(file.FileName); err != nil {
		return err
	}
	return nil
}

func (file *SortedFile) Close() error {
	if file.fp == nil {
		return nil
	}
	err := file.fp.Close()
	file.fp = nil
	return err
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

func (file *SortedFile) Size() int {
	return file.nkeys
}

func (file *SortedFile) Iter() (SortedKVIter, error) {
	iter := &SortedFileIter{file: file, pos: 0}
	if err := iter.loadCurrent(); err != nil {
		return nil, err
	}
	return iter, nil
}

type KVRefac struct {
	log Log
	mem SortedArray
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

func (array *SortedArray) Size() int {
	return len(array.keys)
}

func (array *SortedArray) Iter() (SortedKVIter, error) {
	return &SortedArrayIter{array: array, pos: 0}, nil
}

func (kv *KVRefac) Seek(key []byte) (SortedKVIter, error) {
	return kv.mem.Seek(key)
}

type MergedSortedKV []SortedKV

type sortedKVSeeker interface {
	Seek(key []byte) (SortedKVIter, error)
}

func levelsLowest(levels []SortedKVIter) int {
	winner := -1
	for i, iter := range levels {
		if !iter.Valid() {
			continue
		}
		if winner == -1 {
			winner = i
			continue
		}

		// compare current winnere with this level
		cmp := bytes.Compare(iter.Key(), levels[winner].Key())
		if cmp < 0 {
			// found very lower key
			winner = i
		} else if cmp == 0 {
			// keys are identical
			iter.Next()
		}
	}
	return winner
}

type MergedSortedKVIter struct {
	levels []SortedKVIter
	which  int
}

func (iter *MergedSortedKVIter) Valid() bool {
	return iter.which >= 0 &&
		iter.which < len(iter.levels) &&
		iter.levels[iter.which].Valid()
}

func (iter *MergedSortedKVIter) Key() []byte {
	if !iter.Valid() {
		return nil
	}
	return iter.levels[iter.which].Key()
}

func (iter *MergedSortedKVIter) Val() []byte {
	if !iter.Valid() {
		return nil
	}
	return iter.levels[iter.which].Val()
}

func (iter *MergedSortedKVIter) Next() error {
	if !iter.Valid() {
		return nil
	}
	if err := iter.levels[iter.which].Next(); err != nil {
		return err
	}
	iter.which = levelsLowest(iter.levels)
	return nil
}

func (iter *MergedSortedKVIter) Prev() error {
	return errors.New("MergedSortedKVIter.Prev: not implemented")
}

func (m MergedSortedKV) Iter() (iter SortedKVIter, err error) {
	levels := make([]SortedKVIter, len(m))

	for i, sub := range m {
		if levels[i], err = sub.Iter(); err != nil {
			return nil, err
		}
	}

	return &MergedSortedKVIter{
		levels: levels,
		which:  levelsLowest(levels),
	}, nil
}

func (m MergedSortedKV) Seek(key []byte) (iter SortedKVIter, err error) {
	levels := make([]SortedKVIter, len(m))
	for i, sub := range m {
		if seeker, ok := sub.(sortedKVSeeker); ok {
			if levels[i], err = seeker.Seek(key); err != nil {
				return nil, err
			}
			continue
		}

		// Fallback for SortedKV implementations without Seek:
		// start from Iter and advance until current key >= target key.
		if levels[i], err = sub.Iter(); err != nil {
			return nil, err
		}
		for levels[i].Valid() && bytes.Compare(levels[i].Key(), key) < 0 {
			if err := levels[i].Next(); err != nil {
				return nil, err
			}
		}
	}

	return &MergedSortedKVIter{
		levels: levels,
		which:  levelsLowest(levels),
	}, nil
}

func (m MergedSortedKV) Size() int {
	iter, err := m.Iter()
	if err != nil {
		return 0
	}

	n := 0
	for iter.Valid() {
		n++
		if err := iter.Next(); err != nil {
			break
		}
	}
	return n
}

type KVMerged struct {
	log  Log
	mem  SortedArray
	main SortedFile
}

type SortedArrayMerged struct {
	keys    [][]byte
	vals    [][]byte
	deleted []bool
}

type SortedArrayIterMerged struct {
	keys    [][]byte
	vals    [][]byte
	deleted []bool
	pos     int
}

func syncDir(path string) error {
	dir := filepath.Dir(path)
	df, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer df.Close()
	return df.Sync()
}

func renameSync(oldPath, newPath string) error {
	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}
	// Ensure the directory entry update is durable.
	if err := syncDir(newPath); err != nil {
		return err
	}
	// If the rename crossed directories, sync the source dir too.
	if filepath.Dir(oldPath) != filepath.Dir(newPath) {
		if err := syncDir(oldPath); err != nil {
			return err
		}
	}
	return nil
}

func randomTempPath() string {
	f, err := os.CreateTemp("", "kv-compact-*")
	if err != nil {
		return filepath.Join(os.TempDir(), "kv-compact.tmp")
	}
	_ = f.Close()
	return f.Name()
}
func (kv *KVMerged) Compact() error {
	// 1. Merge MemTable and SSTable, output to a temporary file.
	path := randomTempPath()
	defer os.Remove(path)
	file := SortedFile{FileName: path}
	m := MergedSortedKV{&kv.mem, &kv.main}
	if err := file.CreateFromSorted(m); err != nil {
		return err
	}
	// 2. Replace the original SSTable (atomic operation).
	if err := renameSync(file.FileName, kv.main.FileName); err != nil {
		_ = file.Close()
		return err
	}
	file.FileName = kv.main.FileName
	_ = kv.main.Close()
	kv.main = file
	// 3. Drop the MemTable and the log.
	kv.mem.Clear()
	return kv.log.Truncate()
}

func (l *Log) Truncate() error {
	// Reset log file contents and reopen the file handle for further use.
	if l.fp != nil {
		_ = l.fp.Close()
		l.fp = nil
	}
	if err := os.Truncate(l.FileName, 0); err != nil {
		return err
	}
	return l.Open()
}

func syncDire(file string) error
func RenameSync(src, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		return err
	}

	return syncDire(dst)
}
