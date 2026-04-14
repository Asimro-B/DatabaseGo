package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
)

// LSMTree-writing a new data and later merging it, instead uf updating in place
// BTree-update data directly on disk and modify existing structure

type KVMetaData struct {
	Version uint64
	SSTable string
}

// Always keep: one old valid copy one new copy So after crash: at least one is correct

type KVMetaStore struct {
	slots [2]KVMetaItem
}

type KVMetaItem struct {
	Filename string
	fp       *os.File
	data     KVMetaData
}

func (meta *KVMetaStore) Set(data KVMetaData) error {
	// identify the slot with the smallest version
	idx := 0
	if meta.slots[1].data.Version < meta.slots[0].data.Version {
		idx = 1
	}

	// serialize and calculate chesum
	payload, _ := json.Marshal(data)

	// write to the slot
	slot := meta.slots[idx]
	if _, err := slot.fp.WriteAt(payload, 0); err != nil {
		return err
	}

	// force the hardware to persist the data
	if err := slot.fp.Sync(); err != nil {
		return err
	}

	// update memory slot after successfullu sync to the disk
	meta.slots[idx].data = data

	return nil
}

func (meta *KVMetaStore) Get() KVMetaData {
	if meta.slots[1].data.Version > meta.slots[0].data.Version {
		return meta.slots[1].data
	}
	return meta.slots[0].data
}

type KVOptions struct {
	DirPath string
	// LSM-Tree
	LogShreshold int
	GrowthFactor float32
}

type KV struct {
	options KVOptions
	meta    KVMetaStore
	main    []SortedFile
	version uint64
	log     Log
	mem     map[string][]byte
	keys    [][]byte
	vals    [][]byte
}

// MapSortedKV adapts a Go map into the SortedKV interface by iterating keys
// in lexicographic order.
type MapSortedKV struct {
	m    map[string][]byte
	keys [][]byte
}

type MapSortedKVIter struct {
	src *MapSortedKV
	pos int
}

type SortedArray struct {
	keys    [][]byte
	vals    [][]byte
	deleted []bool // added
}

func (m *MapSortedKV) prepare() {
	if m.keys != nil {
		return
	}
	m.keys = make([][]byte, 0, len(m.m))
	for k := range m.m {
		m.keys = append(m.keys, []byte(k))
	}
	slices.SortFunc(m.keys, bytes.Compare)
}

func (m *MapSortedKV) Size() int {
	return len(m.m)
}

func (m *MapSortedKV) Iter() (SortedKVIter, error) {
	m.prepare()
	return &MapSortedKVIter{src: m, pos: 0}, nil
}

func (it *MapSortedKVIter) Valid() bool {
	return it.src != nil && it.pos >= 0 && it.pos < len(it.src.keys)
}

func (it *MapSortedKVIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.src.keys[it.pos]
}

func (it *MapSortedKVIter) Val() []byte {
	if !it.Valid() {
		return nil
	}
	return it.src.m[string(it.src.keys[it.pos])]
}

func (it *MapSortedKVIter) Next() error {
	if it.src == nil {
		return nil
	}
	if it.pos < len(it.src.keys) {
		it.pos++
	}
	return nil
}

func (it *MapSortedKVIter) Prev() error {
	return errors.New("MapSortedKVIter.Prev: not implemented")
}

func (kv KV) Compact() error {
	// increment version and prepare the new file
	kv.version++
	newFileName := fmt.Sprintf("sstable_%d", kv.version)
	newPath := path.Join(kv.options.DirPath, newFileName)

	file := SortedFile{FileName: newPath}
	mem := MapSortedKV{m: kv.mem}
	return file.CreateFromSorted(&mem)
}

func (kv *KV) Seek(key []byte) (SortedKVIter, error) {
	// prioritize memtable/newest
	mem := MapSortedKV{m: kv.mem}
	levels := MergedSortedKV{&mem}

	// SSTables: from newest to oldest
	for i := range kv.main {
		levels = append(levels, &kv.main[i])
	}

	iter, err := levels.Seek(key)
	if err != nil {
		return nil, err
	}

	return filterDeleted(iter)
}

func filterDeleted(iter SortedKVIter) (SortedKVIter, error) {
	// Current iterator sources do not expose tombstones as visible entries.
	// Keep the helper so Seek can stay consistent with a future tombstone-aware path.
	return iter, nil
}

func (kv *KV) ShouldMerge(level int) bool {
	// simple exponential rule
	// level 0 <= LogThreshold, level1 <= LogThreshold*GrowthFactor, level1 <= LogThreshold*GrowthFactor*2
	limit := float32(kv.options.LogShreshold)
	for i := 0; i < level; i++ {
		limit *= kv.options.GrowthFactor
	}
	return float32(kv.main[level].Size()) > limit
}
