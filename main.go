package main

import (
	"bytes"
	"slices"
)

// Get retrieves a value for a given key.
// Returns the value, a boolean indicating if it was found, and an error.
func (kv *KV) Get(key []byte) (val []byte, ok bool, err error) {
	if idx, ok := slices.BinarySearchFunc(kv.keys, key, bytes.Compare); ok {
		return kv.vals[idx], true, nil
	}
	return nil, false, nil
}

// Set inserts or updates a key-value pair.
// Returns 'updated' as true if the key already existed with different data.
func (kv *KV) Set1(key []byte, val []byte) (updated bool, err error) {
	kStr := string(key)

	// Check if the key exists and if the value is actually different
	oldVal, exists := kv.mem[kStr]

	// We use bytes.Equal to check if the content is actually changing.
	// This helps us fulfill the requirement of reporting state changes.
	if exists && !bytes.Equal(oldVal, val) {
		updated = true
	} else if !exists {
		// If it didn't exist before, the state is changing (new entry).
		updated = true
	} else {
		// Key exists and value is the same; no state change.
		updated = false
	}

	// Store a copy of the value to ensure the DB remains independent
	// of the caller's slice memory.
	content := make([]byte, len(val))
	copy(content, val)
	kv.mem[kStr] = content

	return updated, nil
}

// Del removes a key from the database.
// Returns 'deleted' as true if the key existed and was removed.
func (kv *KV) Del1(key []byte) (deleted bool, err error) {
	kStr := string(key)

	_, exists := kv.mem[kStr]
	if !exists {
		return false, nil
	}

	delete(kv.mem, kStr)
	return true, nil
}
