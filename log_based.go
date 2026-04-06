package main

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path"
	"syscall"
)

type Entry struct {
	key     []byte
	val     []byte
	deleted bool
}

var ErrBadSum = errors.New("bad checksum")

func (e *Entry) Encode() []byte {
	// 1, allocate the total memory needed
	// we need 4 bytes(key size), 4 bytes value size, actual length of key and actual length of value
	checkSum := 4
	headerSize := 4 + 4 + 1
	data := make([]byte, checkSum+headerSize+len(e.key)+len(e.val))

	// 2. Write the metadata (the "Size" headers)
	// binary.LittleEndian.PutUint32 takes a number and turns it into 4 bytes
	// If len(ent.key) is 1, it writes [1, 0, 0, 0] into the first 4 slots of 'data'
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(e.key)))

	// Do the same for the value length in the next 4 slots
	binary.LittleEndian.PutUint32(data[8:12], uint32(len(e.val)))

	if e.deleted {
		data[8] = 1
	} else {
		data[8] = 0
	}

	// 3. Write the actual content (the "Data")
	// Copy the key bytes starting at index 8 (right after the headers)
	copy(data[13:], e.key)

	// Copy the value bytes starting right after the key
	copy(data[13+len(e.key):], e.val)

	// calculate the checksum of everything after the 1st bytes
	sum := crc32.ChecksumIEEE(data[4:])
	binary.LittleEndian.PutUint32(data[:4], sum)

	return data
}

func (e *Entry) Decode(r io.Reader) error {
	// prepare 8byte to read data at once 4 for key and 4 for value
	header := make([]byte, 13) //4+4+1
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	// convert those bytes to int
	storedSum := binary.LittleEndian.Uint32(header[0:4])
	keyLen := binary.LittleEndian.Uint32(header[4:8])
	valLen := binary.LittleEndian.Uint32(header[8:12])
	e.deleted = (header[8] == 1)

	e.key = make([]byte, keyLen)
	e.val = append(e.val, byte(valLen))

	// fill slice with data from reader
	if _, err := io.ReadFull(r, e.key); err != nil {
		return err
	}
	if _, err := io.ReadFull(r, e.val); err != nil {
		return err
	}

	// recalculate the checksum to verify integrity
	// we need to check the exact same bytes we hashed during encode
	actualSum := crc32.ChecksumIEEE(header[4:])
	actualSum = crc32.Update(actualSum, crc32.IEEETable, e.key)
	actualSum = crc32.Update(actualSum, crc32.IEEETable, e.val)

	if storedSum != actualSum {
		return ErrBadSum
	}

	return nil
}

type Log struct {
	FileName string
	fp       *os.File
}

func (l *Log) Open() (err error) {
	l.fp, err = CreateFileSync(l.FileName)
	return err
}

func (l *Log) Close() error {
	return l.fp.Close()
}

func (l *Log) Write(e *Entry) error {
	if _, err := l.fp.Write(e.Encode()); err != nil {
		return err
	}
	return l.fp.Sync()
}

func (l *Log) Read(e *Entry) (eof bool, err error) {
	err = e.Decode(l.fp)
	if err == io.EOF || err == io.ErrUnexpectedEOF || err == ErrBadSum {
		return true, nil
	} else if err != nil {
		return false, err
	} else {
		return false, nil
	}
}

type KV struct {
	log  Log
	mem  map[string][]byte
	keys [][]byte
	vals [][]byte
}

func (kv *KV) Open() error {
	if err := kv.log.Open(); err != nil {
		return err
	}

	kv.mem = make(map[string][]byte)

	// reply the log from start to finish
	for {
		var ent Entry
		eof, err := kv.log.Read(&ent)
		if err != nil {
			return err
		}
		if eof {
			break
		}

		// apply the log entry to the in-memory map
		if ent.deleted {
			delete(kv.mem, string(ent.key))
		} else {
			kv.mem[string(ent.key)] = ent.val
		}
	}
	return nil
}

func (kv *KV) Close() error {
	return kv.log.Close()
}

func (kv *KV) Set(key []byte, val []byte) (bool, error) {
	// create entry object for the log
	ent := &Entry{key: key, val: val, deleted: false}

	// persist the disk, if it fails stop here for data consistency
	if err := kv.log.Write(ent); err != nil {
		return false, err
	}

	// update memory
	keyStr := string(key)

	valCopy := make([]byte, len(val))
	copy(valCopy, val)

	kv.mem[keyStr] = valCopy

	return true, nil
}

func (kv *KV) Del(key []byte) (deleted bool, err error) {
	keyStr := string(key)

	// check if the key exists
	if _, exists := kv.mem[keyStr]; !exists {
		return false, err
	}

	// in append log, we don't erase old data, we just add deleted marker
	ent := &Entry{key: []byte(keyStr), val: nil, deleted: true}

	// persist the deletion to the log
	if err := kv.log.Write(ent); err != nil {
		return false, err
	}

	// remove the key from the memory map
	delete(kv.mem, keyStr)

	return true, nil
}

func SyncDir(file string) error {
	// 1, get the path to the folder which contain the file
	dirPath := path.Dir(file)

	// 2, open the folder in read only mode
	dirfd, err := syscall.Open(dirPath, os.O_RDONLY|syscall.O_DIRECTORY, 0644)
	if err != nil {
		return err
	}
	// close the folder to avoid leakage
	defer syscall.Close(dirfd)

	// 3, force the folders metadata to the disk
	return syscall.Fsync(dirfd)
}

func CreateFileSync(file string) (*os.File, error) {
	fp, err := os.OpenFile(file, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err = SyncDir(path.Base(file)); err != nil {
		_ = fp.Close()
		return nil, err
	}

	return fp, nil
}
