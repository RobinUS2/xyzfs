package main

import (
	"bytes"
	"encoding/binary"
	"sync"
)

// Metadata on a shard

type ShardMeta struct {
	MetaVersion    uint32
	FileCount      uint32
	IndexLength    uint32
	FileMetaLength uint32
	ContentsLength uint32

	// Lock
	mux sync.RWMutex
}

// File count
func (this *Shard) FileCount() uint32 {
	this.shardMeta.mux.RLock()
	defer this.shardMeta.mux.RUnlock()
	return this.shardMeta.FileCount
}

// Set index length
func (this *ShardMeta) SetIndexLength(v uint32) {
	this.mux.Lock()
	// Should never be empty
	if v < 1 {
		panic("Index length seems to be invalid")
	}
	this.IndexLength = v
	this.mux.Unlock()
}

// Set contents length
func (this *ShardMeta) SetContentsLength(v uint32) {
	this.mux.Lock()
	// Empty is acceptable
	if v < 0 {
		panic("Content length seems to be invalid")
	}
	this.ContentsLength = v
	this.mux.Unlock()
}

// Set file metadata length
func (this *ShardMeta) SetFileMetaLength(v uint32) {
	this.mux.Lock()
	// Should never be empty
	if v < 1 {
		panic("File meta length length seems to be invalid")
	}
	this.FileMetaLength = v
	this.mux.Unlock()
}

// To bytes
func (this *ShardMeta) Bytes() []byte {
	this.mux.RLock()
	defer this.mux.RUnlock()
	buf := new(bytes.Buffer)
	buf.Write(BINARY_METADATA_MAGIC_STRING)                     // magic string
	binary.Write(buf, binary.BigEndian, this.MetaVersion)       // meta version
	binary.Write(buf, binary.BigEndian, this.FileCount)         // file count
	binary.Write(buf, binary.BigEndian, this.IndexLength)       // bloom filter length
	binary.Write(buf, binary.BigEndian, this.FileMetaLength)    // file metadata length
	binary.Write(buf, binary.BigEndian, this.ContentsLength)    // contents (actual file bytes) length
	binary.Write(buf, binary.BigEndian, BINARY_METADATA_LENGTH) // length of the metadata
	return buf.Bytes()
}

// From bytes
func (this *ShardMeta) FromBytes(b []byte) {
	buf := bytes.NewReader(b)
	buf.Seek(int64(len(BINARY_METADATA_MAGIC_STRING)), 0) // Skip magic string
	var err error
	err = binary.Read(buf, binary.BigEndian, &this.MetaVersion) // meta version
	panicErr(err)
	err = binary.Read(buf, binary.BigEndian, &this.FileCount) // file count
	panicErr(err)
	err = binary.Read(buf, binary.BigEndian, &this.IndexLength) // bloom filter length
	panicErr(err)
	err = binary.Read(buf, binary.BigEndian, &this.FileMetaLength) // file metadata length
	panicErr(err)
	err = binary.Read(buf, binary.BigEndian, &this.ContentsLength) // contents (actual file bytes) length
	panicErr(err)
	// We don't have to read the binary metadata length, because we already know
}

// Version
const BINARY_VERSION uint32 = 1
const BINARY_METADATA_LENGTH uint32 = 3 + 4 + 4 + 4 + 4 + 4 + 4

var BINARY_METADATA_MAGIC_STRING []byte = []byte("YXZ")

// New metadata
func newShardMeta() *ShardMeta {
	return &ShardMeta{
		MetaVersion:    BINARY_VERSION,
		FileCount:      0,
		IndexLength:    0,
		FileMetaLength: 0,
	}
}

// Binary file format description
// [3]byte - Magic header (string XYZ)
// uint32 - Meta Version - Numeric incremental ID that indicates the version of this file
// uint32 - FileCount - Number of files in this shard
// uint32 - Number of bytes that the metadata takes
// uint32 - IndexLength - Number of bytes that contains the ShardIndex
// uint32 - FileMetaLength - Number of bytes that contains the file metadata contents
// uint32 - ContentsLength - Number of bytes that contain the actual file bytes
