package main

import (
	"bytes"
	"encoding/binary"
	"sync"
)

// Metadata on a shard

type ShardMeta struct {
	MetaVersion uint32
	FileCount   uint32

	// Lock
	mux sync.RWMutex
}

// File count
func (this *Shard) FileCount() uint32 {
	this.shardMeta.mux.RLock()
	defer this.shardMeta.mux.RUnlock()
	return this.shardMeta.FileCount
}

// To bytes
func (this *ShardMeta) Bytes() []byte {
	this.mux.RLock()
	defer this.mux.RUnlock()
	buf := new(bytes.Buffer)
	buf.Write(BINARY_METADATA_MAGIC_STRING)                     // magic string
	binary.Write(buf, binary.BigEndian, this.MetaVersion)       // version
	binary.Write(buf, binary.BigEndian, this.FileCount)         // number of files
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
	// We don't have to read the binary metadata length, because we already know
}

// Version
const BINARY_VERSION uint32 = 1
const BINARY_METADATA_LENGTH uint32 = 3 + 4 + 4 + 4

var BINARY_METADATA_MAGIC_STRING []byte = []byte("YXZ")

// New metadata
func newShardMeta() *ShardMeta {
	return &ShardMeta{
		MetaVersion: BINARY_VERSION,
		FileCount:   0,
	}
}

// Binary file format description
// [3]byte - Magic header (string XYZ)
// uint32 - Meta Version - Numeric incremental ID that indicates the version of this file
// uint32 - FileCount - Number of files in this shard
// uint32 - Number of bytes that the metadata takes
