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
	binary.Write(buf, binary.BigEndian, this.MetaVersion)
	binary.Write(buf, binary.BigEndian, this.FileCount)
	return buf.Bytes()
}

// From bytes
func (this *ShardMeta) FromBytes(b []byte) {
	buf := bytes.NewReader(b)
	var err error
	err = binary.Read(buf, binary.BigEndian, &this.MetaVersion)
	panicErr(err)
	err = binary.Read(buf, binary.BigEndian, &this.FileCount)
	panicErr(err)
}

// Version
const BINARY_VERSION uint32 = 1

// New metadata
func newShardMeta() *ShardMeta {
	return &ShardMeta{
		MetaVersion: BINARY_VERSION,
		FileCount:   0,
	}
}

// Binary file format description
// uint32 - Meta Version - Numeric incremental ID that indicates the version of this file
// uint32 - FileCount - Number of files in this shard
