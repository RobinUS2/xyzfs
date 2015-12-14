package main

import (
	"bytes"
	"encoding/binary"
	"sync"
)

// Metadata on a shard

type ShardMeta struct {
	mux       sync.RWMutex
	FileCount uint32
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
	binary.Write(buf, binary.BigEndian, this.FileCount)
	return buf.Bytes()
}

// From bytes
func (this *ShardMeta) FromBytes(b []byte) {
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.BigEndian, &this.FileCount)
	panicErr(err)
}

func newShardMeta() *ShardMeta {
	return &ShardMeta{}
}
