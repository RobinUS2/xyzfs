package main

import (
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

func newShardMeta() *ShardMeta {
	return &ShardMeta{}
}
