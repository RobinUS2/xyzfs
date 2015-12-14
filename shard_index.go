package main

import (
	"github.com/willf/bloom"
	"sync"
)

// Index on a shard

type ShardIndex struct {
	// Internal bloom filter
	bloomFilter *bloom.BloomFilter

	// Mutex
	mux sync.RWMutex
}

func newShardIndex() *ShardIndex {
	return &ShardIndex{
		bloomFilter: bloom.New(1000000, 7),
	}
}
