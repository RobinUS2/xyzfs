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

// Add to index
func (this *ShardIndex) Add(fullName string) {
	this.mux.Lock()
	this.bloomFilter.Add([]byte(fullName))
	this.mux.Unlock()
}

// Test index contains this file
func (this *ShardIndex) Test(fullName string) bool {
	this.mux.RLock()
	res := this.bloomFilter.Test([]byte(fullName))
	this.mux.RUnlock()
	return res
}

// To bytes
func (this *ShardIndex) Bytes() []byte {
	this.mux.RLock()
	bytes, e := this.bloomFilter.GobEncode()
	panicErr(e)
	this.mux.RUnlock()
	return bytes
}

// From bytes
func (this *ShardIndex) LoadBytes(b []byte) {
	this.mux.Lock()
	e := this.bloomFilter.GobDecode(b)
	panicErr(e)
	this.mux.Unlock()
}

func newShardIndex() *ShardIndex {
	return &ShardIndex{
		bloomFilter: bloom.New(9585059, 7), // 1% error rate for 1MM items
	}
}
