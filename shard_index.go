package main

import (
	"bytes"
	"encoding/binary"
	"github.com/willf/bloom"
	"sync"
)

// Index on a shard

type ShardIndex struct {
	// Internal bloom filter
	bloomFilter *bloom.BloomFilter

	// Mutex
	mux sync.RWMutex

	// Size
	size          uint32
	hashFunctions uint32

	// Id
	ShardId []byte
}

// Add to index
func (this *ShardIndex) Add(fullName string) bool {
	this.mux.Lock()
	this.bloomFilter.Add([]byte(fullName))
	this.mux.Unlock()
	return true
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
	buf := new(bytes.Buffer)
	this.mux.RLock()
	bytes, e := this.bloomFilter.GobEncode()
	panicErr(e)
	binary.Write(buf, binary.BigEndian, uint32(len(bytes))) // Length of index
	buf.Write(bytes)                                        // Actual index
	idLen := uint32(len(this.ShardId))
	if idLen != 16 {
		panic("Id empty")
	}
	binary.Write(buf, binary.BigEndian, idLen) // Length of shard ID
	buf.Write(this.ShardId)                    // Shard ID

	// Unlock
	this.mux.RUnlock()

	return buf.Bytes()
}

// From bytes
func (this *ShardIndex) FromBytes(b []byte) {
	this.mux.Lock()

	buf := bytes.NewReader(b)
	var err error

	// Index length
	var idxLen uint32
	err = binary.Read(buf, binary.BigEndian, &idxLen) // index length
	panicErr(err)

	// Read actual index
	indexBytes := make([]byte, idxLen)
	indexBytesRead, _ := buf.Read(indexBytes)
	if uint32(indexBytesRead) != idxLen {
		panic("Index bytes read mismatch")
	}

	// Decode index
	e := this.bloomFilter.GobDecode(indexBytes)
	panicErr(e)

	// Set size and hash functions from bloom filter
	this.size = uint32(this.bloomFilter.Cap())
	this.hashFunctions = uint32(this.bloomFilter.K())

	// Read shard ID length
	var shardIdLen uint32
	err = binary.Read(buf, binary.BigEndian, &shardIdLen)

	panicErr(err)
	shardIdBytes := make([]byte, shardIdLen)
	shardIdBytesRead, _ := buf.Read(shardIdBytes)
	if uint32(shardIdBytesRead) != shardIdLen {
		panic("Shard id bytes read mismatch")
	}
	this.ShardId = shardIdBytes

	this.mux.Unlock()
}

func newShardIndex(id []byte) *ShardIndex {
	size := uint(9585059)
	k := uint(7)
	return &ShardIndex{
		ShardId:       id,
		bloomFilter:   bloom.New(size, k), // 1% error rate for 1MM items
		size:          uint32(size),
		hashFunctions: uint32(k),
	}
}
