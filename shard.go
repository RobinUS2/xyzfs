package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"sync"
)

// Shard is a partial piece of data in a block
// @todo Store CRC32 checksum in meta file to validate content of shard

type Shard struct {
	Id         []byte // Unique UUID
	BlockIndex uint   // Numeric index (from to 0 to number_of_data_shards+number_of_parity_shards), used to align the shards before a restore using Reed Solomon
	Parity     bool   // Is this real data or parity?

	// Block reference
	block *Block

	// Buffer mode
	bufferMode    ShardBufferMode
	bufferModeMux sync.RWMutex

	// Byte buffers
	// - ON READ: contents + file metadata + shard index + shard metadata
	// - ON WRITE (before flush): contains content
	contents       *bytes.Buffer // Actual byte buffer in-memory, is lazy loaded, use Contents() method to get
	contentsMux    sync.RWMutex
	contentsOffset uint32

	// File metadata, recovered from byte buffers on disk
	shardFileMeta *ShardFileMeta

	// Shard meta
	shardMeta *ShardMeta

	// Shard index
	shardIndex *ShardIndex
}

// Buffer mode
type ShardBufferMode uint

const (
	IdleShardBufferMode  ShardBufferMode = iota // 0 = not doing anything
	ReadShardBufferMode                         // 1 = initial reading the shard from the bytes on disk into memory structures
	WriteShardBufferMode                        // 2 = writing into the shard
	FlushShardBufferMode                        // 3 = final process of writing the shard to bytes for the disk
	ReadyShardBufferMode                        // 4 = this is what an in-memory version looks like that is used for random access
)

// Read contents
// - locking should be done in the buffer itself
func (this *Shard) Contents() *bytes.Buffer {
	// @todo read from disk
	if this.contents == nil {
		this.contents = bytes.NewBuffer(make([]byte, conf.ShardSizeInBytes))
	}
	return this.contents
}

// Persist
func (this *Shard) Persist() {
	// @todo better writing to files, guaranteeing no data corruption
	log.Infof("Persisting shard %s to disk", this.IdStr())
	path := this.FullPath()
	err := ioutil.WriteFile(path, this.Contents().Bytes(), conf.UnixFilePermissions)
	if err != nil {
		// @tood handle better
		panic(err)
	}
}

// Full path
func (this *Shard) FullPath() string {
	var infix string = ""
	if this.Parity {
		infix = ".parity"
	}
	return fmt.Sprintf("%s/s_%s%s.data", this.Block().FullPath(), this.IdStr(), infix)
}

// Block
func (this *Shard) Block() *Block {
	return this.block
}

// To string
func (this *Shard) IdStr() string {
	return uuidToString(this.Id)
}

// Set contents
func (this *Shard) SetContents(b *bytes.Buffer) {
	this.contentsMux.Lock()
	this.contents = b
	this.contentsMux.Unlock()
}

func newShard(b *Block) *Shard {
	return &Shard{
		contents:      nil,
		Id:            randomUuid(),
		block:         b,
		Parity:        false,
		shardMeta:     newShardMeta(),
		shardIndex:    newShardIndex(),
		shardFileMeta: newShardFileMeta(),
	}
}
