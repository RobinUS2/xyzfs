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

	// Byte buffers, file contents only
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
	// Make sure block folder is prepared
	this.Block().PrepareFolder()

	// @todo better writing to files, guaranteeing no data corruption
	path := this.FullPath()
	log.Infof("Persisting shard %s to disk in %s", this.IdStr(), path)
	err := ioutil.WriteFile(path, this._toBinaryFormat(), conf.UnixFilePermissions)
	if err != nil {
		// @tood handle better
		panic(err)
	}
}

// Load from disk
func (this *Shard) Load() {
	log.Infof("Loading shard %s from disk in %s", this.IdStr(), this.FullPath())
	this._fromBinaryFormat()
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

// Add file
func (this *Shard) AddFile(f *FileMeta, b []byte) (*FileMeta, error) {
	// Validate mode
	this.bufferModeMux.Lock()
	if this.bufferMode == ReadShardBufferMode {
		// Reading..
		panic("Shard is reading, can not write")
	}
	this.bufferMode = WriteShardBufferMode
	this.bufferModeMux.Unlock()

	// Calculate length
	fileLen := uint32(len(b))

	// Acquire write lock
	this.contentsMux.Lock()

	// Update metadata
	f.Size = fileLen
	f.StartOffset = this.contentsOffset

	// Write contents to buffer
	this.Contents().Write(b)

	// Update content offset
	this.contentsOffset += f.Size

	// Unlock write
	this.contentsMux.Unlock()

	// Append meta
	this.shardFileMeta.Add(f)

	// Update metadata
	this.shardMeta.mux.Lock()
	this.shardMeta.FileCount++
	newCount := this.shardMeta.FileCount
	this.shardMeta.mux.Unlock()

	// Update index
	this.shardIndex.Add(f.FullName)

	// Log
	log.Infof("Created file %s with size %d in shard %s, now contains %d file(s)", f.FullName, f.Size, this.IdStr(), newCount)

	// Done
	return f, nil
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
