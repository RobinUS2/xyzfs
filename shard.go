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
	Id     []byte // Unique UUID
	Parity bool   // Is this real data or parity?

	// Block reference
	block *Block

	// Buffer mode
	bufferMode    ShardBufferMode
	bufferModeMux sync.RWMutex

	// Byte buffers
	// - ON READ: contents + metadata + bloom filter index
	// - ON WRITE (before flush): contains content
	contents       *bytes.Buffer // Actual byte buffer in-memory, is lazy loaded, use Contents() method to get
	contentsMux    sync.RWMutex
	contentsOffset uint32

	// Metadata, recovered from byte buffers on disk
	fileMeta    []*FileMeta
	fileMetaMux sync.RWMutex
}

// Buffer mode
type ShardBufferMode uint

const (
	IdleShardBufferMode  ShardBufferMode = iota // 0
	ReadShardBufferMode                         // 1
	WriteShardBufferMode                        // 2
)

// Read contents
func (this *Shard) Contents() *bytes.Buffer {
	// this.contentsMux.RLock()
	// @todo read from disk
	if this.contents == nil {
		this.contents = bytes.NewBuffer(make([]byte, conf.ShardSizeInBytes))
	}
	// defer this.contentsMux.RUnlock()
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
	return fmt.Sprintf("%s/s=%s%s.data", this.Block().FullPath(), this.IdStr(), infix)
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
		contents: nil,
		Id:       randomUuid(),
		block:    b,
		Parity:   false,
	}
}
