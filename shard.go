package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"sync"
)

// Shard is a partial piece of data in a block

type Shard struct {
	Id          []byte // Unique UUID
	block       *Block
	contents    *bytes.Buffer // Actual byte buffer in-memory, is lazy loaded, use Contents() method to get
	contentsMux sync.RWMutex
	Parity      bool // Is this real data or parity?
}

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
	path := fmt.Sprintf("%s/s=%s.data", this.Block().FullPath(), this.IdStr())
	err := ioutil.WriteFile(path, this.Contents().Bytes(), conf.UnixFilePermissions)
	if err != nil {
		// @tood handle better
		panic(err)
	}
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
