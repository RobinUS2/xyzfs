package main

import (
	"bytes"
	"sync"
)

// Shard is a partial piece of data in a block

type Shard struct {
	Id          []byte        // Unique ID
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

func newShard() *Shard {
	return &Shard{
		contents: nil,
		Id:       randomUuid(),
		Parity:   false,
	}
}
