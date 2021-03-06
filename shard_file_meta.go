package main

import (
	"encoding/json"
	"sync"
)

type ShardFileMeta struct {
	FileMeta []*FileMeta
	mux      sync.RWMutex
}

// Add file meta
func (this *ShardFileMeta) Add(f *FileMeta) {
	this.mux.Lock()
	this.FileMeta = append(this.FileMeta, f)
	this.mux.Unlock()
}

// To bytes
func (this *ShardFileMeta) Bytes() []byte {
	this.mux.RLock()
	b, be := json.Marshal(this.FileMeta)
	this.mux.RUnlock()
	if be != nil {
		panic("Failed to convert file meta to JSON")
	}
	return b
}

// Get by name
func (this *ShardFileMeta) GetByName(name string) *FileMeta {
	this.mux.RLock()
	for _, elm := range this.FileMeta {
		if elm.FullName == name {
			this.mux.RUnlock()
			return elm
		}
	}
	this.mux.RUnlock()
	return nil
}

// From bytes
func (this *ShardFileMeta) FromBytes(b []byte) {
	this.mux.Lock()
	json.Unmarshal(b, &this.FileMeta)
	this.mux.Unlock()
}

func newShardFileMeta() *ShardFileMeta {
	return &ShardFileMeta{
		FileMeta: make([]*FileMeta, 0),
	}
}
