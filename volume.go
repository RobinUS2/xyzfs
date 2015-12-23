package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

// Volume is a location on a server that points to a local data storage directory

type Volume struct {
	Id   []byte // Unique ID
	Path string // Path to local directory

	// Shards
	shards    map[string]*Shard
	shardsMux sync.RWMutex

	// Blocks
	blocks    map[string]*Block
	blocksMux sync.RWMutex
}

// To string
func (this *Volume) IdStr() string {
	return uuidToString(this.Id)
}

// Get blocks
func (this *Volume) Blocks() map[string]*Block {
	this.blocksMux.RLock()
	defer this.blocksMux.RUnlock()
	return this.blocks
}

// Get shards
func (this *Volume) Shards() map[string]*Shard {
	this.shardsMux.RLock()
	defer this.shardsMux.RUnlock()
	return this.shards
}

// Register shard
func (this *Volume) RegisterShard(s *Shard) {
	this.shardsMux.Lock()
	log.Infof("%d", len(this.shards))
	log.Infof("Registered shard %s with volume %s", s.IdStr(), this.IdStr())
	this.shards[s.IdStr()] = s
	this.shardsMux.Unlock()
}

// Register block
func (this *Volume) RegisterBlock(b *Block) {
	this.blocksMux.Lock()
	log.Infof("Registered block %s with volume %s", b.IdStr(), this.IdStr())
	this.blocks[b.IdStr()] = b

	// Register shards in this block
	for _, shard := range b.DataShards {
		this.RegisterShard(shard)
	}
	for _, shard := range b.ParityShards {
		this.RegisterShard(shard)
	}

	this.blocksMux.Unlock()
}

// Prepare
func (this *Volume) prepare() {
	// Already prepared?
	this.blocksMux.RLock()
	if len(this.blocks) > 0 {
		panic("Volume already prepared")
	}
	this.blocksMux.RUnlock()

	// Folder exists?
	log.Debugf("Preparing volume %s", this.IdStr())
	if _, err := os.Stat(this.FullPath()); os.IsNotExist(err) {
		// Create
		log.Infof("Creating folder for volume %s in %s", this.IdStr(), this.FullPath())
		e := os.MkdirAll(this.FullPath(), conf.UnixFolderPermissions)
		if e != nil {
			log.Errorf("Failed to create %s: %s", this.FullPath(), e)
		}
	}

	// Recover blocks
	list, e := ioutil.ReadDir(this.FullPath())
	if e != nil {
		log.Errorf("Failed to list blocks in %s: %s", this.FullPath(), e)
		return
	}

	// Iterate
	log.Infof("Found %d entries in volume directory", len(list))
	for _, elm := range list {
		split := strings.Split(elm.Name(), "_")
		// Must be in format b_UUID
		if len(split) != 2 || split[0] != "b" {
			continue
		}

		// Init
		b := newBlock(this)
		b.Id = uuidStringToBytes(split[1])

		// Recover shards
		b.recoverShards()

		// Register
		this.RegisterBlock(b)
	}

	// Done
	log.Infof("Loaded volume %s", this.IdStr())
}

// Full path
func (this *Volume) FullPath() string {
	return fmt.Sprintf("%s/v_%s", this.Path, this.IdStr())
}

// New volume
func newVolume() *Volume {
	v := &Volume{
		shards: make(map[string]*Shard),
		blocks: make(map[string]*Block),
	}
	return v
}
