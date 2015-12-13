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

// Register shard
func (this *Volume) RegisterShard(s *Shard) {
	this.shardsMux.Lock()
	log.Infof("Registered shard %s with volume %s", s.IdStr(), this.IdStr())
	this.shards[s.IdStr()] = s
	this.shardsMux.Unlock()
}

// Register block
func (this *Volume) RegisterBlock(b *Block) {
	this.blocksMux.Lock()
	log.Infof("Registered block %s with volume %s", b.IdStr(), this.IdStr())
	this.blocks[b.IdStr()] = b
	this.blocksMux.Unlock()
}

// Prepare
func (this *Volume) prepare() {
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
	for _, elm := range list {
		split := strings.Split(elm.Name(), "=")
		// Must be in format b=UUID
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
	return fmt.Sprintf("%s/v=%s", this.Path, this.IdStr())
}

// New volume
func newVolume() *Volume {
	return &Volume{
		shards: make(map[string]*Shard),
		blocks: make(map[string]*Block),
	}
}
