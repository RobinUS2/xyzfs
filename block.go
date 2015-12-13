package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

// Block of data in a volume

type Block struct {
	Id           []byte // Unique ID
	volume       *Volume
	shardsMux    sync.RWMutex
	DataShards   []*Shard
	ParityShards []*Shard
}

// Init shards
func (this *Block) initShards() {
	for i := 0; i < conf.DataShardsPerBlock; i++ {
		this.DataShards[i] = newShard(this)
	}
	for i := 0; i < conf.ParityShardsPerBlock; i++ {
		this.ParityShards[i] = newShard(this)
		this.ParityShards[i].Parity = true
	}
}

// To string
func (this *Block) IdStr() string {
	return uuidToString(this.Id)
}

// Volume
func (this *Block) Volume() *Volume {
	return this.volume
}

// Persist block to disk
func (this *Block) Persist() {
	log.Infof("Persisting block %s to disk", this.IdStr())

	// Prepare folder
	if _, err := os.Stat(this.FullPath()); os.IsNotExist(err) {
		// Create
		log.Infof("Creating folder for block %s in %s", this.IdStr(), this.FullPath())
		e := os.MkdirAll(this.FullPath(), conf.UnixFolderPermissions)
		if e != nil {
			log.Errorf("Failed to create %s: %s", this.FullPath(), e)
		}
	}

	// Shards
	for _, shard := range this.DataShards {
		shard.Persist()
	}
	for _, shard := range this.ParityShards {
		shard.Persist()
	}
}

// Recover shards
func (this *Block) recoverShards() {
	list, e := ioutil.ReadDir(this.FullPath())
	if e != nil {
		log.Errorf("Failed to list shards in %s: %s", this.FullPath(), e)
		return
	}

	// Iterate
	for _, elm := range list {
		split := strings.Split(elm.Name(), "=")
		// Must be in format s=UUID
		if len(split) != 2 || split[0] != "s" {
			continue
		}

		// Shard
		nameSplit := strings.Split(split[1], ".")
		shard := newShard(this)
		shard.Id = uuidStringToBytes(nameSplit[0])

		// Add to list
		if strings.Contains(elm.Name(), ".parity") {
			// Parity
			shard.Parity = true
			this.RegisterParityShard(shard)
		} else {
			// Data
			this.RegisterDataShard(shard)
		}
	}
}

// Register shards
func (this *Block) RegisterDataShard(s *Shard) {
	if s.Parity == true {
		panic("Not a data shard")
	}
	this.shardsMux.Lock()
	log.Infof("Registered data shard %s with block %s", s.IdStr(), this.IdStr())
	this.DataShards = append(this.DataShards, s)
	this.shardsMux.Unlock()
}

// Register shards
func (this *Block) RegisterParityShard(s *Shard) {
	if s.Parity == false {
		panic("Not a parity shard")
	}
	this.shardsMux.Lock()
	log.Infof("Registered parity shard %s with block %s", s.IdStr(), this.IdStr())
	this.ParityShards = append(this.ParityShards, s)
	this.shardsMux.Unlock()
}

// Full path
func (this *Block) FullPath() string {
	return fmt.Sprintf("%s/b=%s", this.Volume().FullPath(), this.IdStr())
}

func newBlock(v *Volume) *Block {
	b := &Block{
		volume:       v,
		Id:           randomUuid(),
		DataShards:   make([]*Shard, conf.DataShardsPerBlock),
		ParityShards: make([]*Shard, conf.ParityShardsPerBlock),
	}
	b.initShards()
	return b
}
