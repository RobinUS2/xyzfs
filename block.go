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
	Id        []byte // Unique ID
	volume    *Volume
	shardsMux sync.RWMutex

	// these must be an array to preserve the order
	DataShards   []*Shard
	ParityShards []*Shard
}

// Init local shards
func (this *Block) initShards() {
	for i := 0; i < conf.DataShardsPerBlock; i++ {
		shard := newShard(this)
		shard.BlockIndex = uint(i)
		this.RegisterDataShard(shard)
	}
	for i := 0; i < conf.ParityShardsPerBlock; i++ {
		shard := newShard(this)
		shard.Parity = true
		shard.BlockIndex = uint(i + conf.DataShardsPerBlock)
		this.RegisterParityShard(shard)
	}
}

// Init remote shards
func (this *Block) initRemoteShards() (bool, error) {
	// Router criteria
	criteria := newNodeRouterCriteria()
	criteria.ExcludeLocalNodes = true

	// For each data shard
	for _, dataShard := range this.DataShards {
		// Pick random remote node
		node, nodeSelectionErr := datastore.nodeRouter.PickNode(criteria)
		if nodeSelectionErr != nil {
			return false, nodeSelectionErr
		}
		log.Infof("Routing add remote shard (%s) request to %s", dataShard.IdStr(), node)
	}

	// Done
	return true, nil
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
func (this *Block) Persist() bool {
	log.Infof("Persisting block %s to disk", this.IdStr())

	// Prepare folder
	this.PrepareFolder()

	// Shards to disk
	for _, shard := range this.DataShards {
		shard.Persist()
	}
	for _, shard := range this.ParityShards {
		shard.Persist()
	}

	return true
}

// Prepare folder
func (this *Block) PrepareFolder() {
	// @todo cache only once
	// Prepare folder
	if _, err := os.Stat(this.FullPath()); os.IsNotExist(err) {
		// Create
		log.Infof("Creating folder for block %s in %s", this.IdStr(), this.FullPath())
		e := os.MkdirAll(this.FullPath(), conf.UnixFolderPermissions)
		if e != nil {
			log.Errorf("Failed to create %s: %s", this.FullPath(), e)
		}
	}
}

// Recover shards
func (this *Block) recoverShards() {
	// Read files
	list, e := ioutil.ReadDir(this.FullPath())
	if e != nil {
		log.Errorf("Failed to list shards in %s: %s", this.FullPath(), e)
		return
	}

	// Iterate
	log.Infof("Found %d entries in block directory", len(list))
	for _, elm := range list {
		split := strings.Split(elm.Name(), "_")
		// Must be in format s_UUID
		if len(split) != 2 || split[0] != "s" {
			log.Warnf("Ignoring invalid shard %s", elm)
			continue
		}

		// Shard
		nameSplit := strings.Split(split[1], ".")
		shard := newShardFromId(this, uuidStringToBytes(nameSplit[0]))

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
	if len(this.DataShards) > conf.DataShardsPerBlock {
		panic("Block full, can not register data shard")
	}
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
	if len(this.ParityShards) > conf.ParityShardsPerBlock {
		panic("Block full, can not register data shard")
	}
	log.Infof("Registered parity shard %s with block %s", s.IdStr(), this.IdStr())
	this.ParityShards = append(this.ParityShards, s)
	this.shardsMux.Unlock()
}

// Full path
func (this *Block) FullPath() string {
	return fmt.Sprintf("%s/b_%s", this.Volume().FullPath(), this.IdStr())
}

func newBlock(v *Volume) *Block {
	b := &Block{
		volume:       v,
		Id:           randomUuid(),
		DataShards:   make([]*Shard, 0),
		ParityShards: make([]*Shard, 0),
	}
	return b
}
