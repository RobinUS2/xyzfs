package main

import (
	"fmt"
	"os"
)

// Block of data in a volume

type Block struct {
	Id           []byte // Unique ID
	volume       *Volume
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
