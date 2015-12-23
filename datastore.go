package main

import (
	"errors"
)

// Data store

var datastore *Datastore

type Datastore struct {
	fileLocator *FileLocator
	nodeRouter  *NodeRouter
}

// Prepare data store for interactions
func (this *Datastore) prepare() {
	// Load volumes
	for _, volume := range this.Volumes() {
		volume.prepare()
	}
}

// Get volumes
func (this *Datastore) Volumes() []*Volume {
	return conf.Datastore.Volumes
}

// Locate file
func (this *Datastore) LocateFile(fullName string) ([]*ShardIndex, uint32, error) {
	return this.fileLocator._locate(this, fullName)
}

// Add file
func (this *Datastore) AddFile(fullName string, data []byte) (bool, error) {
	// Select node on where to execute this (it will be written there locally to a shard with space)
	node, nodeSelectionErr := this.nodeRouter.PickNode()
	if nodeSelectionErr != nil {
		return false, nodeSelectionErr
	}

	log.Infof("Routing AddFile() request to %s", node)

	// @todo Send data to that node over blocking RPC

	return false, errors.New("Not implemented")
}

// Find block
func (this *Datastore) BlockByIdStr(id string) *Block {
	for _, volume := range this.Volumes() {
		for _, block := range volume.Blocks() {
			if id == block.IdStr() {
				return block
			}
		}
	}
	return nil
}

// New block
func (this *Datastore) NewBlock() *Block {
	log.Info("Allocating new block")

	// @todo Improve volume allocator (e.g. random/least full)
	volume := conf.Datastore.Volumes[0]

	// Create new block
	b := newBlock(volume)

	// Init shards
	b.initShards()

	// Register block (takes care of shard registration internally)
	volume.RegisterBlock(b)

	// @todo replicate shard to other host
	// @todo store shard replication information on disk + in-memory
	// @todo replicate shard index to primary hosts via TCP
	// @todo replicate shard index to non-primary hosts via UDP

	return b
}

func newDatastore() *Datastore {
	d := &Datastore{
		fileLocator: newFileLocator(),
		nodeRouter:  newNodeRouter(),
	}
	d.prepare()
	return d
}
