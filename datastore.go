package main

import (
	"errors"
)

// Data store

var datastore *Datastore

type Datastore struct {
	fileLocator  *FileLocator
	nodeRouter   *NodeRouter
	fileSplitter *BinaryTransportFileSplitter
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
	// Validate max file size
	if len(data) > conf.MaxFileSize {
		return false, errors.New("Exceeds maximum file size")
	}

	// Select node on where to execute this (it will be written there locally to a shard with space)
	node, nodeSelectionErr := this.nodeRouter.PickNode(nil)
	if nodeSelectionErr != nil {
		return false, nodeSelectionErr
	}
	log.Infof("Routing add file request to %s", node)

	// Create meta
	fileMeta := newFileMeta(fullName)
	fileMeta.UpdateFromData(data)

	// Split file into message chunks (no target shard)
	msgs := this.fileSplitter.Split(fileMeta, data, nil)

	// Send data to node
	for _, msg := range msgs {
		// log.Infof("send chunk %d %v", chunkNumber, msg.Data[12:])
		binaryTransport._sendFileChunk(node, msg)
	}

	// @todo Write to other replicate nodes
	// @todo Write shard index change (effectively add file to bloom filter) to all nodes (UDP)

	return true, nil
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

// Find shard
func (this *Datastore) LocalShardByIdStr(id string) *Shard {
	for _, volume := range this.Volumes() {
		for _, shard := range volume.Shards() {
			if shard.IdStr() == id {
				return shard
			}
		}
	}
	return nil
}

// Find writable shard
func (this *Datastore) AllocateShardCapacity(fileMeta *FileMeta) *Shard {
	for _, volume := range this.Volumes() {
		for _, shard := range volume.Shards() {
			// Allocate capacity
			if shard.AllocateCapacity(fileMeta.Size) {
				return shard
			}
		}
	}

	// No capacity found, let's create some
	this.NewBlock()
	return this.AllocateShardCapacity(fileMeta)
}

// Get volume
func (this *Datastore) GetVolume() *Volume {
	volumes := this.Volumes()
	// @todo Improve volume allocator (e.g. least full / fastest)
	volume := volumes[len(volumes)-1]
	return volume
}

// New block
func (this *Datastore) NewBlock() *Block {
	log.Info("Allocating new block")

	// Volume
	volume := this.GetVolume()

	// Create new block
	b := newBlock(volume)

	// Init shards
	b.initShards()

	// Register block (takes care of shard registration internally)
	volume.RegisterBlock(b)

	// Replicate shard to other host
	_, remoteErr := b.initRemoteShards()
	if remoteErr != nil {
		log.Errorf("Failed to initialize remote shards: %s", remoteErr)
	}

	return b
}

func newDatastore() *Datastore {
	d := &Datastore{
		fileLocator:  newFileLocator(),
		nodeRouter:   newNodeRouter(),
		fileSplitter: newBinaryTransportFileSplitter(uint32(conf.BinaryTransportWriteBuffer)),
	}
	d.prepare()
	return d
}
