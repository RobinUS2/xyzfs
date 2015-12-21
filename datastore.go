package main

// Data store

var datastore *Datastore

type Datastore struct {
	fileLocator *FileLocator
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

// New block
func (this *Datastore) NewBlock() *Block {
	log.Info("Allocating new block")

	// @todo Improve volume allocator (e.g. random/least full)
	volume := conf.Datastore.Volumes[0]

	// Create new block
	b := newBlock(volume)

	// Init shards
	b.initShards()

	// Register block
	volume.RegisterBlock(b)

	// Register shards with local volume
	for _, s := range b.DataShards {
		volume.RegisterShard(s)
	}
	for _, s := range b.ParityShards {
		volume.RegisterShard(s)
	}

	// @todo replicate shard to other host
	// @todo store shard replication information on disk + in-memory
	// @todo replicate shard index to primary hosts via TCP
	// @todo replicate shard index to non-primary hosts via UDP

	return b
}

func newDatastore() *Datastore {
	d := &Datastore{
		fileLocator: newFileLocator(),
	}
	d.prepare()
	return d
}
