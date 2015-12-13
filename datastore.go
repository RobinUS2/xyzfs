package main

// Data store

var datastore *Datastore

type Datastore struct {
}

// Prepare data store for interactions
func (this *Datastore) prepare() {
	// Load volumes
	for _, volume := range conf.Datastore.Volumes {
		volume.prepare()
	}
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

	return b
}

func newDatastore() *Datastore {
	d := &Datastore{}
	d.prepare()
	return d
}
