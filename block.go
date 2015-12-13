package main

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
		this.DataShards[i] = newShard()
	}
	for i := 0; i < conf.ParityShardsPerBlock; i++ {
		this.ParityShards[i] = newShard()
		this.ParityShards[i].Parity = true
	}
}

// To string
func (this *Block) IdStr() string {
	return uuidToString(this.Id)
}

// Persist block to disk
func (this *Block) Persist() {
	log.Infof("Persisting block %s to disk", this.IdStr())
	// @todo Implement
}

func newBlock() *Block {
	b := &Block{
		Id:           randomUuid(),
		DataShards:   make([]*Shard, conf.DataShardsPerBlock),
		ParityShards: make([]*Shard, conf.ParityShardsPerBlock),
	}
	b.initShards()
	return b
}
