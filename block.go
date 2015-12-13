package main

// Block of data in a volume

type Block struct {
	DataShards   []*Shard
	ParityShards []*Shard
}

// Init shards
func (this *Block) initShards() {
	for i := 0; i < 10; i++ {
		this.DataShards[i] = newShard()
	}
	for i := 0; i < 3; i++ {
		this.ParityShards[i] = newShard()
	}
}

func newBlock() *Block {
	b := &Block{
		DataShards:   make([]*Shard, 10),
		ParityShards: make([]*Shard, 3),
	}
	b.initShards()
	return b
}
