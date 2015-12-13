package main

// Block of data in a volume

type Block struct {
	Id           []byte // Unique ID
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
		this.ParityShards[i].Parity = true
	}
}

func newBlock() *Block {
	b := &Block{
		Id:           randomUuid(),
		DataShards:   make([]*Shard, 10),
		ParityShards: make([]*Shard, 3),
	}
	b.initShards()
	return b
}
