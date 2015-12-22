package main

type ShardLocation struct {
	Node string
}

func newShardLocation(node string) *ShardLocation {
	return &ShardLocation{
		Node: node,
	}
}
