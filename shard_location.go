package main

type ShardLocation struct {
	Node  string
	Local bool
}

func newShardLocation(node string, local bool) *ShardLocation {
	return &ShardLocation{
		Node:  node,
		Local: local,
	}
}
