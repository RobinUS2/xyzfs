package main

// Responsible for figuring out which other nodes are out there

var gossip *Gossip

type Gossip struct {
}

func newGossip() *Gossip {
	return &Gossip{}
}
