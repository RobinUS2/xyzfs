package main

import (
	"sync"
)

type GossipNodeState struct {
	mux sync.RWMutex
	// Pings between nodes
	LastHelloSent     uint32
	LastHelloReceived uint32
}

func (this *GossipNodeState) UpdateLastHelloSent() {
	this.mux.Lock()
	this.LastHelloSent = unixTsUint32()
	this.mux.Unlock()
}

func (this *GossipNodeState) UpdateLastHelloReceived() {
	this.mux.Lock()
	this.LastHelloReceived = unixTsUint32()
	this.mux.Unlock()
}

func newGossipNodeState() *GossipNodeState {
	return &GossipNodeState{}
}
