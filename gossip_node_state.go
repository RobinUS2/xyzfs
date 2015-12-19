package main

import (
	"sync"
)

type GossipNodeState struct {
	mux  sync.RWMutex
	Node string
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

func (this *GossipNodeState) GetLastHelloSent() uint32 {
	this.mux.RLock()
	defer this.mux.RUnlock()
	return this.LastHelloSent
}

func (this *GossipNodeState) GetLastHelloReceived() uint32 {
	this.mux.RLock()
	defer this.mux.RUnlock()
	return this.LastHelloReceived
}

func newGossipNodeState(node string) *GossipNodeState {
	return &GossipNodeState{
		Node: node,
	}
}
