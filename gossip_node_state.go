package main

import (
	"sync"
)

type GossipNodeState struct {
	mux       sync.RWMutex
	Node      string
	RuntimeId string
	// Pings between nodes
	LastHelloSent     uint32
	LastHelloReceived uint32
	Stats             *PerformanceProfilerStats
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

func (this *GossipNodeState) SetRuntimeId(id string) {
	this.mux.Lock()
	this.RuntimeId = id
	this.mux.Unlock()
}

func (this *GossipNodeState) SetStats(s *PerformanceProfilerStats) {
	this.mux.Lock()
	this.Stats = s
	this.mux.Unlock()
}

func (this *GossipNodeState) GetStats() *PerformanceProfilerStats {
	this.mux.RLock()
	defer this.mux.RUnlock()
	return this.Stats
}

func (this *GossipNodeState) GetRuntimeId() string {
	this.mux.RLock()
	defer this.mux.RUnlock()
	return this.RuntimeId
}

func (this *GossipNodeState) Reset() {
	this.mux.Lock()
	this.LastHelloSent = 0
	this.LastHelloReceived = 0
	this.RuntimeId = ""
	this.mux.Unlock()
}

func newGossipNodeState(node string) *GossipNodeState {
	return &GossipNodeState{
		Node: node,
	}
}
