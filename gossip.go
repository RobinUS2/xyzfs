package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"time"
)

// Responsible for figuring out which other nodes are out there

var gossip *Gossip

type Gossip struct {
	transport *NetworkTransport

	nodesMux sync.RWMutex
	nodes    map[string]*GossipNodeState
}

// Discover seeds
func (this *Gossip) discoverSeeds() {
	for _, seed := range conf.Seeds {
		go this.transport._connect(seed)
	}
}

// Send hello message to node
func (this *Gossip) _sendHello(node string) {
	msg := newGossipMessage(HelloGossipMessageType, nil)
	err := this._send(node, msg)

	// Update last sent
	if err == nil {
		this.GetNodeState(node).UpdateLastHelloSent()
	}
}

// Receive hello
func (this *Gossip) _receiveHello(cmeta *TransportConnectionMeta, msg *GossipMessage) {
	state := this.GetNodeState(cmeta.GetNode())
	timeSinceLastHelloSent := unixTsUint32() - state.GetLastHelloSent()
	if timeSinceLastHelloSent > conf.GossipHelloInterval {
		this._sendHello(cmeta.GetNode())
	}
}

// Send message
func (this *Gossip) _send(node string, msg *GossipMessage) error {
	return this.transport._send(node, msg.Bytes())
}

// Get node state
func (this *Gossip) GetNodeState(node string) *GossipNodeState {
	var s *GossipNodeState
	this.nodesMux.RLock()
	s = this.nodes[node]
	this.nodesMux.RUnlock()

	// Existing node?
	if s == nil {
		// New node
		this.nodesMux.Lock()
		this.nodes[node] = newGossipNodeState(node)
		s = this.nodes[node]
		this.nodesMux.Unlock()

		// Keep list on local disk
		this.PersistNodesToDisk()
	}
	return s
}

// Persist list of nodes to disk for future
func (this *Gossip) PersistNodesToDisk() {
	// To JSON
	this.nodesMux.RLock()
	jsonBytes, jsonE := json.Marshal(this.nodes)
	this.nodesMux.RUnlock()
	panicErr(jsonE)

	// Write to disk
	err := ioutil.WriteFile(fmt.Sprintf("%s/gossip_nodes.json", conf.MetaBasePath), jsonBytes, 0644)
	if err != nil {
		log.Errorf("Failed to persist gossip nodes list to disk: %s", err)
	}
}

// New gossip service
func newGossip() *Gossip {
	// Create gossip
	g := &Gossip{
		transport: newNetworkTransport("tcp", "gossip", conf.GossipPort),
		nodes:     make(map[string]*GossipNodeState),
	}

	// Message
	g.transport._onMessage = func(cmeta *TransportConnectionMeta, b []byte) {
		msg := &GossipMessage{}
		msg.FromBytes(b)

		switch msg.Type {
		case HelloGossipMessageType:
			g._receiveHello(cmeta, msg)
			break
		default:
			log.Warnf("Received unknown message %v", msg)
			break
		}
	}

	// Ticker
	ticker := time.NewTicker(time.Second * 1)
	go func() {
		for _ = range ticker.C {
			// Send new hellos?
			g.nodesMux.RLock()
			for node, _ := range g.nodes {
				timeSinceLastHelloSent := unixTsUint32() - g.GetNodeState(node).GetLastHelloSent()
				if timeSinceLastHelloSent > conf.GossipHelloInterval {
					go g._sendHello(node)
				}
			}
			g.nodesMux.RUnlock()
		}
	}()

	// Connect
	g.transport._onConnect = func(cmeta *TransportConnectionMeta, node string) {
		// Send hello
		g._sendHello(node)
	}

	// Start
	g.transport.start()
	g.discoverSeeds()

	return g
}
