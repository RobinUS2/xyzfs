package main

import (
	"sync"
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
func (this *Gossip) _receiveHello(msg *GossipMessage) {

}

// Send message
func (this *Gossip) _send(node string, msg *GossipMessage) error {
	return this.transport._send(node, msg.Bytes())
}

// Get node state
func (this *Gossip) GetNodeState(node string) *GossipNodeState {
	this.nodesMux.RLock()
	defer this.nodesMux.RUnlock()
	return this.nodes[node]
}

// New gossip service
func newGossip() *Gossip {
	// Create gossip
	g := &Gossip{
		transport: newNetworkTransport("tcp", "gossip", conf.GossipPort),
		nodes:     make(map[string]*GossipNodeState),
	}

	// Message
	g.transport._onMessage = func(b []byte) {
		msg := &GossipMessage{}
		msg.FromBytes(b)
		log.Infof("%s message %v", g.transport.serviceName, msg)

		switch msg.Type {
		case HelloGossipMessageType:
			g._receiveHello(msg)
			break
		}
	}

	// Connect
	g.transport._onConnect = func(node string) {
		// Send hello
		g._sendHello(node)
	}

	// Start
	g.transport.start()
	g.discoverSeeds()

	return g
}
