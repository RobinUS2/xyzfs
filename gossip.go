package main

import (
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
		go this.transport._prepareConnection(seed)
	}
}

// On handshake complete
func (this *Gossip) _onHelloHandshakeComplete(node string) {
	log.Infof("Completed gossip handshake with %s", node)

	// Send node list
	this._sendNodeList(node)

	// Connect binary once handshake is complete (async)
	go binaryTransport.transport._prepareConnection(node)
}

// Send message
func (this *Gossip) _send(node string, msg *GossipMessage) error {
	return this.transport._send(node, msg.Bytes())
}

// Get nodes
func (this *Gossip) GetNodeStates() map[string]*GossipNodeState {
	this.nodesMux.RLock()
	defer this.nodesMux.RUnlock()
	return this.nodes
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

// New gossip service
func newGossip() *Gossip {
	// Create gossip
	g := &Gossip{
		transport: newNetworkTransport("tcp", "gossip", conf.GossipPort, conf.GossipTransportReadBuffer),
		nodes:     make(map[string]*GossipNodeState),
	}

	// Message
	g.transport._onMessage = func(cmeta *TransportConnectionMeta, b []byte) {
		msg := &GossipMessage{}
		msg.FromBytes(b)

		switch msg.Type {
		// Hello, ping-like message between nodes
		case HelloGossipMessageType:
			g._receiveHello(cmeta, msg)
			break

			// List of nodes, discovery service helper
		case NodeListGossipMessageType:
			g._receiveNodeList(cmeta, msg)
			break

			// Unknown
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
		// Reset any previous state
		nodeState := g.GetNodeState(node)
		if nodeState.GetLastHelloReceived() > 0 {
			// Re-transmit indices
			binaryTransport._sendShardIndices(node)
		}
		nodeState.Reset()

		// Send hello
		g._sendHello(node)
	}

	// Start
	g.transport.start()

	// Start discovery
	g.discoverSeeds()

	// Discovery from disk
	g.recoverGossipFromDisk()

	return g
}
