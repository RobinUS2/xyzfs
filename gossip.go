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
func (this *Gossip) _sendHello(node string) error {
	msg := newGossipMessage(HelloGossipMessageType, nil)
	err := this._send(node, msg)

	// Update last sent
	if err == nil {
		// Handshake complete?
		if this.GetNodeState(node).GetLastHelloSent() == 0 {
			this._onHelloHandshakeComplete(node)
		}

		// We have sent hello
		this.GetNodeState(node).UpdateLastHelloSent()
	}
	return err
}

// Receive hello
func (this *Gossip) _receiveHello(cmeta *TransportConnectionMeta, msg *GossipMessage) {
	// State
	state := this.GetNodeState(cmeta.GetNode())

	// Handshake complete?
	if state.GetLastHelloReceived() == 0 {
		this._onHelloHandshakeComplete(cmeta.GetNode())
	}

	// We have received hello
	state.UpdateLastHelloReceived()

	// Should we send something back immediately?
	timeSinceLastHelloSent := unixTsUint32() - state.GetLastHelloSent()
	if timeSinceLastHelloSent > conf.GossipHelloInterval {
		this._sendHello(cmeta.GetNode())
	}
}

// On handshake complete
func (this *Gossip) _onHelloHandshakeComplete(node string) {
	this._sendNodeList(node)
}

// Receive list of nodes
func (this *Gossip) _receiveNodeList(cmeta *TransportConnectionMeta, msg *GossipMessage) {
	log.Debugf("Receiving nodes list %s", string(msg.Data))

	// Parse JSON
	var list []string
	jsonE := json.Unmarshal(msg.Data, &list)
	if jsonE != nil {
		log.Warnf("Failed to read nodes list: %s", jsonE)
		return
	}

	// New nodes?
	var newList []string = make([]string, 0)
	this.nodesMux.RLock()
	for _, elm := range list {
		if this.nodes[elm] == nil {
			newList = append(newList, elm)
		}
	}
	this.nodesMux.RUnlock()

	// Connect to new nodes
	if len(newList) > 0 {
		log.Infof("Discovered new nodes from gossip node list: %v", newList)
		for _, newElm := range newList {
			go this._sendHello(newElm)
		}
	}
}

// Send list of nodes
func (this *Gossip) _sendNodeList(node string) {
	msg := newGossipMessage(NodeListGossipMessageType, this.NodesToJSON())
	this._send(node, msg)
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
func (this *Gossip) NodesToJSON() []byte {
	// To JSON
	this.nodesMux.RLock()
	list := make([]string, 0)
	for node, _ := range this.nodes {
		list = append(list, node)
	}
	jsonBytes, jsonE := json.Marshal(list)
	this.nodesMux.RUnlock()
	panicErr(jsonE)
	return jsonBytes
}

// Persist list of nodes to disk for future
func (this *Gossip) PersistNodesToDisk() {
	// To JSON
	jsonBytes := this.NodesToJSON()

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
		// Send hello
		g._sendHello(node)
	}

	// Start
	g.transport.start()
	g.discoverSeeds()

	return g
}
