package main

// Responsible for figuring out which other nodes are out there

var gossip *Gossip

type Gossip struct {
	transport *NetworkTransport
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
	this._send(node, msg)
}

// Send message
func (this *Gossip) _send(node string, msg *GossipMessage) {
	this.transport._send(node, msg.Bytes())
}

// New gossip service
func newGossip() *Gossip {
	// Create gossip
	g := &Gossip{
		transport: newNetworkTransport("tcp", "gossip", conf.GossipPort),
	}

	// Message
	g.transport._onMessage = func(b []byte) {
		msg := &GossipMessage{}
		msg.FromBytes(b)
		log.Infof("%s message %v", g.transport.serviceName, msg)
	}

	// Connect
	g.transport._onConnect = func(node string) {
		g._sendHello(node)
	}

	// Start
	g.transport.start()

	return g
}
