package main

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
