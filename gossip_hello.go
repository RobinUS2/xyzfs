package main

// Send hello message to node
func (this *Gossip) _sendHello(node string) error {
	// Create message with the runtime ID
	msg := newGossipMessage(HelloGossipMessageType, []byte(runtime.Id))

	// Send
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
	// Ignore messages from ourselves
	if string(msg.Data) == runtime.Id {
		log.Debugf("Ignoring gossip hello from ourselves with runtime ID %s", runtime.Id)
		return
	}

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
