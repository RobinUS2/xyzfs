package main

// Send hello message to node
func (this *Gossip) _sendHello(node string) error {
	// log.Infof("Gossip to %s", node)

	// Create message with the runtime ID
	msg := newGossipMessage(HelloGossipMessageType, []byte(runtime.Id))

	// Send
	_, err := this._send(node, msg)

	// Update last sent
	if err == nil {
		// We have sent hello
		this.GetNodeState(node).UpdateLastHelloSent()
	} else {
		log.Warnf("Failed gossip to %s: %s", node, err)
	}
	return err
}

// Receive hello
func (this *Gossip) _receiveHello(cmeta *TransportConnectionMeta, msg *GossipMessage) {
	// log.Infof("Gossip from %s", cmeta.GetNode())

	// Hello runtime ID
	remoteRuntimeId := string(msg.Data)

	// State
	state := this.GetNodeState(cmeta.GetNode())

	// Runtime
	state.SetRuntimeId(remoteRuntimeId)

	// Ignore messages from ourselves
	// if remoteRuntimeId == runtime.Id {
	// 	runtime.SetNode(cmeta.GetNode())
	// 	log.Debugf("Ignoring gossip hello from ourselves with runtime ID %s", runtime.Id)
	// 	return
	// }

	// Handshake complete?
	if state.GetLastHelloReceived() == 0 {
		go this._onHelloHandshakeComplete(cmeta.GetNode())
	}

	// We have received hello
	state.UpdateLastHelloReceived()

	// Should we send something back immediately?
	timeSinceLastHelloSent := unixTsUint32() - state.GetLastHelloSent()
	if timeSinceLastHelloSent > conf.GossipHelloInterval {
		go this._sendHello(cmeta.GetNode())
	}
}
