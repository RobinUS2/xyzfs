package main

// Binary transport of shards to other nodes

// Send file
func (this *BinaryTransport) _sendFileChunk(node string, msg *BinaryTransportMessage) {
	this._send(node, msg)
}

// Receive file
func (this *BinaryTransport) _receiveFileChunk(cmeta *TransportConnectionMeta, msg *BinaryTransportMessage) {
	log.Infof("Received file chunk with len %d", len(msg.Data))
}
