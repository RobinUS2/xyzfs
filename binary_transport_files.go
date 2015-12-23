package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Binary transport of shards to other nodes

// Send file
func (this *BinaryTransport) _sendFileChunk(node string, msg *BinaryTransportMessage) {
	this._send(node, msg)
}

// Get file receiver
func (this *BinaryTransport) _getFileReceiver(cmeta *TransportConnectionMeta, transferId uint32) *BinaryTransportFileReceiver {
	// Get key
	k := fmt.Sprintf("%s~seq%d", cmeta.GetNode(), transferId)

	// Existing?
	this.fileReceiversMux.RLock()
	if this.fileReceivers[k] != nil {
		defer this.fileReceiversMux.RUnlock()
		return this.fileReceivers[k]
	}
	this.fileReceiversMux.RUnlock()

	// Create
	this.fileReceiversMux.Lock()
	defer this.fileReceiversMux.Unlock()
	this.fileReceivers[k] = newBinaryTransportFileReceiver()
	return this.fileReceivers[k]
}

// Receive file
func (this *BinaryTransport) _receiveFileChunk(cmeta *TransportConnectionMeta, msg *BinaryTransportMessage) {
	// Validate this is what it should be
	if msg.Type != FileBinaryTransportMessageType {
		panic("Invalid message type")
	}

	// New byte reader
	buf := bytes.NewReader(msg.Data)
	var err error

	// Read transfer sequence number
	var transferNumber uint32
	err = binary.Read(buf, binary.BigEndian, &transferNumber)
	panicErr(err)
	if transferNumber < 1 {
		panic("Transfer number too low")
	}

	// Locate file receiver
	receiver := this._getFileReceiver(cmeta, transferNumber)

	// Add to receiver
	done := receiver.Add(buf)

	// Done?
	if done {
		b, be := receiver.Bytes()
		panicErr(be)
		// @todo Store file in shard
		log.Infof("Received file %s of length %d", receiver.fileMeta.FullName, len(b))
	}
}
