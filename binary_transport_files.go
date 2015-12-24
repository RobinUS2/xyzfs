package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

// Binary transport of shards to other nodes

// Send file
func (this *BinaryTransport) _sendFileChunk(node string, msg *BinaryTransportMessage) {
	this._send(node, msg)
}

// Get file receiver
func (this *BinaryTransport) _getFileReceiver(cmeta *TransportConnectionMeta, transferId uint32) *BinaryTransportFileReceiver {
	// Get key
	k := this._fileReceiverKey(cmeta, transferId)

	// @todo Cleanup file receivers that have long not received files

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

// Cleanup receivers
func (this *BinaryTransport) _cleanupReceivers() {
	// List of stale items
	stale := make([]string, 0)

	// Scan
	this.fileReceiversMux.RLock()
	for k, receiver := range this.fileReceivers {
		// Too long ago?
		if time.Now().Sub(receiver.GetLastChunkReceived()).Seconds() > 60 {
			log.Warnf("Removing file %s receiver which didn't have data in timeout period", k)
			stale = append(stale, k)
		}
	}
	this.fileReceiversMux.RUnlock()

	// Found?
	if len(stale) < 1 {
		return
	}

	// Remove
	this.fileReceiversMux.Lock()
	for _, k := range stale {
		delete(this.fileReceivers, k)
	}
	this.fileReceiversMux.Unlock()
}

// File receiver key
func (this *BinaryTransport) _fileReceiverKey(cmeta *TransportConnectionMeta, transferId uint32) string {
	return fmt.Sprintf("%s~seq%d", cmeta.GetNode(), transferId)
}

// Cleanup file receiver
func (this *BinaryTransport) _removeFileReceiver(cmeta *TransportConnectionMeta, transferId uint32) {
	// Get key
	k := this._fileReceiverKey(cmeta, transferId)

	// Delete
	this.fileReceiversMux.Lock()
	delete(this.fileReceivers, k)
	this.fileReceiversMux.Unlock()

	// Every now and then cleanup
	if transferId%10 == 0 {
		go this._cleanupReceivers()
	}
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
		// To bytes
		b, be := receiver.Bytes()
		panicErr(be)

		// Validate file meta
		if receiver.fileMeta == nil {
			panic("Unexpected nil file meta")
		}

		// Store file in shard
		var targetShard *Shard = nil

		// Has target shard?
		if receiver.targetShardId != nil {
			// Has target shard
			targetShard = datastore.LocalShardByIdStr(uuidToString(receiver.targetShardId))
		} else {
			// No target shard
			targetShard = datastore.AllocateShardCapacity(receiver.fileMeta)
		}

		// Target shard must be non-nil
		if targetShard == nil {
			panic("Unexpected nil target shard")
		}

		// Write to shard
		writeResFileMeta, writeResErr := targetShard.AddFile(receiver.fileMeta, b)
		panicErr(writeResErr)

		// Persist shard
		targetShard.Persist()

		// We should replicate if there's no target shard specified
		if receiver.targetShardId == nil {
			// Figure out other targets
			targetShardLocations := datastore.fileLocator.ShardLocationsByIdStr(targetShard.IdStr())
			if len(targetShardLocations) > 1 {
				for _, targetShardLocation := range targetShardLocations {
					// Skip local shards
					if targetShardLocation.Local {
						continue
					}

					// Replicate
					log.Infof("Replicate file to shard location %v", targetShardLocation)

					// Split file
					msgs := datastore.fileSplitter.Split(writeResFileMeta, b, targetShard.Id)

					// Send chunks to remote node
					for _, msg := range msgs {
						this._sendFileChunk(targetShardLocation.Node, msg)
					}
				}
			}
		}

		// Log
		if writeResFileMeta != nil {
			log.Infof("Received file %s of length %d", writeResFileMeta.FullName, writeResFileMeta.Size)
		}

		// Cleanup file receivers
		this._removeFileReceiver(cmeta, transferNumber)
	}
}
