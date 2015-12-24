package main

import (
	"bytes"
)

// Binary transport of shards to other nodes

// Send shards
func (this *BinaryTransport) _sendShardIndices(node string) {
	log.Infof("Sending local shard indices to %s", node)
	for _, volume := range datastore.Volumes() {
		for _, shard := range volume.Shards() {
			go this._sendShardIndex(shard, node)
		}
	}
}

// Broadcast single shard index
func (this *BinaryTransport) _broadcastShardIndex(shard *Shard) {
	// To all nodes
	for _, ns := range gossip.GetNodeStates() {
		go this._sendShardIndex(shard, ns.Node)
	}
}

// Send single shard
func (this *BinaryTransport) _sendShardIndex(shard *Shard, node string) {
	log.Infof("Sending local shard index %s (%s) to %s", shard.IdStr(), uuidToString(shard.shardIndex.ShardId), node)

	// To bytes
	b := shard.ShardIndex().Bytes()
	// log.Infof("Start idx serialized %s", shard.IdStr())

	// Msg
	msg := newBinaryTransportMessage(ShardIdxBinaryTransportMessageType, b)

	// Send
	// log.Infof("Start idx send %s", shard.IdStr())
	this._send(node, msg)
	// log.Infof("Shard idx %s sent", shard.IdStr())
}

// Receive single shard
func (this *BinaryTransport) _receiveShardIndex(cmeta *TransportConnectionMeta, msg *BinaryTransportMessage) {
	// Generate temporary shard to populate
	s := newShardIndex(randomUuid())

	// Load bytes
	s.FromBytes(msg.Data)

	// Load index
	datastore.fileLocator.LoadIndex(cmeta.GetNode(), s.ShardId, s)
}

// Send create shard
func (this *BinaryTransport) _sendCreateShard(node string, blockId []byte, shardId []byte) {
	// Build message
	buf := new(bytes.Buffer)
	buf.Write(blockId)
	buf.Write(shardId)

	// Send
	msg := newBinaryTransportMessage(CreateShardBinaryTransportMessageType, buf.Bytes())
	this._send(node, msg)
}

// Receive create shard
func (this *BinaryTransport) _receiveCreateShard(cmeta *TransportConnectionMeta, msg *BinaryTransportMessage) {
	// Read message
	buf := bytes.NewReader(msg.Data)
	blockId := make([]byte, 16)
	buf.Read(blockId)
	shardId := make([]byte, 16)
	buf.Read(shardId)

	// Get volume
	volume := datastore.GetVolume()

	// Block
	blockIdStr := uuidToString(blockId)
	var block *Block = datastore.BlockByIdStr(blockIdStr)
	if block == nil {
		// New block
		block = newBlockFromId(volume, blockId)

		// Register new block
		volume.RegisterBlock(block)
	}

	// Shard already existing?
	shardIdStr := uuidToString(shardId)
	for _, bs := range block.DataShards {
		if bs.IdStr() == shardIdStr {
			log.Infof("Shard already existing locally")
			return
		}
	}

	// Shard
	shard := newShardFromId(block, shardId)

	// Register shard
	block.RegisterDataShard(shard)

	// Persist shard
	shard.Persist()
}
