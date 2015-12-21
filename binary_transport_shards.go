package main

// Binary transport of shards to other nodes

// Send shards
func (this *BinaryTransport) _sendShardIndices(node string) {
	log.Infof("Sending local shard indices to %s", node)
	for _, volume := range datastore.Volumes() {
		for _, shard := range volume.Shards() {
			this._sendShardIndex(shard, node)
		}
	}
}

// Send single shard
func (this *BinaryTransport) _sendShardIndex(shard *Shard, node string) {
	log.Infof("Sending local shard index %s (%s) to %s", shard.IdStr(), uuidToString(shard.shardIndex.ShardId), node)

	// To bytes
	b := shard.shardIndex.Bytes()

	// Msg
	msg := newBinaryTransportMessage(ShardIdxBinaryTransportMessageType, b)

	// Send
	this._send(node, msg)
}

// Receive single shard
func (this *BinaryTransport) _receiveShardIndex(cmeta *TransportConnectionMeta, msg *BinaryTransportMessage) {
	// Generate temporary shard to populate
	s := newShardIndex(randomUuid())

	// Load bytes
	s.FromBytes(msg.Data)

	// Load index
	datastore.fileLocator.LoadIndex(s.ShardId, s)
}
