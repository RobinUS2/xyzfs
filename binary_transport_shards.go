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
	log.Infof("Sending local shard %s to %s", shard.IdStr(), node)

	// To bytes
	b := shard.shardIndex.Bytes()

	// Msg
	msg := newBinaryTransportMessage(ShardIdxBinaryTransportMessageType, b)

	// Send
	this._send(node, msg)
}
