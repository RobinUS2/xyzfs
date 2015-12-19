package main

// Binary transport of shards to other nodes

// Send shards
func (this *BinaryTransport) _sendShards(node string) {
	log.Infof("Sending local shards to %s", node)
	for _, volume := range datastore.Volumes() {
		for _, shard := range volume.Shards() {
			this._sendShard(shard, node)
		}
	}
}

// Send single shard
func (this *BinaryTransport) _sendShard(shard *Shard, node string) {
	log.Infof("Sending local shard %s to %s", shard.IdStr(), node)
	// @todo
}
