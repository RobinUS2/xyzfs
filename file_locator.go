package main

import (
	"sync"
)

// The file locator helps to find files in the distributed datastore
type FileLocator struct {
	remoteShardIndicesMux sync.RWMutex
	remoteShardIndices    map[string]*ShardIndex
}

// Locate
func (this *FileLocator) _locate(ds *Datastore, fullName string) ([]*ShardIndex, error) {
	// @todo scan all local bloom filters (local shards + distributed shards) (this should cover 99.9% of traffic under regular operations, includes nodes down)
	// @todo IF NOT FOUND => convert name to murmur3 => determine primary bloomfilter index quorum, send RPC call, determine valid quorum (this step is done in order to prevent invalid data on a single node)

	// Scan registered bloom filters
	// @todo make concurrent
	var res []*ShardIndex = make([]*ShardIndex, 0)
	this.remoteShardIndicesMux.RLock()
	for _, idx := range this.remoteShardIndices {
		// Is this file in here? Can give false positives
		if idx.Test(fullName) {
			res = append(res, idx)
		}
	}
	this.remoteShardIndicesMux.RUnlock()

	return res, nil
}

// @todo implement populate the remote shard inidices
// Load index
func (this *FileLocator) LoadIndex(shardId []byte, idx *ShardIndex) {
	if len(shardId) != 16 {
		log.Errorf("Received shard index %v", shardId)
		return
	}
	k := uuidToString(shardId)
	log.Infof("Loading shard index %s into file locator", k)
	this.remoteShardIndicesMux.Lock()
	this.remoteShardIndices[k] = idx
	this.remoteShardIndicesMux.Unlock()
}

// New
func newFileLocator() *FileLocator {
	return &FileLocator{
		remoteShardIndices: make(map[string]*ShardIndex),
	}
}
