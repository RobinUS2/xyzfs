package main

import (
	"errors"
	"fmt"
	"sync"
)

// The file locator helps to find files in the distributed datastore
type FileLocator struct {
	remoteShardIndicesMux sync.RWMutex
	remoteShardIndices    map[string]*ShardIndex
}

// Locate
func (this *FileLocator) _locate(datastore *Datastore, fullName string) ([]*ShardIndex, error) {
	// Result placeholder
	var res []*ShardIndex = make([]*ShardIndex, 0)
	var scanCount uint32 = 0

	// @todo scan all local bloom filters (local shards + distributed shards) (this should cover 99.9% of traffic under regular operations, includes nodes down)
	if datastore != nil {
		for _, volume := range datastore.Volumes() {
			for _, shard := range volume.Shards() {
				scanCount++
				if shard.shardIndex.Test(fullName) {
					// Result found
					res = append(res, shard.shardIndex)
				}
			}
		}
	} else {
		// Warning
		log.Warn("Executing file locate without datastore, this should only happen during testing")
	}

	// Scan registered bloom filters
	// @todo make concurrent
	this.remoteShardIndicesMux.RLock()
	for _, idx := range this.remoteShardIndices {
		// Is this file in here? Can give false positives
		scanCount++
		if idx.Test(fullName) {
			// Result found
			res = append(res, idx)
		}
	}
	this.remoteShardIndicesMux.RUnlock()

	// @todo IF NOT FOUND => convert name to murmur3 => determine primary bloomfilter index quorum, send RPC call, determine valid quorum (this step is done in order to prevent invalid data on a single node)

	// Not found?
	if len(res) == 0 {
		return nil, errors.New(fmt.Sprintf("File %s not found after scanning %d indices", fullName, scanCount))
	}

	// Found :)
	return res, nil
}

// Load index
func (this *FileLocator) LoadIndex(shardId []byte, idx *ShardIndex) {
	if len(shardId) != 16 {
		log.Errorf("Received invalid remote shard index %v", shardId)
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
