package main

import (
	"errors"
	"fmt"
	"sync"
)

// The file locator helps to find files in the distributed datastore
type FileLocator struct {
	// Shard to node map
	shardLocationsMux sync.RWMutex
	shardLocations    map[string][]*ShardLocation

	// Shard indices
	remoteShardIndicesMux sync.RWMutex
	remoteShardIndices    map[string]*ShardIndex
}

// Locate
// @todo Support first-match returns mode, for faster reads
func (this *FileLocator) _locate(datastore *Datastore, fullName string) ([]*ShardIndex, uint32, error) {
	// Result placeholder
	var res []*ShardIndex = make([]*ShardIndex, 0)
	var scanCount uint32 = 0

	// @todo scan all local bloom filters (local shards + distributed shards) (this should cover 99.9% of traffic under regular operations, includes nodes down)
	if datastore != nil {
		for _, volume := range datastore.Volumes() {
			for _, shard := range volume.Shards() {
				scanCount++
				if shard.TestContainsFile(fullName) {
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
		return nil, scanCount, errors.New(fmt.Sprintf("File %s not found after scanning %d indices", fullName, scanCount))
	}

	// Found :)
	return res, scanCount, nil
}

// Load index
func (this *FileLocator) LoadIndex(node string, shardId []byte, idx *ShardIndex) {
	if len(shardId) != 16 {
		log.Errorf("Received invalid remote shard index %v from %s", shardId, node)
		return
	}
	k := uuidToString(shardId)
	log.Infof("Loading shard index %s from %s into file locator", k, node)

	// Load shard index
	this.remoteShardIndicesMux.Lock()
	this.remoteShardIndices[k] = idx
	this.remoteShardIndicesMux.Unlock()

	// Register mapping (non-local)
	this._addShardNodeMapping(shardId, node, false)
}

// Add shard=>node mapping
func (this *FileLocator) _addShardNodeMapping(shardId []byte, node string, localShard bool) {
	k := uuidToString(shardId)

	// Add to map
	this.shardLocationsMux.Lock()

	// Prepare key with array
	if this.shardLocations[k] == nil {
		this.shardLocations[k] = make([]*ShardLocation, 0)
	}

	// Already in there?
	var alreadyExisting bool = false
	for _, l := range this.shardLocations[k] {
		if l.Node == node {
			alreadyExisting = true
			break
		}
	}

	// Found?
	if alreadyExisting == false {
		this.shardLocations[k] = append(this.shardLocations[k], newShardLocation(node, localShard))
	}

	// Unlock
	this.shardLocationsMux.Unlock()
}

// Get shard locations
func (this *FileLocator) ShardLocations() map[string][]*ShardLocation {

	// Local shards
	for _, volume := range datastore.Volumes() {
		for _, shard := range volume.Shards() {
			// Register local mapping
			this._addShardNodeMapping(shard.Id, "localhost", true)
		}
	}

	this.shardLocationsMux.RLock()
	defer this.shardLocationsMux.RUnlock()
	return this.shardLocations
}

// New
func newFileLocator() *FileLocator {
	return &FileLocator{
		remoteShardIndices: make(map[string]*ShardIndex),
		shardLocations:     make(map[string][]*ShardLocation),
	}
}
