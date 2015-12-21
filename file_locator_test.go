package main

import (
	"testing"
)

func TestFileLocator(t *testing.T) {
	// Locator
	l := newFileLocator()

	// Filename
	fileName := "test.txt"
	fullNameNotExisting := "file_not_found.txt"

	// Create few indices
	var expectedIndices map[string]bool = make(map[string]bool, 0)
	for i := 0; i < 1000; i++ {
		// New shard
		id := randomUuid()
		idStr := uuidToString(id)
		idx := newShardIndex(id)

		// Add filename to every x-th index
		if i%200 == 0 {
			idx.Add(fileName)
			expectedIndices[idStr] = true
		}

		// Into index
		l.LoadIndex(idx.ShardId, idx)
	}

	// Locate file
	res, e := l._locate(nil, fileName)
	panicErr(e)
	if len(res) != 5 {
		log.Error("File should have been located in 5 shard indices")
	}

	// Validate indices
	for idStr, _ := range expectedIndices {
		var found bool = false
		for _, resIdx := range res {
			if uuidToString(resIdx.ShardId) == idStr {
				found = true
				break
			}
		}
		if !found {
			log.Error("Not able to find shard indice in map")
		}
	}

	// Locate non-existant file
	resNonExist, errNonExist := l._locate(nil, fullNameNotExisting)
	if resNonExist != nil || errNonExist == nil {
		log.Error("Found non-existing file")
	}
}
