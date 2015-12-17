package main

import (
	"testing"
)

func TestNewFile(t *testing.T) {
	startApplication()

	// Register on volume
	b := datastore.NewBlock()

	// Get shard
	shard := b.DataShards[0]

	// == FIRST FILE ==
	// Fake file
	fileBytes := []byte("Hello File")
	fileMeta := newFileMeta("/images/robin/hello.txt")

	// Add file
	updatedFileMeta, err := shard.AddFile(fileMeta, fileBytes)
	// No errors
	if err != nil {
		t.Error("Failed to add file")
	}

	// Validate size
	if updatedFileMeta.Size != uint32(len(fileBytes)) {
		t.Error("Failed size validation")
	}

	// Validate offset (first file in test)
	if updatedFileMeta.StartOffset != 0 {
		t.Error("Start offset must be 0 for first file")
	}

	// == SECOND FILE ==
	// Fake another file
	fileBytes2 := []byte("Hello number two")
	fileMeta2 := newFileMeta("/images/robin/hello2.txt")

	// Add another file
	updatedFileMeta2, err2 := shard.AddFile(fileMeta2, fileBytes2)
	if err2 != nil {
		t.Error(err2)
	}

	// Offset of file 2 should be after file 1
	if updatedFileMeta2.StartOffset != uint32(len(fileBytes)) {
		t.Error("Should start after file 1")
	}

	// Validate shard meta
	if shard.FileCount() != 2 {
		t.Error("Shard should contain 2 files")
	}

	// Validate shard index
	if shard.shardIndex.Test("/non-existing") == true {
		t.Error("Shard index false positive")
	}
	if shard.shardIndex.Test(fileMeta.FullName) == false {
		t.Error("Shard index does not contain file 1")
	}
	if shard.shardIndex.Test(fileMeta2.FullName) == false {
		t.Error("Shard index does not contain file 2")
	}

	// Validate shard meta (de)serialization
	shardFileMetaBytes := shard.shardFileMeta.Bytes()
	if len(shardFileMetaBytes) < 1 {
		t.Error("Shard file meta bytes must be non-zero")
	}

	// Load these bytes into new to confirm it's still intact
	newShardFileMetaInstance := newShardFileMeta()
	if len(newShardFileMetaInstance.fileMeta) != 0 {
		t.Error("Empty shard file meta must be empty")
	}
	newShardFileMetaInstance.FromBytes(shardFileMetaBytes)
	if len(newShardFileMetaInstance.fileMeta) != 2 {
		t.Error("Loaded shard file meta must be 2")
	}

	// Persist shard to disk
	shard.Persist()

	// Reset pointers in this shard
	shard.contents = nil
	shard.shardIndex = nil
	shard.shardMeta = nil
	shard.shardFileMeta = nil

	// Read from disk
	shard.Load()

	// Validate (no need to check contents as those are not used directly)
	if shard.shardIndex == nil {
		t.Error("Failed to load shard index")
	}
	if shard.shardMeta == nil {
		t.Error("Failed to load shard meta")
	}
	if shard.shardFileMeta == nil {
		t.Error("Failed to load shard file meta")
	}

	// Functional validaton on the index
	if shard.shardIndex.Test(fileMeta.FullName) == false {
		t.Error("Shard index (after persist, nil, load) does not contain file 1")
	}

	// Function validation on shard meta
	if shard.FileCount() != 2 {
		t.Error("Shard (after persist, nil, load) should contain 2 files")
	}
}
