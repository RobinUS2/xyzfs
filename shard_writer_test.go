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
}
