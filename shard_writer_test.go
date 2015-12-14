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

	// Fake file
	fileBytes := []byte("Hello File")
	fileMeta := newFileMeta("/images/robin/hello.txt")

	// Add file
	updatedFileMeta, err := shard.AddFile(fileMeta, fileBytes)
	// No errors
	if err != nil {
		panic("Failed to add file")
	}

	// Validate size
	if updatedFileMeta.Size != uint32(len(fileBytes)) {
		panic("Failed size validation")
	}
}
