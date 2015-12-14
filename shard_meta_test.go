package main

import (
	"testing"
)

func TestShardMeta(t *testing.T) {
	meta := newShardMeta()

	// Set some file count
	meta.mux.Lock()
	meta.FileCount++
	meta.mux.Unlock()

	// To bytes
	b := meta.Bytes()
	if len(b) != 4 {
		t.Error("Meta should be 4 bytes")
	}

	// New meta
	meta2 := newShardMeta()

	// Load bytes
	meta2.FromBytes(b)

	// Validate count
	if meta2.FileCount != 1 {
		t.Error("Count should be 1 after loading bytes")
	}
}
