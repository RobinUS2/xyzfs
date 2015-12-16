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
	expectedMetaLength := 15 // do not use BINARY_METADATA_LENGTH as we need to use this test to verify it is still intact
	if len(b) != expectedMetaLength {
		t.Errorf("Meta should be %d bytes was %d", expectedMetaLength, len(b))
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
