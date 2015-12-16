package main

import (
	"testing"
)

func TestShardMeta(t *testing.T) {
	meta := newShardMeta()

	// Set some file count
	meta.mux.Lock()
	meta.FileCount += 123
	meta.mux.Unlock()

	// To bytes
	b := meta.Bytes()
	expectedMetaLength := 27 // do not use BINARY_METADATA_LENGTH as we need to use this test to verify it is still intact
	if len(b) != expectedMetaLength {
		t.Errorf("Meta should be %d bytes was %d", expectedMetaLength, len(b))
	}
	if uint32(expectedMetaLength) != BINARY_METADATA_LENGTH {
		t.Error("Meta data length does not match binary metadata length constnat")
	}

	// New meta
	meta2 := newShardMeta()

	// Load bytes
	meta2.FromBytes(b)

	// Validate count
	if meta2.FileCount != 123 {
		t.Error("Count should be 123 after loading bytes")
	}
}
