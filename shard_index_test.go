package main

import (
	"testing"
)

func TestShardIndex(t *testing.T) {
	idx := newShardIndex()

	// Test empty
	if idx.Test("test") == true {
		t.Error("Should not be in here")
	}

	// Add
	idx.Add("test")

	// Test contains
	if idx.Test("test") == false {
		t.Error("Should be in here")
	}

	// Convert to bytes
	idxB := idx.Bytes()
	if len(idxB) < 1024 {
		t.Error("Should be of substantial size")
	}

	// New empty filter
	idx2 := newShardIndex()
	if idx2.Test("test") == true {
		t.Error("Empty filter must be empty")
	}

	// Populate from bytes
	idx2.LoadBytes(idxB)

	// Should now be in there
	if idx2.Test("test") == false {
		t.Error("Empty filter must be populated after loading bytes")
	}
}
