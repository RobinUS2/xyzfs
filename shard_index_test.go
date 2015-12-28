package main

import (
	"fmt"
	"testing"
)

func TestShardIndex(t *testing.T) {
	id := randomUuid()
	idx := newShardIndex(id)

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

	// Test contains after to bytes
	if idx.Test("test") == false {
		t.Error("Should still be in here after to bytes")
	}

	// New empty filter
	idx2 := newShardIndex(randomUuid())
	if idx2.Test("test") == true {
		t.Error("Empty filter must be empty")
	}

	// Populate from bytes
	idx2.FromBytes(idxB)

	// Should now be in there
	if idx2.Test("test") == false {
		t.Error("Empty filter must be populated after loading bytes")
	}

	// Id recovered
	if uuidToString(idx2.ShardId) != uuidToString(idx.ShardId) {
		t.Errorf("Failed to load shard id from bytes %s %s", uuidToString(idx2.ShardId), uuidToString(idx.ShardId))
	}
}

var writeRes bool = false

func BenchmarkShardIndexWrite(b *testing.B) {
	var res bool
	idx := newShardIndex(randomUuid())
	for n := 0; n < b.N; n++ {
		res = idx.Add(fmt.Sprintf("%d", n))
	}
	writeRes = res
}

var readRes bool = false

func BenchmarkShardIndexRead(b *testing.B) {
	idx := newShardIndex(randomUuid())
	idx.Add("asdf")
	var res bool
	for n := 0; n < b.N; n++ {
		if n%3 == 0 {
			res = idx.Test("asdf")
		} else {
			res = idx.Test("not")
		}
	}
	readRes = res
}
