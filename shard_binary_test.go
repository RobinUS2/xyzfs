package main

import (
	"testing"
)

func TestShardWrite(t *testing.T) {
	// Shard to bytes
	s := newShard(nil)
	bTo := s._toBinaryFormat()
	if bTo == nil || len(bTo) < 1 {
		panic("Binary format too short")
	}
}
