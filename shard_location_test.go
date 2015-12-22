package main

import (
	"testing"
)

func TestNewShardLocation(t *testing.T) {
	l := newShardLocation("localhost")
	if l.Node != "localhost" {
		t.Error("Shard location not found")
	}
}
