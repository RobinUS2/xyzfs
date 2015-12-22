package main

import (
	"testing"
)

func TestNewShardLocation(t *testing.T) {
	l := newShardLocation("localhost", true)
	if l.Node != "localhost" {
		t.Error("Shard location not correct")
	}
	if l.Local != true {
		t.Error("Shard local must be true")
	}
}
