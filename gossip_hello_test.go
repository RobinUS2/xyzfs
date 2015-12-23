package main

import (
	"testing"
)

func TestGossipHello(t *testing.T) {
	// Send hello to localhost
	// the running gossip will accept this and process it
	gossip._sendHello("127.0.0.1")
}
