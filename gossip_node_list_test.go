package main

import (
	"testing"
)

func TestGossipNodeList(t *testing.T) {
	// Send node list to localhost
	// the running gossip will accept this and process it
	gossip._sendNodeList("127.0.0.1")
}
