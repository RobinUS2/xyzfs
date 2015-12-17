package main

// Message to send to other nodes
type GossipMessage struct {
	Version           uint   // Message format (e.g. 1)
	GossipMessageType uint   // Sort message
	Len               uint   // Length of data
	Data              []byte // Actual data
}

// This version
const GOSSIP_MESSAGE_VERSION uint = 1

// Message type
type GossipMessageType uint

// Message types
const (
	EmptyGossipMessageType     GossipMessageType = iota // 0 = not set
	NodeStateGossipMessageType                          // 1 = node state changes
)
