package main

import (
	"bytes"
	"encoding/binary"
)

// Message to send to other nodes
type GossipMessage struct {
	Version uint32            // Message format (e.g. 1)
	Type    GossipMessageType // Sort message
	Len     uint32            // Length of data
	Data    []byte            // Actual data
}

// This version
const GOSSIP_MESSAGE_VERSION uint32 = 1

// Message type
type GossipMessageType uint32

// Message types
const (
	EmptyGossipMessageType     GossipMessageType = iota // 0 = not set
	HelloGossipMessageType                              // 1 = initial hello message for handshake
	NodeStateGossipMessageType                          // 2 = node state changes
	NodeListGossipMessageType                           // 3 = node list (exchange list of servers)
)

// To bytes
func (this *GossipMessage) Bytes() []byte {
	if this.Type < 1 || this.Len < 1 {
		panic("Can not convert empty message to bytes")
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, this.Version) // meta version
	binary.Write(buf, binary.BigEndian, this.Type)    // type
	binary.Write(buf, binary.BigEndian, this.Len)     // data length
	if this.Data != nil {
		buf.Write(this.Data)
	}
	return buf.Bytes()
}

// From bytes
func (this *GossipMessage) FromBytes(b []byte) {
	buf := bytes.NewReader(b)
	var err error
	err = binary.Read(buf, binary.BigEndian, &this.Version) // meta version
	panicErr(err)
	err = binary.Read(buf, binary.BigEndian, &this.Type) // type
	panicErr(err)
	err = binary.Read(buf, binary.BigEndian, &this.Len) // length
	panicErr(err)

	// Read data
	this.Data = make([]byte, this.Len)
	buf.Read(this.Data)
}

// New message
func newGossipMessage(t GossipMessageType, data []byte) *GossipMessage {
	var l uint32 = 0
	if data != nil {
		l = uint32(len(data))
	}
	return &GossipMessage{
		Version: GOSSIP_MESSAGE_VERSION,
		Type:    t,
		Len:     l,
		Data:    data,
	}
}
