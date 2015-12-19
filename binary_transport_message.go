package main

import (
	"bytes"
	"encoding/binary"
)

// Message to send to other nodes
type BinaryTransportMessage struct {
	Version uint32                     // Message format (e.g. 1)
	Type    BinaryTransportMessageType // Sort message
	Len     uint32                     // Length of data
	Data    []byte                     // Actual data
}

// This version
const BINARY_TRANSPORT_MESSAGE_VERSION uint32 = 1

// Message type
type BinaryTransportMessageType uint32

// Message types
const (
	EmptyBinaryTransportMessageType    BinaryTransportMessageType = iota // 0 = not set
	ShardIdxBinaryTransportMessageType                                   // 1 = shard index
)

// To bytes
func (this *BinaryTransportMessage) Bytes() []byte {
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
func (this *BinaryTransportMessage) FromBytes(b []byte) {
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
func newBinaryTransportMessage(t BinaryTransportMessageType, data []byte) *BinaryTransportMessage {
	var l uint32 = 0
	if data != nil {
		l = uint32(len(data))
	}
	return &BinaryTransportMessage{
		Version: BINARY_TRANSPORT_MESSAGE_VERSION,
		Type:    t,
		Len:     l,
		Data:    data,
	}
}
