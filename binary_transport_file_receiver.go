package main

import (
// "bytes"
// "encoding/binary"
)

// Receiver, should only be used for one file
type BinaryTransportFileReceiver struct {
}

// Receive chunk
func (this *BinaryTransportFileReceiver) AddMessage(msg *BinaryTransportMessage) {
	// @todo
}

// New receiver
func newBinaryTransportFileReceiver() *BinaryTransportFileReceiver {
	return &BinaryTransportFileReceiver{}
}
