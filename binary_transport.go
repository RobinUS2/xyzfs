package main

// Binary transport: inter-node transfer of data

var binaryTransport *BinaryTransport

type BinaryTransport struct {
	transport *NetworkTransport
}

func newBinaryTransport() *BinaryTransport {
	return &BinaryTransport{
		transport: newNetworkTransport("tcp", "binary", conf.BinaryPort),
	}
}
