package main

// Binary transport: inter-node transfer of data

var binaryTransport *BinaryTransport

type BinaryTransport struct {
	transport *NetworkTransport
}

func newBinaryTransport() *BinaryTransport {
	b := &BinaryTransport{
		transport: newNetworkTransport("tcp", "binary", conf.BinaryPort),
	}

	// Listeners
	b.transport._onConnect = func(cmeta *TransportConnectionMeta, node string) {
		// @todo implement
	}
	b.transport._onMessage = func(cmeta *TransportConnectionMeta, b []byte) {
		// @todo implement
	}

	// Start listening
	b.transport.start()

	return b
}
