package main

// Binary transport: inter-node transfer of data

// Global var
var binaryTransport *BinaryTransport

// Binary transport
type BinaryTransport struct {
	transport *NetworkTransport
}

// Create new binary transport
func newBinaryTransport() *BinaryTransport {
	b := &BinaryTransport{
		transport: newNetworkTransport("tcp", "binary", conf.BinaryPort, conf.BinaryTransportReadBuffer),
	}

	// Listeners
	b.transport._onConnect = func(cmeta *TransportConnectionMeta, node string) {
		// Send snapshot of shards
		b._sendShards(node)
	}
	b.transport._onMessage = func(cmeta *TransportConnectionMeta, b []byte) {
		// @todo implement
	}

	// Start listening
	b.transport.start()

	return b
}
