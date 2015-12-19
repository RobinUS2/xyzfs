package main

// Binary transport: inter-node transfer of data

// Global var
var binaryTransport *BinaryTransport

// Binary transport
type BinaryTransport struct {
	transport    *NetworkTransport
	udpTransport *NetworkTransport
}

// Create new binary transport
func newBinaryTransport() *BinaryTransport {
	b := &BinaryTransport{
		transport:    newNetworkTransport("tcp", "binary", conf.BinaryPort, conf.BinaryTransportReadBuffer),
		udpTransport: newNetworkTransport("udp", "binary_udp", conf.BinaryUdpPort, conf.BinaryTransportReadBuffer),
	}

	// Binary on connect
	b.transport._onConnect = func(cmeta *TransportConnectionMeta, node string) {
		// Send snapshot of shards
		b._sendShards(node)
	}

	// Binary on message
	b.transport._onMessage = func(cmeta *TransportConnectionMeta, b []byte) {
		// @todo implement
	}

	// Binary on UDP message
	b.udpTransport._onMessage = func(cmeta *TransportConnectionMeta, b []byte) {
		// @todo implement
	}

	// Start listening
	b.transport.start()
	b.udpTransport.start()

	return b
}
