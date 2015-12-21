package main

// Binary transport: inter-node transfer of data

// Global var
var binaryTransport *BinaryTransport

// Binary transport
type BinaryTransport struct {
	transport    *NetworkTransport
	udpTransport *NetworkTransport
}

// Send message
func (this *BinaryTransport) _send(node string, msg *BinaryTransportMessage) error {
	return this.transport._send(node, msg.Bytes())
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
		b._sendShardIndices(node)
	}

	// Binary on message
	b.transport._onMessage = func(cmeta *TransportConnectionMeta, by []byte) {
		msg := &BinaryTransportMessage{}
		msg.FromBytes(by)

		log.Infof("Received binary TCP message %d bytes", len(by))

		switch msg.Type {
		// Shard index
		case ShardIdxBinaryTransportMessageType:
			b._receiveShardIndex(cmeta, msg)
			break

			// Unknown
		default:
			log.Warnf("Received unknown binary TCP message %v", msg)
			break
		}
	}

	// Binary on UDP message
	b.udpTransport._onMessage = func(cmeta *TransportConnectionMeta, by []byte) {
		log.Infof("Received binary UDP message %d bytes", len(by))
		// @todo implement
	}

	// Start listening
	b.transport.start()
	b.udpTransport.start()

	return b
}
