package main

import (
	"sync"
)

// Binary transport: inter-node transfer of data

// Global var
var binaryTransport *BinaryTransport

// Binary transport
type BinaryTransport struct {
	// Transport layers
	transport    *NetworkTransport
	udpTransport *NetworkTransport

	// File receivers
	fileReceiversMux sync.RWMutex
	fileReceivers    map[string]*BinaryTransportFileReceiver
}

// Send message
func (this *BinaryTransport) _send(node string, msg *BinaryTransportMessage) ([]byte, error) {
	return this.transport._send(node, msg.Bytes())
}

// Create new binary transport
func newBinaryTransport() *BinaryTransport {
	b := &BinaryTransport{
		transport:     newNetworkTransport("tcp", "binary", conf.BinaryPort, conf.BinaryTransportReadBuffer, conf.BinaryTransportNumStreams, false),
		udpTransport:  newNetworkTransport("udp", "binary_udp", conf.BinaryUdpPort, conf.BinaryTransportReadBuffer, conf.BinaryTransportNumStreams, false),
		fileReceivers: make(map[string]*BinaryTransportFileReceiver),
	}

	// Binary on connect
	b.transport._onConnect = func(cmeta *TransportConnectionMeta, node string) {
		// Send snapshot of shards
		b._sendShardIndices(node)
	}

	// Binary on message
	b.transport._onMessage = func(cmeta *TransportConnectionMeta, by []byte) []byte {
		msg := &BinaryTransportMessage{}
		msg.FromBytes(by)

		log.Debugf("Received binary TCP message %d bytes", len(by))

		switch msg.Type {
		// Shard index
		case ShardIdxBinaryTransportMessageType:
			b._receiveShardIndex(cmeta, msg)
			break

		// File chunk
		case FileBinaryTransportMessageType:
			b._receiveFileChunk(cmeta, msg)
			break

			// Create shard
		case CreateShardBinaryTransportMessageType:
			b._receiveCreateShard(cmeta, msg)
			break

			// Unknown
		default:
			log.Warnf("Received unknown binary TCP message %v", msg)
			break
		}

		// No response
		return nil
	}

	// Binary on UDP message
	b.udpTransport._onMessage = func(cmeta *TransportConnectionMeta, by []byte) []byte {
		log.Infof("Received binary UDP message %d bytes", len(by))
		// @todo implement

		// No response
		return nil
	}

	// Start listening
	b.transport.start()
	b.udpTransport.start()

	return b
}
