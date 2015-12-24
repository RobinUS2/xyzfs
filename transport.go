package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	// "errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"strings"
	"sync"
	// "time"
)

var TRANSPORT_MAGIC_FOOTER []byte = []byte{'\r', '\n', 'X', 'Y', 'Z'}

// Network transport layer

type NetworkTransport struct {
	// Generic mutex
	mux sync.RWMutex
	// Config
	protocol         string
	port             int
	serviceName      string
	listenAddr       string // Local listen address
	receiveBufferLen int
	// Handlers
	_onMessage func(*TransportConnectionMeta, []byte)
	_onConnect func(*TransportConnectionMeta, string)
	// Connections
	connectionPoolSize int
	connections        map[string]*TransportConnectionPool
	connectionsMux     sync.RWMutex
	// Is connecting
	isConnectingMux sync.RWMutex
	isConnecting    map[string]bool
	// Compression
	_compress   func([]byte) ([]byte, error)
	_decompress func([]byte) ([]byte, error)
}

// Listen
func (this *NetworkTransport) listen() {
	log.Infof("Starting %s on port %s/%d", this.serviceName, strings.ToUpper(this.protocol), this.port)
	switch this.protocol {
	case "tcp":
		this._listenTcp()
		break
	case "udp":
		this._listenUdp()
		break
	default:
		panic(fmt.Sprintf("Protocol %s not supported", this.protocol))
	}
}

// Listen TCP
func (this *NetworkTransport) _listenTcp() {
	ln, err := net.Listen(this.protocol, fmt.Sprintf(":%d", this.port))
	this.mux.Lock()
	this.listenAddr = ln.Addr().String()
	this.mux.Unlock()
	if err != nil {
		panicErr(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
		}
		go this.handleConnection(conn)
	}
}

// Listen TCP
func (this *NetworkTransport) _listenUdp() {
	// Prepare address
	serverAddr, err := net.ResolveUDPAddr(this.protocol, fmt.Sprintf(":%d", this.port))
	panicErr(err)

	ln, err := net.ListenUDP(this.protocol, serverAddr)
	this.mux.Lock()
	this.listenAddr = ln.LocalAddr().String()
	this.mux.Unlock()
	if err != nil {
		panicErr(err)
	}
	for {
		tbuf := make([]byte, this.receiveBufferLen)
		n, addr, err := ln.ReadFromUDP(tbuf)
		log.Debugf("Received ", string(tbuf[0:n]), " from ", addr)

		if err != nil {
			log.Errorf("UDP receive error: %s", err)
		} else {
			// Read message
			this._onMessage(newTransportConnectionMeta(ln.RemoteAddr().String()), tbuf[0:n])
		}
	}
}

// Validate magic footer, returns true if all have passed
func (this *NetworkTransport) _validateMagicFooter(connbuf *bufio.Reader, magicStart int, magicEnd int) bool {
	for i := magicStart; i <= magicEnd; i++ {
		// Read byte
		byTerminator, _ := connbuf.ReadByte()

		// Is this the correct footer byte?
		if byTerminator != TRANSPORT_MAGIC_FOOTER[i] {
			// Put data back in bufffer
			connbuf.UnreadByte()

			// Not valid
			return false
		}

		// Last?
		if i == magicEnd {
			return true
		}
	}
	// Should never come here
	panic("Magic bytes invalid")
}

// Handle connection
func (this *NetworkTransport) handleConnection(conn net.Conn) {
	// Read bytes
	// tbuf := make([]byte, this.receiveBufferLen)
	tbuf := new(bytes.Buffer)

	// Connection nil?
	if conn == nil {
		log.Error("Nil connection received into handleConnection")
		return
	}

	// Reader
	connbuf := bufio.NewReader(conn)

	for {
		// Reader until first \r
		by, err := connbuf.ReadBytes(TRANSPORT_MAGIC_FOOTER[0])
		log.Infof("Received %d %s bytes for %s from %s", len(by), this.protocol, this.serviceName, conn.RemoteAddr().String())
		// Was there an error in reading ?
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %s", err)
			}
			break
		}

		// Write to buffer
		tbuf.Write(by)
		log.Infof("tbuf now %d", len(tbuf.Bytes()))

		// Is this the end?
		if this._validateMagicFooter(connbuf, 1, 4) == false {
			log.Infof("Magic footer not yet found")
			// Not yet, continue reading
			continue
		}

		// Decompress
		db, de := this._decompress(tbuf.Bytes())
		if de != nil {
			log.Errorf("Decompress error: %s", de)
			panic("Failed to decompress")
		}

		// Read message
		this._onMessage(newTransportConnectionMeta(conn.RemoteAddr().String()), db)

		// Ack with checksum of decompressed received bytes
		// this is after the message processing, to make it synchronous
		// the client can become async to do something like "go transport._senc()"
		// the receiver can become async by implementing the read message with a go routine
		ackBuf := new(bytes.Buffer)
		receivedCrc := crc32.Checksum(db, crcTable)
		binary.Write(ackBuf, binary.BigEndian, receivedCrc)
		conn.Write(ackBuf.Bytes())

		// New buffer
		tbuf = new(bytes.Buffer)
	}
}

// Prepare connection
func (this *NetworkTransport) _prepareConnection(node string) {
	// Get channel
	this.connectionsMux.Lock()
	if this.connections[node] == nil {
		this.connections[node] = newTransportConnectionPool(this, node, 4)

		// Send HELLO message to new nodes
		if this._onConnect != nil {
			// Begin gossip
			go func() {
				tc := this.connections[node].GetConnection()
				conn := tc.Conn()
				this._onConnect(newTransportConnectionMeta(conn.RemoteAddr().String()), node)
				this.connections[node].ReturnConnection(tc)
			}()
		}
	}
	this.connectionsMux.Unlock()
}

// Get connection
func (this *NetworkTransport) _getConnection(node string) *TransportConnection {
	// Make sure prepared
	this._prepareConnection(node)

	// Get
	this.connectionsMux.RLock()
	defer this.connectionsMux.RUnlock()
	return this.connections[node].GetConnection()
}

// Return connection
func (this *NetworkTransport) _returnConnection(node string, tc *TransportConnection) {
	this.connectionsMux.RLock()
	defer this.connectionsMux.RUnlock()
	this.connections[node].ReturnConnection(tc)
}

// Discard connection
func (this *NetworkTransport) _discardConnection(node string, tc *TransportConnection) {
	this.connectionsMux.RLock()
	defer this.connectionsMux.RUnlock()
	this.connections[node].DiscardConnection(tc)
}

// Send message
func (this *NetworkTransport) _send(node string, b []byte) error {
	// Retries
	var errb error
	for i := 0; i < 5; i++ {
		// Get connection
		tc := this._getConnection(node)

		// Compress
		bc, ce := this._compress(b)
		if ce != nil {
			panic("Failed to compress")
		}

		// CRC
		sendCrc := crc32.Checksum(b, crcTable)

		// Get socket
		conn := tc.Conn()

		// Before send log
		log.Infof("Sending %d (crc=%d, compressed %d) %s bytes for %s to %s over %s", len(b), sendCrc, len(bc), this.protocol, this.serviceName, node, tc.id)

		// Write data + footer
		_, errb = conn.Write(bc)
		_, errf := conn.Write(TRANSPORT_MAGIC_FOOTER)

		// OK?
		if errb != nil || errf != nil {
			// Uups..
			this._discardConnection(node, tc)

			// @todo sleep with jitter

			// Try again
			continue
		}

		// After send log
		log.Infof("Sent %d (crc=%d, compressed %d) %s bytes for %s to %s over %s", len(b), sendCrc, len(bc), this.protocol, this.serviceName, node, tc.id)

		// Read ACK on this socket
		// @Todo timeout ACK wait
		log.Infof("Waiting for ack %s", this.serviceName)
		ackBytes := make([]byte, 4)
		conn.Read(ackBytes)
		ackBuf := bytes.NewReader(ackBytes)
		var receivedCrc uint32
		ackReadErr := binary.Read(ackBuf, binary.BigEndian, &receivedCrc)
		panicErr(ackReadErr)

		// Validate CRC
		if receivedCrc != sendCrc {
			log.Warnf("Acked transport bytes crc %d send crc %d", receivedCrc, sendCrc)

			// Discard and retry
			this._discardConnection(node, tc)
			continue
		} else {
			log.Infof("Ack passed for %d", sendCrc)
		}

		// Return connection
		this._returnConnection(node, tc)

		// OK :)
		return nil
	}

	return errb

	// OLD CODE BELOW

	// // Get connection
	// log.Infof("_send %s", this.serviceName)
	// var connection *TransportConnection = this._getConnection(node)
	// if connection == nil {
	// 	return errors.New("Unable to get conection")
	// }

	// // CRC
	// sendCrc := crc32.Checksum(b, crcTable)

	// // Compress
	// bc, ce := this._compress(b)
	// if ce != nil {
	// 	panic("Failed to compress")
	// }

	// log.Infof("Sending %d %s bytes for %s to %s over %s", len(b), this.protocol, this.serviceName, node, connection.id)

	// // Simple retry handler
	// var err error

	// // Get socket
	// var conn net.Conn = *connection.conn

	// // Write data
	// // log.Infof("Writing %s", this.serviceName)
	// _, err = conn.Write(bc)

	// // Write magic footer
	// conn.Write(TRANSPORT_MAGIC_FOOTER)

	// // Validate
	// if err != nil {
	// 	log.Warnf("Failed to write %d %s bytes to %s: %s", len(b), this.serviceName, node, err)

	// 	// Close socket
	// 	connection.Close()

	// 	// Create new
	// 	go this._connect(node)

	// 	// Do not return connection, it's broken

	// 	// Attempt two
	// 	return err
	// } else {
	// 	log.Debugf("Written %d (raw %d) %s bytes to %s", len(bc), len(b), this.serviceName, node)
	// }

	// // Read ACK on this socket
	// // @Todo timeout ACK wait
	// // log.Infof("Waiting for ack %s", this.serviceName)
	// ackBytes := make([]byte, 4)
	// conn.Read(ackBytes)
	// ackBuf := bytes.NewReader(ackBytes)
	// var receivedCrc uint32
	// ackReadErr := binary.Read(ackBuf, binary.BigEndian, &receivedCrc)
	// panicErr(ackReadErr)

	// // Validate CRC
	// if receivedCrc != sendCrc {
	// 	// Warning
	// 	log.Warnf("Acked transport bytes crc %d send crc %d", receivedCrc, sendCrc)

	// 	// Return client
	// 	this._returnConnection(node, connection)

	// 	// Try again
	// 	return errors.New("CRC mismatch")
	// }

	// // Return client
	// this._returnConnection(node, connection)

	// return err
}

// Start
func (this *NetworkTransport) start() {
	// Validate transport
	if this.protocol == "tcp" && this._onConnect == nil {
		panic("No on-connect defined for TCP")
	}
	if this._onMessage == nil {
		panic("No on-message defined")
	}

	// Start server
	go this.listen()
}

// New NetworkTransport service
func newNetworkTransport(protocol string, serviceName string, port int, receiveBufferLen int) *NetworkTransport {
	g := &NetworkTransport{
		connectionPoolSize: 1,
		protocol:           protocol,
		port:               port,
		serviceName:        serviceName,
		connections:        make(map[string]*TransportConnectionPool),
		isConnecting:       make(map[string]bool),
		receiveBufferLen:   receiveBufferLen,
	}

	// Compression
	gzip := newTransportGzip()
	g._compress = gzip._compress
	g._decompress = gzip._decompress

	return g
}
