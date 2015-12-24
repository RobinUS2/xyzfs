package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"strings"
	"sync"
	"time"
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
	connections    map[string]chan *TransportConnection
	connectionsMux sync.RWMutex
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
		// Was there an error in reading ?
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %s", err)
			}
			break
		}

		// Write to buffer
		tbuf.Write(by)

		// Is this the end?
		if this._validateMagicFooter(connbuf, 1, 4) == false {
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

// Discover a single seed
func (this *NetworkTransport) _connect(node string) *TransportConnection {
	// Start contacting node
	log.Infof("Contacting node %s for %s", node, this.serviceName)
	var conn net.Conn
	var err error
	for i := 0; i < 5; i++ {
		conn, err = net.Dial(this.protocol, fmt.Sprintf("%s:%d", node, this.port))
		if err != nil {
			// handle error
			log.Errorf("Failed to contact node %s for %s: %s", node, this.serviceName, err)

			// Close connection
			if conn != nil {
				conn.Close()
			}

			// Wait a bit
			time.Sleep(50 * time.Millisecond)

			// Try again
			continue
		}

		log.Infof("Established connection with node %s for %s", node, this.serviceName)
		break
	}
	if conn == nil {
		return nil
	}

	// Get channel
	this.connectionsMux.RLock()
	c := this.connections[node]
	this.connectionsMux.RUnlock()

	// Existing channel?
	if c == nil {
		this.connectionsMux.Lock()
		this.connections[node] = make(chan *TransportConnection, 4) // @todo configure connection pool size
		this.connectionsMux.Unlock()

		// Send HELLO message
		if this._onConnect != nil {
			this._onConnect(newTransportConnectionMeta(conn.RemoteAddr().String()), node)
		}
	}

	return newTransportConnection(node, &conn)
}

// Get connection
func (this *NetworkTransport) _getConnection(node string) *TransportConnection {
	// Get channel
	this.connectionsMux.RLock()
	c := this.connections[node]
	this.connectionsMux.RUnlock()

	// Connection from channel
	select {
	case conn := <-c:
		return conn
	default:
		return this._connect(node)
	}
}

// Return connection
func (this *NetworkTransport) _returnConnection(node string, tc *TransportConnection) {
	// Get channel
	this.connectionsMux.RLock()
	c := this.connections[node]
	this.connectionsMux.RUnlock()

	select {
	case c <- tc:
		// Into channel
		break
	default:
		// overflow
		tc.Close()
	}
}

// Send message
func (this *NetworkTransport) _send(node string, b []byte) error {
	// Get connection
	var connection *TransportConnection = this._getConnection(node)
	if connection == nil {
		return errors.New("Unable to get conection")
	}

	// Get socket
	var conn net.Conn = *connection.conn

	// CRC
	sendCrc := crc32.Checksum(b, crcTable)

	// Compress
	bc, ce := this._compress(b)
	if ce != nil {
		panic("Failed to compress")
	}

	// Simple retry handler
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		// Write data
		// log.Infof("Writing %s", this.serviceName)
		_, err = conn.Write(bc)

		// Write magic footer
		conn.Write(TRANSPORT_MAGIC_FOOTER)

		// Validate
		if err != nil {
			log.Warnf("Failed to write %d %s bytes to %s: %s", len(b), this.serviceName, node, err)

			// Close socket
			connection.Close()

			// Do not return connection, it's broken

			// Get new connection
			connection = this._getConnection(node)

			// Attempt two
			continue
		} else {
			log.Debugf("Written %d (raw %d) %s bytes to %s", len(bc), len(b), this.serviceName, node)
		}

		// Read ACK on this socket
		// @Todo timeout ACK wait
		// log.Infof("Waiting for ack %s", this.serviceName)
		ackBytes := make([]byte, 4)
		conn.Read(ackBytes)
		ackBuf := bytes.NewReader(ackBytes)
		var receivedCrc uint32
		ackReadErr := binary.Read(ackBuf, binary.BigEndian, &receivedCrc)
		panicErr(ackReadErr)

		// Validate CRC
		if receivedCrc != sendCrc {
			// Warning
			log.Warnf("Acked transport bytes crc %d send crc %d", receivedCrc, sendCrc)

			// Try again
			continue
		}

		// log.Infof("Done %s", this.serviceName)
		break
	}

	// Return client
	this._returnConnection(node, connection)

	return err
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
		protocol:         protocol,
		port:             port,
		serviceName:      serviceName,
		connections:      make(map[string]chan *TransportConnection),
		isConnecting:     make(map[string]bool),
		receiveBufferLen: receiveBufferLen,
	}

	// Compression
	gzip := newTransportGzip()
	g._compress = gzip._compress
	g._decompress = gzip._decompress

	return g
}
