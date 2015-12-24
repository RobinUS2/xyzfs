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
	connections    map[string]*TransportConnection
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
		log.Infof("Received ", string(tbuf[0:n]), " from ", addr)

		if err != nil {
			log.Errorf("UDP receive error: %s", err)
		} else {
			// Read message
			this._onMessage(newTransportConnectionMeta(ln.RemoteAddr().String()), tbuf[0:n])
		}
	}
}

// Handle connection
func (this *NetworkTransport) handleConnection(conn net.Conn) {
	// Read bytes
	// tbuf := make([]byte, this.receiveBufferLen)
	tbuf := new(bytes.Buffer)

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
		byTerminator, _ := connbuf.ReadByte()
		if byTerminator != TRANSPORT_MAGIC_FOOTER[1] {
			// Reverse
			connbuf.UnreadByte()

			// Continue reading rest
			continue
		}

		// Validate final
		// @todo Apply same principal with UnreadByte() as above
		if val, _ := connbuf.ReadByte(); val != TRANSPORT_MAGIC_FOOTER[2] {
			panic("Missed X from magic suffix")
		}
		if val, _ := connbuf.ReadByte(); val != TRANSPORT_MAGIC_FOOTER[3] {
			panic("Missed Y from magic suffix")
		}
		if val, _ := connbuf.ReadByte(); val != TRANSPORT_MAGIC_FOOTER[4] {
			panic("Missed Z from magic suffix")
		}

		// Decompress
		db, de := this._decompress(tbuf.Bytes())
		if de != nil {
			log.Errorf("Decompress error: %s", de)
			panic("Failed to decompress")
		}

		// Ack with checksum of decompressed received bytes
		ackBuf := new(bytes.Buffer)
		receivedCrc := crc32.Checksum(db, crcTable)
		binary.Write(ackBuf, binary.BigEndian, receivedCrc)
		conn.Write(ackBuf.Bytes())

		// Read message
		this._onMessage(newTransportConnectionMeta(conn.RemoteAddr().String()), db)

		// New buffer
		tbuf = new(bytes.Buffer)
	}
}

// Discover a single seed
func (this *NetworkTransport) _connect(node string) {
	// Are we already connected?
	this.connectionsMux.RLock()
	if this.connections[node] != nil {
		this.connectionsMux.RUnlock()
		log.Infof("Ignore connect, we are aleady connected to %s", node)
		return
	}
	this.connectionsMux.RUnlock()

	// Are we already waiting for this?
	this.isConnectingMux.Lock()
	if this.isConnecting[node] {
		this.isConnectingMux.Unlock()
		log.Infof("Ignore connect, we are aleady connecting to %s", node)
		return
	}
	this.isConnecting[node] = true
	this.isConnectingMux.Unlock()

	// Start contacting node
	log.Infof("Contacting node %s for %s", node, this.serviceName)
	var conn net.Conn
	var err error
	for {
		conn, err = net.Dial(this.protocol, fmt.Sprintf("%s:%d", node, this.port))
		if err != nil {
			// handle error
			log.Errorf("Failed to contact node %s for %s: %s", node, this.serviceName, err)

			// Close connection
			if conn != nil {
				conn.Close()
			}

			// Wait a bit
			time.Sleep(1 * time.Second)

			// Try again
			continue
		}

		log.Infof("Established connection with node %s for %s", node, this.serviceName)
		break
	}

	// Keep connection
	this.connectionsMux.Lock()
	this.connections[node] = newTransportConnection(node, &conn)
	this.connectionsMux.Unlock()

	// Done connecting
	this.isConnectingMux.Lock()
	delete(this.isConnecting, node)
	this.isConnectingMux.Unlock()

	// Send HELLO message
	if this._onConnect != nil {
		this._onConnect(newTransportConnectionMeta(conn.RemoteAddr().String()), node)
	}
}

// Send message
func (this *NetworkTransport) _send(node string, b []byte) error {
	// Lock until done
	this.connectionsMux.Lock()
	defer this.connectionsMux.Unlock()

	// Are we connected?
	if this.connections[node] == nil {
		// No connection
		errorMsg := fmt.Sprintf("No connection found to %s for %s", node, this.serviceName)
		log.Warn(errorMsg)

		// We will try to open one for next time
		go this._connect(node) // async

		// Return error
		return errors.New(errorMsg) //async, direct error
	}

	// get connection
	var connection *TransportConnection = this.connections[node]
	var conn net.Conn = *connection.conn

	// CRC
	sendCrc := crc32.Checksum(b, crcTable)

	// Compress
	bc, ce := this._compress(b)
	if ce != nil {
		panic("Failed to compress")
	}

	// Write data
	_, err := conn.Write(bc)

	// Write magic footer
	conn.Write([]byte{'\r', '\n', 'X', 'Y', 'Z'})

	// Validate
	if err != nil {
		log.Warnf("Failed to write %d %s bytes to %s: %s", len(b), this.serviceName, node, err)

		// Reset connection
		delete(this.connections, node)
	} else {
		log.Debugf("Written %d (raw %d) %s bytes to %s", len(bc), len(b), this.serviceName, node)
	}

	// Read ACK on this socket
	ackBytes := make([]byte, 4)
	conn.Read(ackBytes)
	ackBuf := bytes.NewReader(ackBytes)
	var receivedCrc uint32
	ackReadErr := binary.Read(ackBuf, binary.BigEndian, &receivedCrc)
	panicErr(ackReadErr)

	// Validate CRC
	if receivedCrc != sendCrc {
		panic(fmt.Sprintf("Acked transport bytes crc %d send crc %d", receivedCrc, sendCrc))
	}

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
		connections:      make(map[string]*TransportConnection),
		isConnecting:     make(map[string]bool),
		receiveBufferLen: receiveBufferLen,
	}

	// Compression
	gzip := newTransportGzip()
	g._compress = gzip._compress
	g._decompress = gzip._decompress

	return g
}
