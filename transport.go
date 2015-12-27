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
	"time"
)

var TRANSPORT_MAGIC_FOOTER []byte = []byte{'\r', '\n', 'X', 'Y', 'Z'}

// Network transport layer

type NetworkTransport struct {
	traceLog bool
	// Generic mutex
	mux sync.RWMutex
	// Config
	protocol         string
	port             int
	serviceName      string
	listener         *net.Listener
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
	// Performance profiler
	profiler *PerformanceProfiler
}

// Close
func (this *NetworkTransport) close() {
	// Only applies to TCP
	if this.protocol != "tcp" {
		return
	}

	// Close transport
	log.Infof("Closing transport %s", this.serviceName)
	log.Infof("Closing transport socket %s", this.serviceName)
	this.mux.Lock()
	if this.listener != nil {
		ln := (*this.listener)
		if ln != nil {
			ln.Close()
		}
		this.listener = nil
		this.listenAddr = ""
	}
	this.mux.Unlock()
	log.Infof("Closed transport socket %s", this.serviceName)

	// Close pools
	log.Infof("Closing transport pools %s", this.serviceName)
	this.connectionsMux.Lock()
	for _, tcp := range this.connections {
		tcp.Close()
	}
	this.connectionsMux.Unlock()
	log.Infof("Closed transport pools %s", this.serviceName)

	log.Infof("Closed transport %s", this.serviceName)

	// Wait a bit, so the OS can cleanup resources
	time.Sleep(50 * time.Millisecond)
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
	// Start TCP
	ln, err := net.Listen(this.protocol, fmt.Sprintf(":%d", this.port))
	if err != nil {
		panicErr(err)
	}

	// Store listen details
	this.mux.Lock()
	this.listener = &ln
	this.listenAddr = ln.Addr().String()
	this.mux.Unlock()
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			if this.listener == nil {
				log.Infof("Exiting %s listen loop", this.serviceName)
				break
			}
			log.Infof("Listen %s error: %s", this.serviceName, err)
			continue
		}
		go this.handleConnection(conn)
	}
}

// Listen TCP
func (this *NetworkTransport) _listenUdp() {
	// Prepare address
	serverAddr, err := net.ResolveUDPAddr(this.protocol, fmt.Sprintf(":%d", this.port))
	panicErr(err)

	// Start UDP
	ln, err := net.ListenUDP(this.protocol, serverAddr)
	if err != nil {
		panicErr(err)
	}

	// Store listen details
	this.mux.Lock()
	this.listener = nil
	this.listenAddr = ln.LocalAddr().String()
	this.mux.Unlock()

	// Listen for incoming
	for {
		tbuf := make([]byte, this.receiveBufferLen)
		n, addr, err := ln.ReadFromUDP(tbuf)
		log.Debugf("Received ", string(tbuf[0:n]), " from ", addr)

		// Error?
		if err != nil {
			log.Errorf("UDP %s receive error: %s", this.serviceName, err)
			continue
		}

		// Read message
		this._onMessage(newTransportConnectionMeta(ln.RemoteAddr().String()), tbuf[0:n])
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
	// Recover from panics in TCP layer
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Recovered in handleConnection of %s: %s", this.serviceName, r)
			conn.Close()
		}
	}()

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
		if this.traceLog {
			log.Infof("Received %d %s bytes for %s from %s", len(by), this.protocol, this.serviceName, conn.RemoteAddr().String())
		}
		// Was there an error in reading ?
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %s", err)
			}
			break
		}

		// Write to buffer
		tbuf.Write(by)
		if this.traceLog {
			log.Infof("tbuf now %d", len(tbuf.Bytes()))
		}

		// Is this the end?
		if this._validateMagicFooter(connbuf, 1, 4) == false {
			if this.traceLog {
				log.Infof("Magic footer not yet found")
			}
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
		if this.traceLog {
			log.Infof("Sending ack %d to %s for %s", receivedCrc, conn.RemoteAddr().String(), this.serviceName)
		}

		// New buffer
		tbuf = new(bytes.Buffer)
	}
}

// Prepare connection
func (this *NetworkTransport) _prepareConnection(node string) {
	// Get channel
	this.connectionsMux.Lock()
	if this.connections[node] == nil {
		this.connections[node] = newTransportConnectionPool(this, node, this.connectionPoolSize)

		// Send HELLO message to new nodes
		if this._onConnect != nil {
			// On-connect
			go func() {
				log.Infof("Starting on-connect to %s for %s", node, this.serviceName)
				tc := this.connections[node].GetConnection()
				conn := tc.Conn()
				if conn != nil {
					this._onConnect(newTransportConnectionMeta(conn.RemoteAddr().String()), node)
					log.Infof("Executed on-connect to %s for %s", node, this.serviceName)
				}
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

	// Compress
	bc, ce := this._compress(b)
	if ce != nil {
		panic("Failed to compress")
	}

	// CRC
	sendCrc := crc32.Checksum(b, crcTable)

	// Retries
	var errb error
	for i := 0; i < 2; i++ {
		// Get connection
		tc := this._getConnection(node)

		// Get socket
		conn := tc.Conn()

		// Socket received?
		if conn == nil {
			log.Warnf("Unable to get socket on connection %s to %s for %s", tc.id, node, this.serviceName)

			// Try again
			continue
		}

		// Before send log
		if this.traceLog {
			log.Infof("Sending %d (crc=%d, compressed %d) %s bytes for %s to %s over %s", len(b), sendCrc, len(bc), this.protocol, this.serviceName, node, tc.id)
		}

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
		if this.traceLog {
			log.Infof("Sent %d (crc=%d, compressed %d) %s bytes for %s to %s over %s", len(b), sendCrc, len(bc), this.protocol, this.serviceName, node, tc.id)
		}

		// Read ACK on this socket
		// @Todo timeout ACK wait
		if this.traceLog {
			log.Infof("Waiting for ack %s", this.serviceName)
		}
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
			if this.traceLog {
				log.Infof("Ack passed for %d", sendCrc)
			}
		}

		// Return connection
		this._returnConnection(node, tc)

		// OK :)
		return nil
	}

	return errb
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
func newNetworkTransport(protocol string, serviceName string, port int, receiveBufferLen int, numStreams int, traceLog bool) *NetworkTransport {
	g := &NetworkTransport{
		connectionPoolSize: numStreams,
		protocol:           protocol,
		port:               port,
		serviceName:        serviceName,
		connections:        make(map[string]*TransportConnectionPool),
		isConnecting:       make(map[string]bool),
		receiveBufferLen:   receiveBufferLen,
		traceLog:           traceLog,
	}

	// Compression
	gzip := newTransportGzip()
	g._compress = gzip._compress
	g._decompress = gzip._decompress

	return g
}
