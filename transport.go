package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// Network transport layer

type NetworkTransport struct {
	protocol    string
	port        int
	serviceName string
	// Handlers
	_onMessage func([]byte)
	_onConnect func(string)
	// Connections
	connections    map[string]*net.Conn
	connectionsMux sync.RWMutex
}

// Listen
func (this *NetworkTransport) listen() {
	log.Infof("Starting %s on port %s/%d", this.serviceName, strings.ToUpper(this.protocol), this.port)
	ln, err := net.Listen(this.protocol, fmt.Sprintf(":%d", this.port))
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

// Handle connection
func (this *NetworkTransport) handleConnection(conn net.Conn) {
	// Read bytes
	tbuf := make([]byte, 8096)

	for {
		n, err := conn.Read(tbuf)
		// Was there an error in reading ?
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %s", err)
			}
			break
		}
		log.Infof("Bytes %v", tbuf[0:n])

		// Read message
		this._onMessage(tbuf[0:n])
	}
}

// Discover seeds
func (this *NetworkTransport) discoverSeeds() {
	for _, seed := range conf.Seeds {
		go this._connect(seed)
	}
}

// Discover a single seed
func (this *NetworkTransport) _connect(node string) {
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
	this.connections[node] = &conn
	this.connectionsMux.Unlock()

	// Send HELLO message
	this._onConnect(node)
}

// Send message
func (this *NetworkTransport) _send(node string, b []byte) {
	// @todo rewrite, use a channel to buffer messages and write async in order to node
	this.connectionsMux.Lock()
	defer this.connectionsMux.Unlock()
	if this.connections[node] == nil {
		log.Warnf("No connection found to %s for %s", node, this.serviceName)
		return
	}
	conn := *this.connections[node]
	conn.Write(b)
	log.Debugf("Written %d %s bytes to %s", len(b), this.serviceName, node)
}

// Start
func (this *NetworkTransport) start() {
	// Start server
	go this.listen()

	// Start seed discovery
	this.discoverSeeds()
}

// New NetworkTransport service
func newNetworkTransport(protocol string, serviceName string, port int) *NetworkTransport {
	g := &NetworkTransport{
		protocol:    protocol,
		port:        port,
		serviceName: serviceName,
		connections: make(map[string]*net.Conn),
	}

	return g
}
