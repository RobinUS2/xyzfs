package main

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Responsible for figuring out which other nodes are out there

var gossip *Gossip

type Gossip struct {
	// Connections
	connections    map[string]*net.Conn
	connectionsMux sync.RWMutex
}

// Listen
func (this *Gossip) listen() {
	log.Infof("Starting gossip on port TCP/%d", conf.GossipPort)
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.GossipPort))
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
func (this *Gossip) handleConnection(conn net.Conn) {
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
		msg := &GossipMessage{}
		msg.FromBytes(tbuf[0:n])
		log.Infof("Gossip message %v", msg)
	}
}

// Discover seeds
func (this *Gossip) discoverSeeds() {
	for _, seed := range conf.Seeds {
		go this._connect(seed)
	}
}

// Discover a single seed
func (this *Gossip) _connect(node string) {
	log.Infof("Contacting node %s", node)
	var conn net.Conn
	var err error
	for {
		conn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", node, conf.GossipPort))
		if err != nil {
			// handle error
			log.Errorf("Failed to contact node %s: %s", node, err)

			// Close connection
			if conn != nil {
				conn.Close()
			}

			// Wait a bit
			time.Sleep(1 * time.Second)

			// Try again
			continue
		}

		log.Infof("Established connection with node %s", node)
		break
	}

	// Keep connection
	this.connectionsMux.Lock()
	this.connections[node] = &conn
	this.connectionsMux.Unlock()

	// Send HELLO message
	this._sendHello(node)
}

// Send hello message to node
func (this *Gossip) _sendHello(node string) {
	msg := newGossipMessage(HelloGossipMessageType, nil)
	this._send(node, msg)
}

// Send message
func (this *Gossip) _send(node string, msg *GossipMessage) {
	// @todo rewrite, use a channel to buffer messages and write async in order to node
	this.connectionsMux.Lock()
	defer this.connectionsMux.Unlock()
	if this.connections[node] == nil {
		log.Warnf("No connection found to %s", node)
		return
	}
	conn := *this.connections[node]
	b := msg.Bytes()
	conn.Write(b)
	log.Debugf("Written %d gossip bytes to %s", len(b), node)
}

// New gossip service
func newGossip() *Gossip {
	g := &Gossip{
		connections: make(map[string]*net.Conn),
	}

	// Start server
	go g.listen()

	// Start seed discovery
	g.discoverSeeds()

	return g
}
