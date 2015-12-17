package main

import (
	"fmt"
	"io"
	"net"
)

// Responsible for figuring out which other nodes are out there

var gossip *Gossip

type Gossip struct {
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
	}
}

func newGossip() *Gossip {
	g := &Gossip{}

	// Start server
	go g.listen()

	return g
}
