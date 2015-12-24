package main

import (
	"fmt"
	"net"
)

type TransportConnection struct {
	node string
	pool *TransportConnectionPool
	conn *net.Conn
	id   string
}

func (this *TransportConnection) Conn() net.Conn {
	return *this.conn
}

func (this *TransportConnection) Close() {
	log.Infof("Closing transport connection to %s", this.node)
	if this.conn != nil {
		(*this.conn).Close()
		this.conn = nil
	}
}

func (this *TransportConnection) Connect() {
	// Close previous
	this.Close()

	// Connect
	conn, conE := net.Dial(this.pool.Transport.protocol, fmt.Sprintf("%s:%d", this.pool.Node, this.pool.Transport.port))
	if conE != nil {
		log.Errorf("Failed to connect to %s: %s", this.node, conE)
	}
	this.conn = &conn
}

func newTransportConnection(pool *TransportConnectionPool) *TransportConnection {
	return &TransportConnection{
		pool: pool,
		id:   uuidToString(randomUuid()),
	}
}
