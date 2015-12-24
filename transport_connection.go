package main

import (
	"net"
)

type TransportConnection struct {
	node string
	conn *net.Conn
}

func (this *TransportConnection) Close() {
	log.Infof("Closing transport connection to %s", this.node)
	if this.conn != nil {
		(*this.conn).Close()
		this.conn = nil
	}
}

func newTransportConnection(node string, conn *net.Conn) *TransportConnection {
	return &TransportConnection{
		node: node,
		conn: conn,
	}
}
