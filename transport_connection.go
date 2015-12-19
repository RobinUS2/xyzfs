package main

import (
	"net"
)

type TransportConnection struct {
	node string
	conn *net.Conn
}

func newTransportConnection(node string, conn *net.Conn) *TransportConnection {
	return &TransportConnection{
		node: node,
		conn: conn,
	}
}
