package main

import (
	"fmt"
	"net"
	"time"
)

type TransportConnection struct {
	node                string
	pool                *TransportConnectionPool
	conn                *net.Conn
	id                  string
	profilerMeasurement *PerformanceProfilerMeasurement
}

func (this *TransportConnection) Conn() net.Conn {
	// Try connect
	if this.conn == nil {
		this.Connect()
	}
	// Still not worked?
	if this.conn == nil {
		return nil
	}
	// Yes connection
	return *this.conn
}

func (this *TransportConnection) AttachProfiler(p *PerformanceProfilerMeasurement) {
	this.profilerMeasurement = p
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
	for i := 0; i < 5; i++ {
		conn, conE := net.Dial(this.pool.Transport.protocol, fmt.Sprintf("%s:%d", this.pool.Node, this.pool.Transport.port))
		if conE != nil {
			log.Errorf("Failed to connect to %s: %s", this.node, conE)

			if conn != nil {
				conn.Close()
			}
			time.Sleep(1 * time.Second)
			continue
		}

		// Yay!
		this.conn = &conn
		break
	}
}

func newTransportConnection(pool *TransportConnectionPool) *TransportConnection {
	return &TransportConnection{
		node: pool.Node,
		pool: pool,
		id:   uuidToString(randomUuid()),
	}
}
