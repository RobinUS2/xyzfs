package main

import (
	"time"
)

type TransportConnectionPool struct {
	Transport *NetworkTransport
	Node      string
	Chan      chan *TransportConnection
	PoolSize  int
}

// Get
func (this *TransportConnectionPool) GetConnection() *TransportConnection {
	// Timeout
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(1 * time.Second)
		timeout <- true
	}()

	select {
	case tc := <-this.Chan:
		if this.Transport.traceLog {
			log.Infof("Pool %s hand out %s", this.Transport.serviceName, tc.id)
		}
		return tc
		break
	case <-timeout:
		log.Warnf("Pool %s hand out timeout warning", this.Transport.serviceName)
		return this._newConnection()
		break
	}
	return nil
}

// Return
func (this *TransportConnectionPool) ReturnConnection(tc *TransportConnection) {
	select {
	case this.Chan <- tc:
		if this.Transport.traceLog {
			log.Infof("Pool %s received returned %s", this.Transport.serviceName, tc.id)
		}
		break
	default:
		log.Warnf("Pool %s overflow of returned %s", this.Transport.serviceName, tc.id)
		break
	}
}

// Discard
func (this *TransportConnectionPool) DiscardConnection(tc *TransportConnection) {
	// Close
	tc.Close()

	// Log discard
	log.Infof("Pool %s received discarded %s", this.Transport.serviceName, tc.id)

	// Open new one
	go this._addConnection()
}

// Prepare
func (this *TransportConnectionPool) _prepare() {
	for i := 0; i < this.PoolSize; i++ {
		go this._addConnection()
	}
}

// Add connection
func (this *TransportConnectionPool) _addConnection() {
	tc := this._newConnection()
	log.Infof("Pool %s added %s", this.Transport.serviceName, tc.id)
	this.Chan <- tc
}

// New connection
func (this *TransportConnectionPool) _newConnection() *TransportConnection {
	tc := newTransportConnection(this)
	log.Infof("Pool %s created %s", this.Transport.serviceName, tc.id)
	tc.Connect()
	return tc
}

// New pool
func newTransportConnectionPool(t *NetworkTransport, node string, n int) *TransportConnectionPool {
	o := &TransportConnectionPool{
		Node:      node,
		Transport: t,
		Chan:      make(chan *TransportConnection, n),
		PoolSize:  n,
	}
	o._prepare()
	return o
}
