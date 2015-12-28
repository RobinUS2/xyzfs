package main

import (
	"time"
)

type TransportConnectionPool struct {
	Transport *NetworkTransport
	Node      string
	Chan      chan *TransportConnection
	PoolSize  int
	Closed    bool
	Profiler  *PerformanceProfiler
}

// Get
func (this *TransportConnectionPool) GetConnection() *TransportConnection {
	// Timeout
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(1 * time.Second)
		timeout <- true
	}()

	// Fetch from chan, or create on timeout
	var tc *TransportConnection
	select {
	case tc = <-this.Chan:
		if this.Transport.traceLog {
			log.Infof("Pool %s hand out %s", this.Transport.serviceName, tc.id)
		}
		break
	case <-timeout:
		log.Warnf("Pool %s hand out timeout warning", this.Transport.serviceName)
		tc = this._newConnection()
		break
	}

	// Attach profiler
	tc.AttachProfiler(this.Profiler.Start())

	return tc
}

// Return
func (this *TransportConnectionPool) ReturnConnection(tc *TransportConnection) {
	// Closed?
	if this.Closed {
		if this.Transport.traceLog {
			log.Infof("Pool %s received returned %s, closing now", this.Transport.serviceName, tc.id)
		}
		tc.Close()
		return
	}

	// Finalize profiler
	tc.profilerMeasurement.Success()

	// Into channel
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

	// Finalize profiler
	tc.profilerMeasurement.Error()

	// Log discard
	if this.Transport.traceLog {
		log.Infof("Pool %s received discarded %s", this.Transport.serviceName, tc.id)
	}

	// Open new one
	go this._addConnection()
}

// Prepare
func (this *TransportConnectionPool) _prepare() {
	for i := 0; i < this.PoolSize; i++ {
		go this._addConnection()
	}
}

// Close all, does not return to pool
func (this *TransportConnectionPool) Close() {
	// Set closed flag
	this.Closed = true

	// Close
	for i := 0; i < this.PoolSize; i++ {
		select {
		case tc := <-this.Chan:
			tc.Close()
			break
		default:
			// Ignore
		}
	}
}

// Add connection
func (this *TransportConnectionPool) _addConnection() {
	if this.Closed {
		log.Warnf("Not creating connection for closed pool %s", this.Transport.serviceName)
		return
	}
	tc := this._newConnection()
	if this.Transport.traceLog {
		log.Infof("Pool %s added %s to %s", this.Transport.serviceName, tc.id, this.Node)
	}
	this.Chan <- tc
}

// New connection
func (this *TransportConnectionPool) _newConnection() *TransportConnection {
	tc := newTransportConnection(this)
	if this.Transport.traceLog {
		log.Infof("Pool %s created %s", this.Transport.serviceName, tc.id)
	}
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
		Closed:    false,
		Profiler:  newPerformanceProfiler(),
	}
	o._prepare()
	return o
}
