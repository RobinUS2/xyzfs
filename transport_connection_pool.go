package main

type TransportConnectionPool struct {
	Transport *NetworkTransport
	Node      string
	Chan      chan *TransportConnection
	PoolSize  int
}

// Get
func (this *TransportConnectionPool) GetConnection() *TransportConnection {
	tc := <-this.Chan
	log.Infof("Pool %s hand out %s", this.Transport.serviceName, tc.id)
	return tc
}

// Return
func (this *TransportConnectionPool) ReturnConnection(tc *TransportConnection) {
	this.Chan <- tc
	log.Infof("Pool %s received returned %s", this.Transport.serviceName, tc.id)
}

// Discard
func (this *TransportConnectionPool) DiscardConnection(tc *TransportConnection) {
	// Close
	tc.Close()

	// Log discard
	log.Infof("Pool %s received discarded %s", this.Transport.serviceName, tc.id)

	// Open new one
	this._addConnection()
}

// Prepare
func (this *TransportConnectionPool) _prepare() {
	for i := 0; i < this.PoolSize; i++ {
		this._addConnection()
	}
}

// Add connection
func (this *TransportConnectionPool) _addConnection() {
	tc := newTransportConnection(this)
	tc.Connect()
	log.Infof("Pool %s created %s", this.Transport.serviceName, tc.id)
	this.Chan <- tc
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
