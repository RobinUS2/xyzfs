package main

import (
	"bufio"
	"bytes"
	"testing"
	"time"
)

func TestMagicFooterValidation(t *testing.T) {
	// Transport
	tr := newNetworkTransport("tcp", "binary", 123456, 1024*1024, true)

	// Vars
	var br *bufio.Reader
	var res bool

	// Zero bytes
	br = bufio.NewReader(bytes.NewReader([]byte{0, 0, 0, 0, 0}))
	res = tr._validateMagicFooter(br, 1, 4)
	if res {
		t.Error("Byte sequence should not be valid")
	}

	// 1st char
	br = bufio.NewReader(bytes.NewReader([]byte{0, 0, 0, 0}))
	res = tr._validateMagicFooter(br, 1, 4)
	if res {
		t.Error("Byte sequence should not be valid")
	}

	// 2nd char
	br = bufio.NewReader(bytes.NewReader([]byte{'\n', 0, 0, 0}))
	res = tr._validateMagicFooter(br, 1, 4)
	if res {
		t.Error("Byte sequence should not be valid")
	}

	// 3rd char
	br = bufio.NewReader(bytes.NewReader([]byte{'\n', 'X', 0, 0}))
	res = tr._validateMagicFooter(br, 1, 4)
	if res {
		t.Error("Byte sequence should not be valid")
	}

	// 4th char
	br = bufio.NewReader(bytes.NewReader([]byte{'\n', 'X', 'Y', 0}))
	res = tr._validateMagicFooter(br, 1, 4)
	if res {
		t.Error("Byte sequence should not be valid")
	}

	// 5th char
	br = bufio.NewReader(bytes.NewReader([]byte{'\n', 'X', 'Y', 'Z'}))
	res = tr._validateMagicFooter(br, 1, 4)
	if res == false {
		t.Error("Byte sequence should be valid")
	}
}

func TestTransport(t *testing.T) {
	// Transport
	tr := newNetworkTransport("tcp", "binary", 12345, 1024*1024, true)

	// On-connect
	tr._onConnect = func(cmeta *TransportConnectionMeta, node string) {
		// Stub
	}

	// On-message
	tr._onMessage = func(cmeta *TransportConnectionMeta, b []byte) {
		// Stub
	}

	// Start listening
	tr.start()

	// Configure timeout
	var timeout chan bool
	timeout = make(chan bool, 1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		timeout <- true
	}()

	// Start fetching
	var res chan interface{}
	res = make(chan interface{}, 1)
	go func() {
		res <- tr._getConnection("127.0.0.1")
		log.Infof("Got conn")
	}()

	// Wait for connection receive, or error timeout
	var conn *TransportConnection
	select {
	case x := <-res:
		conn = x.(*TransportConnection)
		// OK :)
		break
	case <-timeout:
		// We hit timeout, this means something is wrong with the connection pool
		t.Error("Get connection hit timeout")
		break
	}

	// Return connection
	tr._returnConnection("127.0.0.1", conn)

	// Get another connection
	conn2 := tr._getConnection("127.0.0.1")

	// Discard connection
	tr._discardConnection("127.0.0.1", conn2)

	// Get another connection
	conn3 := tr._getConnection("127.0.0.1")

	// Return connection
	tr._returnConnection("127.0.0.1", conn3)

}
