package main

import (
	"bufio"
	"bytes"
	"testing"
)

func TestMagicFooterValidation(t *testing.T) {
	// Transport
	tr := newNetworkTransport("tcp", "binary", 12345, 1024*1024, true)

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
