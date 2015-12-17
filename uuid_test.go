package main

import (
	"testing"
)

func TestUuid(t *testing.T) {
	u := randomUuid()
	if len(u) != 16 {
		t.Error("Uuid not of length 16")
	}

	// To string
	us := uuidToString(u)
	if len(us) != 36 {
		t.Error("Uuid string not of length 36")
	}

	// Back to bytes again
	usb := uuidStringToBytes(us)
	if len(usb) != 16 {
		t.Error("Uuid not of length 16 from string")
	}
}
