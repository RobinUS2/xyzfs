package main

import (
	"testing"
)

func TestTransportGzip(t *testing.T) {
	g := newTransportGzip()

	input := "john doe"

	// Compress
	bc, ce := g._compress([]byte(input))
	if ce != nil {
		log.Errorf("Error while compressing: %s", ce)
	}
	if len(bc) < 1 {
		log.Errorf("Empty compressed bytes")
	}

	// Decompress
	db, de := g._decompress(bc)
	if de != nil {
		log.Errorf("Error while decompressing: %s", de)
	}
	if input != string(db) {
		log.Errorf("Expected %s found %s", input, string(db))
	}
}
