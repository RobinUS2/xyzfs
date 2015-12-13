package main

import (
	"testing"
)

func TestNewBlock(t *testing.T) {
	// Register on volume
	b := datastore.NewBlock()

	// Persist
	b.Persist()

	// Encode
	b.ErasureEncoding()

	// Persist with encoding
	b.Persist()
}
