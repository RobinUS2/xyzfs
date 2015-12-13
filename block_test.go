package main

import (
	"testing"
)

func TestNewBlock(t *testing.T) {
	// Create
	b := newBlock()

	// Persist
	b.Persist()

	// Encode
	b.ErasureEncoding()

	// Persist with encoding
	b.Persist()
}
