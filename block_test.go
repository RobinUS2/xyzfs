package main

import (
	"testing"
)

func TestNewBlock(t *testing.T) {
	b := newBlock()
	b.ErasureEncoding()
}
