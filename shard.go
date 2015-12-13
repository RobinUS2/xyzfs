package main

import (
	"bytes"
)

// Shard is a partial piece of data in a block

type Shard struct {
	contents *bytes.Buffer
}

func newShard() *Shard {
	return &Shard{
		contents: nil,
	}
}
