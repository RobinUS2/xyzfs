package main

import (
	"bytes"
)

// Byte writer to a shard

func (this *Shard) _toBinaryFormat() []byte {
	buf := new(bytes.Buffer)

	// Global read lock
	this.contentsMux.Lock()

	// Actual file contents
	if this.contents != nil {
		buf.Write(this.contents.Bytes())
	}

	// File meta
	buf.Write(this.shardFileMeta.Bytes())

	// File index
	buf.Write(this.shardIndex.Bytes())

	// Shard meta
	buf.Write(this.shardMeta.Bytes())

	// Unlock
	this.contentsMux.Unlock()

	return buf.Bytes()
}
