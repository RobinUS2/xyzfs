package main

import (
	"bytes"
	"github.com/klauspost/reedsolomon"
)

// Create parity of block
func (this *Block) ErasureEncoding() {
	// Create encoder
	enc, err := reedsolomon.New(len(this.DataShards), len(this.ParityShards))
	if err != nil {
		panic(err)
	}

	// Build data array
	var data [][]byte = make([][]byte, len(this.DataShards)+len(this.ParityShards))
	for i, s := range this.DataShards {
		data[i] = s.Contents().Bytes()
	}
	for ip, is := range this.ParityShards {
		data[len(this.DataShards)+ip] = is.Contents().Bytes()
	}

	// Encode
	encodeE := enc.Encode(data)
	if encodeE != nil {
		panic(encodeE)
	}

	// Write party into shard
	for i := len(this.DataShards); i < len(this.ParityShards); i++ {
		this.ParityShards[i].SetContents(bytes.NewBuffer(data[i]))
	}
}
