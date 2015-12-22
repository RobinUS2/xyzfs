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
		buf := s.Contents()
		data[i] = padByteArrZeros(buf.Bytes(), conf.ShardSizeInBytes)
	}
	for ip, is := range this.ParityShards {
		buf := is.Contents()
		data[len(this.DataShards)+ip] = padByteArrZeros(buf.Bytes(), conf.ShardSizeInBytes)
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

// Pad zero bytes
func padByteArrZeros(in []byte, n int) []byte {
	padlen := n - len(in)
	padding := bytes.Repeat([]byte{byte(0)}, padlen)
	return append(in, padding...)
}
