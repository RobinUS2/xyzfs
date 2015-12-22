package main

import (
	"testing"
)

func TestBinaryTransportFileSplitter(t *testing.T) {
	splitter := newBinaryTransportFileSplitter(1024)

	// Test wide range of data sizes
	for i := 0; i < 4096; i++ {
		fileMeta := newFileMeta("text.txt")
		data := make([]byte, i)
		fileMeta.Size = uint32(len(data))
		chunks := splitter.Split(fileMeta, data)
		if len(chunks) < 1 {
			log.Errorf("Should have at least one chunk for size %d bytes", i)
		}
		if len(chunks) > 5 {
			log.Error("Largest chunk count should be 5")
		}
		for _, chunk := range chunks {
			if chunk.Data == nil {
				log.Error("Chunk nill")
			} else if len(chunk.Data) > 1024 {
				log.Error("Chunk contains more data than split size")
			} else if len(chunk.Data) == 0 {
				log.Error("Chunk empty")
			}
		}
	}
}
