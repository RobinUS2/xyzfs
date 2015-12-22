package main

import (
	"math/rand"
	"testing"
	"time"
)

func TestBinaryTransportFileSplitter(t *testing.T) {
	splitter := newBinaryTransportFileSplitter(1024)

	// Test wide range of data sizes
	for i := 0; i < 4096; i++ {
		// Fake file with data
		fileMeta := newFileMeta("text.txt")
		data := make([]byte, i)
		fileMeta.Size = uint32(len(data))

		// Split
		chunks := splitter.Split(fileMeta, data)

		// Chunk count check
		if len(chunks) < 1 {
			log.Errorf("Should have at least one chunk for size %d bytes", i)
		}
		if len(chunks) > 5 {
			log.Error("Largest chunk count should be 5")
		}

		// Validate chunks
		for _, chunk := range chunks {
			if chunk.Data == nil {
				log.Error("Chunk nill")
			} else if len(chunk.Data) > 1024 {
				log.Error("Chunk contains more data than split size")
			} else if len(chunk.Data) == 0 {
				log.Error("Chunk empty")
			}
		}

		// Now, we're going to receive this file in chunks
		receiver := newBinaryTransportFileReceiver()

		// Randomize to fake order
		chunksShuffled := shuffle(chunks)

		// Duplicate message
		chunks = append(chunks, chunks[0])

		// Add messages to receiver
		for _, chunk := range chunksShuffled {
			receiver.AddMessage(chunk)
		}
	}
}

// Randomize order
func shuffle(arr []*BinaryTransportMessage) []*BinaryTransportMessage {
	t := time.Now()
	rand.Seed(int64(t.Nanosecond())) // no shuffling without this line

	for i := len(arr) - 1; i > 0; i-- {
		j := rand.Intn(i)
		arr[i], arr[j] = arr[j], arr[i]
	}
	return arr
}
