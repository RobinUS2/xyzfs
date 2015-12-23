package main

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
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
		fileMeta.Checksum = crc32.Checksum(data, crcTable)

		// Split (no target shard id)
		chunks := splitter.Split(fileMeta, data, nil)

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

		// Duplicate message
		if len(chunks) > 1 {
			// Not first message
			chunks = append(chunks, chunks[1])
		}
		// First message
		chunks = append(chunks, chunks[0])

		// Randomize to fake order
		chunksShuffled := shuffle(chunks)

		// Add messages to receiver
		var done bool = false
		for _, chunk := range chunksShuffled {
			// New byte reader
			buf := bytes.NewReader(chunk.Data)
			var err error

			// Read transfer sequence number
			var transferNumber uint32
			err = binary.Read(buf, binary.BigEndian, &transferNumber)
			panicErr(err)

			// Add message
			done = receiver.Add(buf)
			if done {
				break
			}
		}

		// Done?
		if done == false {
			log.Error("Should have been done by now..")
		}

		// To bytes
		b, be := receiver.Bytes()
		if be != nil {
			log.Errorf("Receiver to bytes has unexpected error: %s", be)
		}
		if len(b) != i {
			log.Errorf("Receiver returned %d bytes instead of expted %d", len(b), i)
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
