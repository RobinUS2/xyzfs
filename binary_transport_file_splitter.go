package main

import (
	"bytes"
	"encoding/binary"
	"math"
)

// Binary transport of files
// as files can be pretty big we transport them in chunks
// chunk 0: chunk number (uint32) - file meta length (uint32) - actual file meta bytes - chunk count (uint32) - content chunk length (uint32) - content bytes
// chunk 1-N: chunk number (uint32) - content chunk length (uint32) - content bytes
// the chunk number is 0-based index

// Splitter
type BinaryTransportFileSplitter struct {
	ChunkSize                      uint32
	effectiveBytesAdditionalChunks uint32
}

// Split
func (this *BinaryTransportFileSplitter) Split(meta *FileMeta, data []byte) []*BinaryTransportMessage {
	// Chunks
	chunks := make([]*BinaryTransportMessage, 0)

	// Meta to binary
	metaBytes := meta.Bytes()
	metaBytesLen := uint32(len(metaBytes))
	if metaBytesLen > this.ChunkSize {
		panic("Meta bytes does not fit in chunk size of split")
	}

	// Byte length
	dataLen := uint32(len(data))

	// Determine chunk count
	var chunkCount uint32
	var availableContentBytesFirstChunk uint32 = this.ChunkSize - 4 /* chunk number */ - 4 /* file meta length */ - metaBytesLen - 4 /* chunk count */ - 4 /* content chunk length */
	if availableContentBytesFirstChunk >= dataLen {
		// All fits in one chunk
		chunkCount = 1
	} else {
		// Overhead per chunk after the first one
		chunkCount = 1
		dataLeft := dataLen - availableContentBytesFirstChunk
		chunkCount += uint32(math.Ceil(float64(dataLeft) / float64(this.effectiveBytesAdditionalChunks)))
	}

	// Create chunks
	var contentPointer uint32 = 0
	for i := 0; i < int(chunkCount); i++ {
		// New buffer
		buf := new(bytes.Buffer)

		// Chunk number
		binary.Write(buf, binary.BigEndian, uint32(i))

		// Content length var in this buffer
		var contentLen uint32

		// Based on chunk
		switch i {
		case 0:
			// First meta chunk
			binary.Write(buf, binary.BigEndian, uint32(metaBytesLen)) // file meta length
			buf.Write(metaBytes)                                      // meta bytes
			binary.Write(buf, binary.BigEndian, uint32(chunkCount))   // chunk count

			// Bytes in chunk
			if dataLen < availableContentBytesFirstChunk {
				contentLen = dataLen
			} else {
				contentLen = availableContentBytesFirstChunk
			}
			binary.Write(buf, binary.BigEndian, uint32(contentLen)) // content length of this chunk
			buf.Write(data[contentPointer : contentPointer+contentLen])
			break
		default:
			// Additional chunks

			// Data left
			var dataLeft uint32 = dataLen - contentPointer

			// Bytes in chunk
			if dataLeft < this.effectiveBytesAdditionalChunks {
				contentLen = dataLeft
			} else {
				contentLen = this.effectiveBytesAdditionalChunks
			}
			binary.Write(buf, binary.BigEndian, uint32(contentLen)) // content length of this chunk
			buf.Write(data[contentPointer : contentPointer+contentLen])
			break
		}

		// Conver to message
		// log.Infof("Chunk #%d contains %d content bytes (total bytes %d) (pointer at %d)", i, contentLen, len(buf.Bytes()), contentPointer)
		chunk := newBinaryTransportMessage(FileBinaryTransportMessageType, buf.Bytes())
		chunks = append(chunks, chunk)

		// Move content pointer
		contentPointer += contentLen
	}

	// log.Infof("Chunk count %d (first chunk %d content, additioanl chunk %d content, meta len %d, data len %d)", chunkCount, availableContentBytesFirstChunk, this.effectiveBytesAdditionalChunks, metaBytesLen, dataLen)

	// Return chunks
	return chunks
}

// New splitter
func newBinaryTransportFileSplitter(chunkSize uint32) *BinaryTransportFileSplitter {
	return &BinaryTransportFileSplitter{
		ChunkSize:                      chunkSize,
		effectiveBytesAdditionalChunks: chunkSize - 4 /* chunk number */ - 4 /* content chunk length */ - 0,
	}
}
