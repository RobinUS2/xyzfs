package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"time"
)

// Receiver, should only be used for one file
type BinaryTransportFileReceiver struct {
	lastChunkReceived  time.Time
	mux                sync.RWMutex
	fileMeta           *FileMeta
	data               *bytes.Buffer
	targetShardId      []byte
	chunkCount         uint32
	receivedDataChunks map[uint32][]byte // Data
}

// Receive chunk, returns true if done
func (this *BinaryTransportFileReceiver) Add(buf *bytes.Reader) bool {
	// Update last chunk received
	this.mux.Lock()
	this.lastChunkReceived = time.Now()
	this.mux.Unlock()

	// Read chunk number
	var err error
	var chunkNumber uint32
	err = binary.Read(buf, binary.BigEndian, &chunkNumber)
	panicErr(err)

	// Dedup based on chunk number
	this.mux.Lock()
	if this.receivedDataChunks[chunkNumber] != nil {
		this.mux.Unlock()
		log.Debug("Ignoring data file transfer chunk, duplicate message")
		return false
	}
	this.receivedDataChunks[chunkNumber] = make([]byte, 0)
	this.mux.Unlock()

	// Handle this message
	if chunkNumber == 0 {
		this._readFirstChunk(chunkNumber, buf)
	} else {
		this._readContentChunk(chunkNumber, buf)
	}

	// Done?
	this.mux.RLock()
	defer this.mux.RUnlock()
	return uint32(len(this.receivedDataChunks)) == this.chunkCount
}

// Get last chunk received time
func (this *BinaryTransportFileReceiver) GetLastChunkReceived() time.Time {
	this.mux.RLock()
	defer this.mux.RUnlock()
	return this.lastChunkReceived
}

// Read first chunk
func (this *BinaryTransportFileReceiver) _readFirstChunk(chunkNumber uint32, buf *bytes.Reader) {
	var err error

	// Meta already here? Deduplicate payloads (chunk number could have been 0 by malformed byte)
	this.mux.RLock()
	if this.fileMeta != nil {
		this.mux.RUnlock()
		log.Warn("Ignoring metadata file transfer chunk, duplicate message")
		return
	}
	this.mux.RUnlock()

	// Read target shard flag
	targetShardSet, targetShardSetErr := buf.ReadByte()
	panicErr(targetShardSetErr)

	// Target shard id
	targetShardIdBytes := make([]byte, 16)
	targetShardIdBytesRead, _ := buf.Read(targetShardIdBytes)
	if uint32(targetShardIdBytesRead) != 16 {
		panic("Target shard id bytes read mismatch")
	}

	// Set target shard
	if targetShardSet == 1 {
		this.mux.Lock()
		this.targetShardId = targetShardIdBytes
		this.mux.Unlock()
	}

	// Read meta length
	var metaLen uint32
	err = binary.Read(buf, binary.BigEndian, &metaLen)
	panicErr(err)

	// Read meta
	metaBytes := make([]byte, metaLen)
	metaBytesRead, _ := buf.Read(metaBytes)
	if uint32(metaBytesRead) != metaLen {
		panic("Meta bytes read mismatch")
	}
	meta := &FileMeta{}
	meta.FromBytes(metaBytes)

	// Read chunk count
	var chunkCount uint32
	err = binary.Read(buf, binary.BigEndian, &chunkCount)
	panicErr(err)

	// Set meta into receiver
	this.mux.Lock()
	this.fileMeta = meta
	this.chunkCount = chunkCount
	this.mux.Unlock()

	// Read actual content
	this._readContent(chunkNumber, buf)
}

// Read content chunk
func (this *BinaryTransportFileReceiver) _readContentChunk(chunkNumber uint32, buf *bytes.Reader) {
	// Read content
	this._readContent(chunkNumber, buf)
}

// Read content
func (this *BinaryTransportFileReceiver) _readContent(chunkNumber uint32, buf *bytes.Reader) {
	var err error

	// Read content length
	var contentLen uint32
	err = binary.Read(buf, binary.BigEndian, &contentLen)
	panicErr(err)

	// Read content
	contentBytes := make([]byte, contentLen)
	contentBytesRead, _ := buf.Read(contentBytes)
	if uint32(contentBytesRead) != contentLen {
		panic("Data bytes read mismatch")
	}
	this.mux.Lock()
	this.receivedDataChunks[chunkNumber] = contentBytes
	this.mux.Unlock()
}

// To file bytes (ordered, deduplicated, complete)
func (this *BinaryTransportFileReceiver) Bytes() ([]byte, error) {
	// Write all to final buffer
	finalBuf := new(bytes.Buffer)
	for i := 0; i < int(this.chunkCount); i++ {
		// Write to final buffer
		k := uint32(i)
		finalBuf.Write(this.receivedDataChunks[k])
		delete(this.receivedDataChunks, k)

	}

	// To byte array
	b := finalBuf.Bytes()

	// Validate final CRC with metadata
	crc := crc32.Checksum(b, crcTable)
	if crc != this.fileMeta.Checksum {
		return nil, errors.New(fmt.Sprintf("Checksum not valid, expected %d (len %d) found %d (len %d)", this.fileMeta.Checksum, this.fileMeta.Size, crc, len(b)))
	}

	return b, nil
}

// New receiver
func newBinaryTransportFileReceiver() *BinaryTransportFileReceiver {
	return &BinaryTransportFileReceiver{
		fileMeta:           nil,
		receivedDataChunks: make(map[uint32][]byte),
		lastChunkReceived:  time.Now(),
	}
}
