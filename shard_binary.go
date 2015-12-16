package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
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
	log.Infof("%v", this.shardMeta.Bytes())
	buf.Write(this.shardMeta.Bytes())

	// Unlock
	this.contentsMux.Unlock()

	return buf.Bytes()
}

func (this *Shard) _fromBinaryFormat() {
	// Open file
	f, err := os.Open(this.FullPath())
	if err != nil {
		panic("File not found")
	}

	// File stat for length
	fi, fierr := f.Stat()
	if fierr != nil {
		panic("Failed to stat file")
	}
	flen := fi.Size()

	// Go to last byte to determine size of metadata
	f.Seek(flen-4, 0)
	buf := bufio.NewReader(f)
	var metadataLength uint32
	binary.Read(buf, binary.BigEndian, &metadataLength)
	log.Debugf("Meta is size of %d", metadataLength)

	// Read metadata
	f.Seek(flen-int64(metadataLength), 0)
	buf = bufio.NewReader(f)
	metaBytes := make([]byte, metadataLength)
	metaBytesRead, _ := buf.Read(metaBytes)
	if metaBytesRead != len(metaBytes) || metaBytesRead < 1 {
		panic("Failed to read metadata bytes")
	}
	log.Debugf("Read %d metadata bytes: %v", metaBytesRead, metaBytes)
	this.shardFileMeta = newShardFileMeta()
	this.shardFileMeta.FromBytes(metaBytes)
	log.Infof("Metadata: %v", this.shardFileMeta)
}
