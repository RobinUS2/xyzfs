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

	// Re-usable byte array
	var b []byte = nil

	// Global read lock
	this.contentsMux.Lock()

	// Actual file contents
	if this.contents != nil {
		b := this.contents.Bytes()
		this.shardMeta.SetContentsLength(uint32(len(b)))
		buf.Write(b)
		b = nil
	} else {
		log.Infof("Converting empty shard %s to bytes", this.IdStr())
		this.shardMeta.SetContentsLength(0)
	}

	// File meta
	b = this.shardFileMeta.Bytes()
	this.shardMeta.SetFileMetaLength(uint32(len(b)))
	buf.Write(this.shardFileMeta.Bytes())
	b = nil

	// File index
	b = this.shardIndex.Bytes()
	this.shardMeta.SetIndexLength(uint32(len(b)))
	buf.Write(b)
	b = nil

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
	this.shardMeta = newShardMeta()
	this.shardMeta.FromBytes(metaBytes)
	if this.shardMeta.MetaVersion < 1 {
		panic("Failed to read metadata, could not find version")
	}
	log.Infof("Metadata: %v", this.shardMeta)
}
