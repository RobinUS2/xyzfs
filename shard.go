package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
)

// Shard is a partial piece of data in a block
// @todo Store CRC32 checksum in meta file to validate content of shard

type Shard struct {
	Id         []byte // Unique UUID
	BlockIndex uint   // Numeric index (from to 0 to number_of_data_shards+number_of_parity_shards), used to align the shards before a restore using Reed Solomon
	Parity     bool   // Is this real data or parity?

	// Block reference
	block *Block

	// Buffer mode
	bufferMode    ShardBufferMode
	bufferModeMux sync.RWMutex

	// Byte buffers, file contents only
	contents       *bytes.Buffer // Actual byte buffer in-memory, is lazy loaded, use Contents() method to get
	contentsMux    sync.RWMutex
	contentsOffset uint32

	// File metadata, recovered from byte buffers on disk
	shardFileMeta *ShardFileMeta

	// Shard meta
	shardMeta *ShardMeta

	// Shard index
	shardIndex *ShardIndex

	// Is loaded?
	isLoaded    bool
	isLoadedMux sync.RWMutex
}

// Buffer mode
type ShardBufferMode uint

const (
	IdleShardBufferMode  ShardBufferMode = iota // 0 = not doing anything
	ReadShardBufferMode                         // 1 = initial reading the shard from the bytes on disk into memory structures
	WriteShardBufferMode                        // 2 = writing into the shard
	FlushShardBufferMode                        // 3 = final process of writing the shard to bytes for the disk
	ReadyShardBufferMode                        // 4 = this is what an in-memory version looks like that is used for random access
)

// Read contents
// - locking should be done in the buffer itself
func (this *Shard) Contents() *bytes.Buffer {
	// @todo read from disk
	if this.contents == nil {
		this.contents = bytes.NewBuffer(make([]byte, 0))
	}
	return this.contents
}

// Persist
func (this *Shard) Persist() {
	// Make sure block folder is prepared
	this.Block().PrepareFolder()

	// @todo better writing to files, guaranteeing no data corruption
	path := this.FullPath()
	log.Infof("Persisting shard %s to disk in %s", this.IdStr(), path)
	err := ioutil.WriteFile(path, this._toBinaryFormat(), conf.UnixFilePermissions)
	if err != nil {
		// @tood handle better
		panic(err)
	}

	// Send full index over wire
	// @todo Remove, this is should be covered by the distributed index mutations
	binaryTransport._broadcastShardIndex(this)
}

// Load from disk
func (this *Shard) Load() {
	this.isLoadedMux.Lock()
	defer this.isLoadedMux.Unlock()

	// Already loaded
	if this.isLoaded {
		// Yes
		return
	}

	// Load
	log.Infof("Loading shard %s from disk in %s", this.IdStr(), this.FullPath())
	res, e := this._fromBinaryFormat()
	if e != nil || !res {
		log.Errorf("Failed to load shard %s from disk in %s: %s", this.IdStr(), this.FullPath(), e)
		// Error
		return
	}

	// Done
	this.isLoaded = true
}

// Full path
func (this *Shard) FullPath() string {
	var infix string = ""
	if this.Parity {
		infix = ".parity"
	}
	return fmt.Sprintf("%s/s_%s%s.data", this.Block().FullPath(), this.IdStr(), infix)
}

// Block
func (this *Shard) Block() *Block {
	return this.block
}

// To string
func (this *Shard) IdStr() string {
	return uuidToString(this.Id)
}

// Set contents
func (this *Shard) SetContents(b *bytes.Buffer) {
	this.contentsMux.Lock()
	this.contents = b
	this.contentsMux.Unlock()
}

// Read file bytes
func (this *Shard) ReadFile(filename string) ([]byte, error) {
	// @todo support reading from this.Contents() in-memory buffer (E.g. during writes on this shard)
	this.contentsMux.RLock()
	if this.contents != nil {
		panic("Can not read file on shard that has data in-memory")
	}
	this.contentsMux.RUnlock()

	// Get meta
	meta := this.shardFileMeta.GetByName(filename)

	// Meta found?
	if meta == nil {
		return nil, errors.New("File not found")
	}

	// Open file
	f, err := this._openFile()
	defer f.Close()
	if err != nil {
		return nil, err
	}

	// Seek to start of file
	f.Seek(int64(meta.StartOffset), 0)

	// New reader
	buf := bufio.NewReader(f)

	// Buffer
	fileBytes := make([]byte, int(meta.Size))

	// Read
	fileBytesRead, readE := buf.Read(fileBytes)
	if readE != nil {
		return nil, readE
	}

	// Validate size and read
	if int(meta.Size) != fileBytesRead {
		return nil, errors.New("File bytes read mismatch")
	}

	return fileBytes, nil
}

// Add file
func (this *Shard) AddFile(f *FileMeta, b []byte) (*FileMeta, error) {
	// Validate mode
	this.bufferModeMux.Lock()
	if this.bufferMode == ReadShardBufferMode {
		// Reading..
		panic("Shard is reading, can not write")
	}
	this.bufferMode = WriteShardBufferMode
	this.bufferModeMux.Unlock()

	// Calculate length
	fileLen := uint32(len(b))

	// Acquire write lock
	this.contentsMux.Lock()

	// Update metadata
	f.Size = fileLen
	f.StartOffset = this.contentsOffset

	// Write contents to buffer
	this.Contents().Write(b)

	// Update content offset
	this.contentsOffset += f.Size

	// Unlock write
	this.contentsMux.Unlock()

	// Append meta
	this.shardFileMeta.Add(f)

	// Update metadata
	this.shardMeta.mux.Lock()
	this.shardMeta.FileCount++
	newCount := this.shardMeta.FileCount
	this.shardMeta.mux.Unlock()

	// Update index
	this.shardIndex.Add(f.FullName)

	// @todo Write to other replicate nodes
	// @todo Write shard index change (effectively add file to bloom filter) to all nodes (UDP)

	// Log
	log.Infof("Created file %s with size %d in shard %s, now contains %d file(s)", f.FullName, f.Size, this.IdStr(), newCount)

	// Done
	return f, nil
}

func newShard(b *Block) *Shard {
	id := randomUuid()
	return &Shard{
		contents:      nil,
		Id:            id,
		block:         b,
		Parity:        false,
		shardMeta:     newShardMeta(),
		shardIndex:    newShardIndex(id),
		shardFileMeta: newShardFileMeta(),
	}
}
