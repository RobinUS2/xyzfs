package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"sync"
)

// Shard is a partial piece of data in a block

// CRC table
var crcTable *crc32.Table = crc32.MakeTable(crc32.Castagnoli)

// Shard object
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

	// Allocated capacity, this is used to acquire data in a shard to write data (a file) to
	allocationMux       sync.RWMutex
	allocatedBytesCount uint32

	// File metadata, recovered from byte buffers on disk
	shardFileMeta *ShardFileMeta

	// Shard meta
	shardMeta *ShardMeta

	// Shard index
	shardIndex *ShardIndex

	// Is loaded?
	isLoaded    bool
	isLoadedMux sync.RWMutex

	// Is flushed? (is this written to disk?)
	isFlushed    bool
	isFlushedMux sync.RWMutex
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

// Contains file?
func (this *Shard) TestContainsFile(fullName string) bool {
	return this.ShardIndex().Test(fullName)
}

// Get shard index
func (this *Shard) ShardIndex() *ShardIndex {
	// Lazy load?
	this.Load()

	// Return
	return this.shardIndex
}

// Get shard meta
func (this *Shard) ShardMeta() *ShardMeta {
	// Lazy load?
	this.Load()

	// Return
	return this.shardMeta
}

// Get shard meta
func (this *Shard) ShardFileMeta() *ShardFileMeta {
	// Lazy load?
	this.Load()

	// Return
	return this.shardFileMeta
}

// Allocate space, returns true if
func (this *Shard) AllocateCapacity(n uint32) bool {
	// Lock
	this.allocationMux.Lock()
	defer this.allocationMux.Unlock()

	// Available
	available := uint32(conf.ShardSizeInBytes) - this.contentsOffset

	// Does it fit?
	if available >= n {
		// Allocate
		this.allocatedBytesCount += n

		// :)
		return true
	}
	// Nope..
	return false
}

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
	// Only persist if this one is dirty / not-flushed before
	this.isFlushedMux.Lock()
	defer this.isFlushedMux.Unlock()
	if this.isFlushed {
		log.Debugf("Not persisting shard %s as this is already flushed")
		return
	}

	// Make sure is loaded
	this.Load()

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

	// Done
	this.isFlushed = true
}

// Reset loaded
func (this *Shard) ResetLoaded() {
	this.isLoadedMux.Lock()
	this.isLoaded = false
	this.isLoadedMux.Unlock()
}

// Load from disk
func (this *Shard) Load() (bool, error) {
	this.isLoadedMux.Lock()
	defer this.isLoadedMux.Unlock()

	// Already loaded
	if this.isLoaded {
		// Yes
		return true, nil
	}

	// Load
	log.Infof("Loading shard %s from disk in %s", this.IdStr(), this.FullPath())
	res, e := this._fromBinaryFormat()
	if e != nil || !res {
		log.Errorf("Failed to load shard %s from disk in %s: %s", this.IdStr(), this.FullPath(), e)
		// Error
		return false, e
	}

	// Done
	this.isLoaded = true
	return true, nil
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
// file bytes - read error - from memory
func (this *Shard) ReadFile(filename string) ([]byte, error, bool) {
	// Only on data shards
	if this.Parity {
		panic("Can not read file directly for parity shard")
	}

	// Get meta
	meta := this.ShardFileMeta().GetByName(filename)

	// Meta found?
	if meta == nil {
		return nil, errors.New("File not found"), false
	}

	// Support reading from this.Contents() in-memory buffer (E.g. during writes on this shard)
	// log.Infof("Contents on read file %v", this.contents)
	this.contentsMux.RLock()
	if this.contents != nil && uint32(this.contents.Len()) >= meta.StartOffset+meta.Size {
		defer this.contentsMux.RUnlock()
		// log.Infof("Reading file at %d until %d from %s", meta.StartOffset, meta.StartOffset+meta.Size, this.IdStr())
		return this.contents.Bytes()[meta.StartOffset : meta.StartOffset+meta.Size], nil, true
	}
	this.contentsMux.RUnlock()

	// Open file on disk for random read
	f, err := this._openFile()
	defer f.Close()
	if err != nil {
		return nil, err, false
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
		return nil, readE, false
	}

	// Validate size and read
	if int(meta.Size) != fileBytesRead {
		return nil, errors.New("File bytes read mismatch"), false
	}

	// Validate CRC
	readCrc := crc32.Checksum(fileBytes, crcTable)
	if readCrc != meta.Checksum {
		if meta.Size < 1024 {
			log.Errorf("File bytes with wrong CRC: %v %s", fileBytes, string(fileBytes))
		}
		return nil, errors.New(fmt.Sprintf("CRC checksum mismatch, was %d (len %d) expected %d (len %d)", readCrc, len(fileBytes), meta.Checksum, meta.Size)), false
	}

	return fileBytes, nil, false
}

// Add file
func (this *Shard) AddFile(f *FileMeta, b []byte) (*FileMeta, error) {
	// Only on data shards
	if this.Parity {
		panic("Can not add file to parity shard")
	}

	// Validate mode
	this.bufferModeMux.Lock()
	if this.bufferMode == ReadShardBufferMode {
		// Reading..
		panic("Shard is reading, can not write")
	}
	this.bufferMode = WriteShardBufferMode
	this.bufferModeMux.Unlock()

	// Make sure loaded
	this.Load()

	// We should flush again
	this.isFlushedMux.Lock()
	this.isFlushed = false
	this.isFlushedMux.Unlock()

	// Acquire write lock
	this.contentsMux.Lock()

	// Update metadata
	f.UpdateFromData(b)

	// Set start offset in shard
	f.StartOffset = this.contentsOffset
	log.Infof("Create file offset %d", f.StartOffset)

	// Write contents to buffer
	this.Contents().Write(b)

	// Update content offset
	this.contentsOffset += f.Size
	log.Infof("New contents offset %d", this.contentsOffset)

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
	this.ShardIndex().Add(f.FullName)

	// Log
	log.Infof("Created file %s with size %d in shard %s, now contains %d file(s)", f.FullName, f.Size, this.IdStr(), newCount)

	// Broadcast full index
	// @todo Remove this, we should only broadcast the changes
	binaryTransport._broadcastShardIndex(this)

	// Validate file in index
	if this.TestContainsFile(f.FullName) == false {
		panic(fmt.Sprintf("Unable to find file %s in index after creation", f.FullName))
	}

	// Done
	return f, nil
}

func newShard(b *Block) *Shard {
	id := randomUuid()
	return newShardFromId(b, id)
}

func newShardFromId(b *Block, id []byte) *Shard {
	return &Shard{
		contents:      nil,
		Id:            id,
		block:         b,
		Parity:        false,
		shardMeta:     newShardMeta(),
		shardIndex:    newShardIndex(id),
		shardFileMeta: newShardFileMeta(),
		isFlushed:     false,
	}
}
