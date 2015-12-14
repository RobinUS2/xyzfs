package main

// Byte writer to a shard

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
	this.fileMetaMux.Lock()
	if this.fileMeta == nil {
		this.fileMeta = make([]*FileMeta, 0)
	}
	this.fileMeta = append(this.fileMeta, f)
	this.fileMetaMux.Unlock()

	// Log
	log.Infof("Created file %s with size %d", f.FullName, f.Size)

	// Done
	return f, nil
}
