package main

// Byte writer to a shard

func (this *Shard) AddFile(f *FileMeta, b []byte) {
	// Validate mode
	this.bufferModeMux.Lock()
	if this.bufferMode == ReadShardBufferMode {
		// Reading..
		panic("Shard is reading, can not write")
	}
	this.bufferMode = WriteShardBufferMode
	this.bufferModeMux.Unlock()

	// Append meta
	this.fileMetaMux.Lock()
	if this.fileMeta == nil {
		this.fileMeta = make([]*FileMeta, 0)
	}
	this.fileMeta = append(this.fileMeta, f)
	this.fileMetaMux.Unlock()

	// Write contents to buffer
}
