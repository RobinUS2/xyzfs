package main

import (
	"fmt"
	"os"
)

// Volume is a location on a server that points to a local data storage directory

type Volume struct {
	Id   []byte // Unique ID
	Path string // Path to local directory
}

// Prepare
func (this *Volume) prepare() {
	// Folder exists?
	log.Debug("Preparing volume %s", this.Id)
	if _, err := os.Stat(this.FullPath()); os.IsNotExist(err) {
		// @todo create
	}
}

// Full path
func (this *Volume) FullPath() string {
	return fmt.Sprintf("%s/v=%s", this.Path, this.Id)
}
