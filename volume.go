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

func (this *Volume) IdStr() string {
	return uuidToString(this.Id)
}

// Prepare
func (this *Volume) prepare() {
	// Folder exists?
	log.Debugf("Preparing volume %s", this.IdStr())
	if _, err := os.Stat(this.FullPath()); os.IsNotExist(err) {
		// Create
		log.Infof("Creating folder for volume %s in %s", this.IdStr(), this.FullPath())
		e := os.MkdirAll(this.FullPath(), conf.UnixFolderPermissions)
		if e != nil {
			log.Errorf("Failed to create %s: %s", this.FullPath(), e)
		}
	}

	// Done
	log.Infof("Loaded volume %s", this.IdStr())
}

// Full path
func (this *Volume) FullPath() string {
	return fmt.Sprintf("%s/v=%s", this.Path, this.IdStr())
}
