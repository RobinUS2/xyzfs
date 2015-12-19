package main

// Virtual file representation

type FileMeta struct {
	Id          []byte // Random uuid
	FullName    string // virtual path, folder/directory + filename (e.g. /images/robin/profile.JPG)
	Created     uint32 // Unix timestamp
	Size        uint32 // Length of file in bytes
	StartOffset uint32 // Offset in bytes to start reading contents
}

// New file meta
func newFileMeta(name string) *FileMeta {
	return &FileMeta{
		Id:          randomUuid(),
		FullName:    name,
		Created:     unixTsUint32(),
		Size:        0, // auto-calculated on write
		StartOffset: 0, // auto-calculated on write
	}
}
