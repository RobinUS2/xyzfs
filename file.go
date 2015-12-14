package main

// Virtual file representation

type FileMeta struct {
	Id       []byte // Random uuid
	FullName string // virtual path, folder/directory + filename (e.g. /images/robin/profile.JPG)
	Created  uint32 // Unix timestamp
	Size     uint32 // Length of file in bytes
}
