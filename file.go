package main

// Virtual file representation

type FileMeta struct {
	Id   []byte // Random uuid
	Name string // virtual base name
	Path string // virtual path, folder/directory
}
