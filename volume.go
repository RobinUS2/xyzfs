package main

// Volume is a location on a server that points to a local data storage directory

type Volume struct {
	Id   []byte // Unique ID
	Path string // Path to local directory
}
