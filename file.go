package main

import (
	"bytes"
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"hash/crc32"
)

// Virtual file representation

type FileMeta struct {
	Id          []byte // Random uuid
	FullName    string // virtual path, folder/directory + filename (e.g. /images/robin/profile.JPG)
	Created     uint32 // Unix timestamp
	Size        uint32 // Length of file in bytes
	StartOffset uint32 // Offset in bytes to start reading contents
	Checksum    uint32 // Crc 32 (Castagnoli)
}

// Serialize to bytes
func (this *FileMeta) Bytes() []byte {
	buf := new(bytes.Buffer)
	buf.Write(this.Id)                                                      // ID, 16 bytes
	binary.Write(buf, binary.BigEndian, uint32(len([]byte(this.FullName)))) // Length of name
	buf.Write([]byte(this.FullName))                                        // Name
	binary.Write(buf, binary.BigEndian, this.Created)                       // Created
	binary.Write(buf, binary.BigEndian, this.Size)                          // Size
	binary.Write(buf, binary.BigEndian, this.StartOffset)                   // StartOffset
	binary.Write(buf, binary.BigEndian, this.Checksum)                      // Checksum
	return buf.Bytes()
}

// Recover from bytes
func (this *FileMeta) FromBytes(b []byte) {
	buf := bytes.NewReader(b)
	var err error

	// Id
	idBytes := make([]byte, 16)
	idBytesRead, _ := buf.Read(idBytes)
	if idBytesRead != 16 {
		panic("ID not 16 bytes")
	}

	// Name length
	var nameLen uint32
	err = binary.Read(buf, binary.BigEndian, &nameLen) // Length of name
	panicErr(err)

	// Read actual name
	nameBytes := make([]byte, nameLen)
	nameBytesRead, _ := buf.Read(nameBytes)
	if uint32(nameBytesRead) != nameLen {
		panic("Name bytes read mismatch")
	}
	this.FullName = string(nameBytes)

	// Created
	err = binary.Read(buf, binary.BigEndian, &this.Created)
	panicErr(err)

	// Size
	err = binary.Read(buf, binary.BigEndian, &this.Size)
	panicErr(err)

	// StartOffset
	err = binary.Read(buf, binary.BigEndian, &this.StartOffset)
	panicErr(err)

	// Checksum
	err = binary.Read(buf, binary.BigEndian, &this.Checksum)
	panicErr(err)

}

// Update contents from data of the file (e.g. length)
func (this *FileMeta) UpdateFromData(b []byte) {
	// Size
	this.Size = uint32(len(b))
	if this.Size > uint32(conf.MaxFileSize) {
		panic("Exceeds maximum file size")
	}

	// File meta checksum
	this.Checksum = crc32.Checksum(b, crcTable)
}

// Get murmur hash
func (this *FileMeta) GetHash() uint64 {
	return murmur3.Sum64([]byte(this.FullName))
}

// New file meta
func newFileMeta(name string) *FileMeta {
	return &FileMeta{
		Id:          randomUuid(),
		FullName:    name,
		Created:     unixTsUint32(),
		Size:        0, // auto-calculated on write
		StartOffset: 0, // auto-calculated on write
		Checksum:    0, // auto-calculated on write
	}
}
