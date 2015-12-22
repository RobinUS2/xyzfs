package main

import (
	"fmt"
	"testing"
)

func TestNewFileHash(t *testing.T) {
	f := newFileMeta("test.txt")
	if fmt.Sprintf("%d", f.GetHash()) != "751898125953766072" {
		t.Error("File hash invalid")
	}

	// Test serialisation
	f.StartOffset = 111
	f.Size = 222
	f.Checksum = 333

	// To bytes
	b := f.Bytes()
	if len(b) < 1 {
		t.Error("Bytes should be non-empty")
	}

	// Read
	f2 := &FileMeta{}
	f2.FromBytes(b)
	if f2.FullName != "test.txt" {
		t.Error("Failed name check")
	}
	if f2.StartOffset != 111 {
		t.Error("Failed start offset")
	}
	if f2.Size != 222 {
		t.Error("Failed size")
	}
	if f2.Checksum != 333 {
		t.Error("Failed checksum")
	}
}
