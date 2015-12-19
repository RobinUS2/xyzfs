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
}
