package main

import (
	"crypto/md5"
	"fmt"
	"testing"
)

func TestNewBlock(t *testing.T) {
	startApplication()

	// Register on volume
	b := datastore.NewBlock()

	// Persist
	b.Persist()

	// Calculate hashes
	preHash := make([]string, 10)
	for i, ds := range b.DataShards {
		h := md5.New()
		h.Write(ds.Contents().Bytes())
		preHash[i] = fmt.Sprintf("%x", h.Sum(nil))
	}

	// Encode
	b.ErasureEncoding()

	// Validate hashes after are equal, meaning the data has not changed
	for i, ds := range b.DataShards {
		h := md5.New()
		h.Write(ds.Contents().Bytes())
		if preHash[i] != fmt.Sprintf("%x", h.Sum(nil)) {
			panic("Erasure encoding has changed file contents of data partition")
		}
	}

	// Persist with encoding
	b.Persist()
}
