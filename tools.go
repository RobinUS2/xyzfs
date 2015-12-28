package main

import (
	"fmt"
	"time"
)

func panicErr(e error) {
	if e != nil {
		panic(e)
	}
}

func unixTsUint32() uint32 {
	return uint32(time.Now().Unix())
}

func allocByteArr(size uint32, maxSize uint32) []byte {
	if size > maxSize {
		panic(fmt.Sprintf("Unable to allocate byte array, size %d exceeds maximum of %d", size, maxSize))
	}
	return make([]byte, size)
}
