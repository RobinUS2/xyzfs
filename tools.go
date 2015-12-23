package main

import (
	"time"
)

func panicErr(e error) {
	if e != nil {
		panic(e)
	}
}

func panicNil(x interface{}) {
	if x == nil {
		panic("Unexpected nil")
	}
}

func unixTsUint32() uint32 {
	return uint32(time.Now().Unix())
}
