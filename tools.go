package main

import (
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
