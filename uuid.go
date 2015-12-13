package main

import (
	"fmt"
	"github.com/satori/go.uuid"
)

func randomUuid() []byte {
	return uuid.NewV4().Bytes()
}

func uuidToString(b []byte) string {
	u := uuid.FromBytesOrNil(b)
	if u == uuid.Nil {
		panic(fmt.Sprintf("Invalid uuid %v", b))
	}
	return u.String()
}
