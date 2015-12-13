package main

import (
	"github.com/satori/go.uuid"
)

func randomUuid() []byte {
	return uuid.NewV4().Bytes()
}
