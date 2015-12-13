#!/bin/bash
export GOPATH=`pwd`

go get "github.com/satori/go.uuid"
go get "github.com/klauspost/reedsolomon"
go get "github.com/Sirupsen/logrus"

go build "$@" .
