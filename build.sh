#!/bin/bash
export GOPATH=`pwd`

go get "github.com/satori/go.uuid"
go get "github.com/klauspost/reedsolomon"
go get "github.com/Sirupsen/logrus"
go get "github.com/willf/bloom"

case "$1" in
	docker)
		# Cross compile (use brew on OSX: brew install go --with-cc-common )
		env GOOS=linux GOARCH=386 go build .
	;;
	race)
		go build -race .
	;;
	*)
		go build .
	;;
esac
