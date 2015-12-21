#!/bin/bash
# Working directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "Building in $DIR"
cd $DIR

# Set go path
export GOPATH=`pwd`

# Fetch dependencies
go get "github.com/satori/go.uuid"
go get "github.com/klauspost/reedsolomon"
go get "github.com/Sirupsen/logrus"
go get "github.com/willf/bloom"
go get "github.com/spaolacci/murmur3"
go get "github.com/julienschmidt/httprouter"
go get "github.com/RobinUS2/golang-jresp"

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
