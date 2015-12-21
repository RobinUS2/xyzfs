#!/bin/bash

# Build
source ./build.sh race

# Run tests
go get golang.org/x/tools/cmd/cover
go test -cover -bench -v .

# Run vet
go vet .
