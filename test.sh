#!/bin/bash

# Run vet
go vet .
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi

# Build
source ./build.sh race
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi

# Test deps
go get golang.org/x/tools/cmd/cover

# Run tests and benchmark
go test -cover -bench -v .
rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
