#!/bin/bash

# Build
source ./build.sh race

# Run tests
go test -cover -v .

# Run vet
go vet .
