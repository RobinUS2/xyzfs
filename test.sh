#!/bin/bash

# Build
source build.sh -race

# Run tests
go test -v .

# Run vet
go vet .
