#!/bin/bash

# Build
source build.sh -race

# Run tests
go test .

# Run vet
go vet .
