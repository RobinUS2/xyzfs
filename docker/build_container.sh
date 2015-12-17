#!/bin/bash

# Build binary for docker
../build.sh docker

# Copy binary into build context
cp ../xyzfs .

# Build container
docker build --tag xyzfs .

# Cleanup
rm -f xyzfs
