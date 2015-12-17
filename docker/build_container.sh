#!/bin/bash

# Copy binary into build context
cp ../xyzfs .

# Build container
docker build .

# Cleanup
rm -f xyzfs
