#!/bin/bash
./build_container.sh
docker rm -f xyzfs
./run_container.sh
docker logs -f xyzfs
