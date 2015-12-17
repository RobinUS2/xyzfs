#!/bin/bash
docker run --name xyzfs \
	-d \
	--restart=always \
	--memory="4G" \
	-p 8080:8080 \
	xyzfs
