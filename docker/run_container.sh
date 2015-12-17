#!/bin/bash
docker run --name xyzfs \
	-d \
	--restart=always \
	--memory="4G" \
	-p 8080:8080 \
	-p 3322:3322 \
	-p 3323:3323 \
	-p 3324:3324 \
	xyzfs
