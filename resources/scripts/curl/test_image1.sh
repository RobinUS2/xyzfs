#!/bin/bash
TS=`date +%s`
NAME="/image1_$TS.jpg"
FILE="../../testdata/image1.jpg"
echo "Creating '$NAME' with data '$FILE'"
curl -XPOST -i -F filedata=@$FILE http://localhost:8080/v1/file?filename=$NAME
echo "Reading back file"
sleep 1
curl -v http://localhost:8080/v1/file?filename=$NAME > /dev/null
echo ""
