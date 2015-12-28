#!/bin/bash
TS=`date +%s`
NAME="/test_$TS.txt"
DATA="Current epoch $TS"
echo "Creating '$NAME' with data '$DATA'"
curl -XPOST --data "'""$DATA""'" http://localhost:8080/v1/file?filename=$NAME
echo "Reading back file"
curl http://localhost:8080/v1/file?filename=$NAME
echo ""
