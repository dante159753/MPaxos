#!/usr/bin/env bash

PID_FILE=server.pid

./server -log_dir=. -log_level=info -id $ID -algorithm $1 -fz=0 &
echo $! >> ${PID_FILE}
