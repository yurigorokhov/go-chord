#!/bin/sh

EXE=$1

for i in 2 4 8 16 32 64 128 256; do
    echo "[RUN] cache=false nodes=$i"
    $EXE -numnodes=$i -tcpdelay=20 -faketcp=true -usecache=false

    echo "[RUN] cache=false nodes=$i"
    $EXE -numnodes=$i -tcpdelay=20 -faketcp=true -usecache=true
done
