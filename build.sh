#! /bin/bash

. config.sh

docker build . -t "$image"
