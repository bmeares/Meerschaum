#! /bin/bash

. config.sh

docker run --rm -v "$src":/mnt "$image" "$@"
