#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$DIR/../"

cd "$DIR"

echo "Will create mrsm-minimal-linux.tar.gz and mrsm-full-linux.tar.gz."
echo "Please run this after generating the files from Unix with scripts/portable/build.sh."

./minimal.sh
./full.sh

echo "Finished creating archives"

