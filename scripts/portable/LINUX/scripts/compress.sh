#! /usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$DIR/../"

cd "$ROOT/.."
echo "Compressing files, please wait..."
tar --exclude='LINUX/scripts' --transform "s/LINUX/mrsm/" -czvf mrsm.tar.gz LINUX
mv mrsm.tar.gz "$ROOT/scripts/"
echo "Created mrsm.tar.gz"
