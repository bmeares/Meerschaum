#! /usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$DIR/../"

cd "$DIR"

echo "About to create a minimal Meerschaum portable archive."
./uninstall.sh
./compress.sh
mv mrsm.tar.gz mrsm-minimal-linux.tar.gz
echo "Created file mrsm-minimal-linux.tar.gz"

