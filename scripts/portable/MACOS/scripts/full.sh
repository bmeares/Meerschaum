#! /usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$DIR/../"

cd "$DIR"

echo "About to create a full Meerschaum portable archive."
./install.sh
./compress.sh
./uninstall.sh
mv mrsm.tar.gz mrsm-full-macos.tar.gz
echo "Created file mrsm-full-macos.tar.gz."

