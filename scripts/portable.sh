#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

mkdir -p "$PARENT/portable"
cd "$PARENT/portable"

declare -A urls
urls["WINDOWS"]="https://github.com/indygreg/python-build-standalone/releases/download/20210103/cpython-3.9.1-x86_64-pc-windows-msvc-shared-pgo-20210103T1125.tar.zst"
urls["DEBIAN"]="https://github.com/indygreg/python-build-standalone/releases/download/20210103/cpython-3.9.1-x86_64-unknown-linux-gnu-pgo-20210103T1125.tar.zst"
urls["MACOS"]="https://github.com/indygreg/python-build-standalone/releases/download/20210103/cpython-3.9.1-x86_64-apple-darwin-pgo-20210103T1125.tar.zst"

declare -A tars
tars["WINDOWS"]="windows.tar.zst"
tars["DEBIAN"]="debian.tar.zst"
tars["MACOS"]="macos.tar.gz"

for os in "${!urls[@]}"; do
  if [ ! -f "${tars[$key]}" ]; then
    echo "${tars[$key]}"
    wget -O "${tars[$key]}" "${urls[$key]}"
    tar -I zstd -xvf "${tars[$key]}"
  fi
done
