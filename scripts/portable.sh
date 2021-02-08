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

### Check if zstd is installed.
v=$(zstd -V)
if [ "$?" != 0 ]; then
  echo "Installing zstd..."
  sudo apt-get update && sudo apt-get install zstd -y
fi
h=$(clang --help)
if [ $? != 0 ]; then
  echo "Installing clang..."
  sudo apt-get install clang -y
fi

shopt -s extglob

### Download and extract the files
for os in "${!urls[@]}"; do
  if [ ! -f "${tars[$os]}" ]; then
    mkdir -p "${os}"
    cd "${os}"
    wget -O "${tars[$os]}" "${urls[$os]}"
    tar -axvf "${tars[$os]}"
    cd python
    rm -rf $(ls | grep -v install) && mv install ..
    cd ..
    rm -rf python && mv install python
    cd ..
  fi
done

cd "$PARENT"
# cp -r 

### Install Meerschaum into each directory
