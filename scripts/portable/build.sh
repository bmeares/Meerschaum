#! /usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/../config.sh

mkdir -p "$PARENT/portable"
cd "$PARENT/portable"
mkdir -p "cache"

declare -A urls
urls["WINDOWS"]="https://github.com/indygreg/python-build-standalone/releases/download/20210103/cpython-3.9.1-x86_64-pc-windows-msvc-shared-pgo-20210103T1125.tar.zst"
urls["LINUX"]="https://github.com/indygreg/python-build-standalone/releases/download/20210103/cpython-3.9.1-x86_64-unknown-linux-gnu-pgo-20210103T1125.tar.zst"
urls["MACOS"]="https://github.com/indygreg/python-build-standalone/releases/download/20210103/cpython-3.9.1-x86_64-apple-darwin-pgo-20210103T1125.tar.zst"

declare -A tars
tars["WINDOWS"]="windows.tar.zst"
tars["LINUX"]="debian.tar.zst"
tars["MACOS"]="macos.tar.gz"

declare -A taropts
taropts["WINDOWS"]="-I zstd -xvf"
taropts["LINUX"]="-I zstd -xvf"
taropts["MACOS"]="-xvzf"

declare -A sites
sites["WINDOWS"]="Lib/site-packages"
sites["LINUX"]="lib/python3.9/site-packages"
sites["MACOS"]="lib/python3.9/site-packages"

### Check if zstd is installed.
v=$(zstd -V)
if [ "$?" != 0 ]; then
  echo "zstd is not installed. Please install zstd and restart."
  exit 1
fi

### Download archives.
for os in "${!urls[@]}"; do
  if [ ! -f "cache/${tars[$os]}" ]; then
    wget -O "cache/${tars[$os]}" "${urls[$os]}"
  fi
done

### Download and extract the files
for os in "${!tars[@]}"; do
  rm -rf "${os}"
  mkdir -p "${os}"
  mkdir -p "${os}/root"
  tar "${taropts[$os]}" "cache/${tars[$os]}" -C "${os}"
  cd "${os}/python"
  rm -rf $(ls | grep -v install) && mv install ../
  cd ../
  rm -rf python && mv install python
  cd ../
done

for os in "${!sites[@]}"; do
  cp -r "$PARENT/scripts/portable/${os}/"* "${os}"
  mkdir -p "${os}/scripts/install"
  cp -r "$PARENT/meerschaum" "$PARENT/setup.py" "$PARENT/README.md" "${os}/scripts/install"
  cp -r "$PARENT/meerschaum" "${os}/python/${sites[$os]}"
done

