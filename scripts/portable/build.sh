#! /usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/../config.sh
shopt -s extglob

mkdir -p "$PARENT/portable"
cd "$PARENT/portable"
mkdir -p "cache"

declare -a supported_systems=("WINDOWS" "LINUX" "MACOS")
declare -a systems=()
if [ ! -z "$1" ]; then
  for a in "$@"; do
    if [[ ${supported_systems[*]} =~ "${a}" ]]; then
      systems+=("$a")
    else
      echo "Unsupported system '$a'. Skipping..."
    fi
  done
else
  systems=("${supported_systems[@]}")
fi

declare -A urls
urls["WINDOWS"]="https://github.com/indygreg/python-build-standalone/releases/download/20230507/cpython-3.11.3+20230507-x86_64-pc-windows-msvc-shared-pgo-full.tar.zst"
urls["LINUX"]="https://github.com/indygreg/python-build-standalone/releases/download/20230507/cpython-3.11.3+20230507-x86_64-unknown-linux-gnu-pgo+lto-full.tar.zst"
urls["MACOS"]="https://github.com/indygreg/python-build-standalone/releases/download/20230507/cpython-3.11.3+20230507-x86_64-apple-darwin-pgo+lto-full.tar.zst"
urls["get-pip.py"]="https://bootstrap.pypa.io/get-pip.py"

declare -A tars
tars["WINDOWS"]="windows.tar.zst"
tars["LINUX"]="debian.tar.zst"
tars["MACOS"]="macos.tar.gz"

declare -A taropts
taropts["WINDOWS"]="-I zstd -xf "
taropts["LINUX"]="-I zstd -xf "
taropts["MACOS"]="-I zstd -xf "

declare -A sites
sites["WINDOWS"]="Lib/site-packages"
sites["LINUX"]="lib/python3.11/site-packages"
sites["MACOS"]="lib/python3.11/site-packages"

### Check if zstd is installed.
v=$(zstd -V)
rc="$?"
if [ "$rc" != 0 ]; then
  echo "zstd is not installed. Please install zstd and restart."
  exit 1
fi
g=$(tar --version | grep "GNU")
rc="$?"
if [ -z "$g" ] || [ "$rc" != 0 ]; then
  echo "GNU tar is not installed. Please install GNU tar and restart."
  exit 1
fi

### Download archives.
for os in "${systems[@]}"; do
  if [ ! -f "cache/${tars[$os]}" ]; then
    wget -O "cache/${tars[$os]}" "${urls[$os]}"
  fi
done
if [ ! -f "cache/get-pip.py" ]; then
  wget -O "cache/get-pip.py" "${urls["get-pip.py"]}"
fi

### Extract the files.
for os in "${systems[@]}"; do
  rm -rf "${os}"
  mkdir -p "${os}"
  mkdir -p "${os}/root"
  tar ${taropts[$os]} "cache/${tars[$os]}" -C "${os}" || exit 1
  cd "${os}/python"
  rm -rf !(install) && mv install ../
  cd ../
  rm -rf python && mv install python
  cd ../
done

for os in "${systems[@]}"; do
  cp -r "$PARENT/scripts/portable/${os}/"* "${os}"
  mkdir -p "${os}/scripts/install"
  cp -r "$PARENT/meerschaum" "$PARENT/setup.py" "$PARENT/README.md" "${os}/scripts/install"
  cp -r "$PARENT/meerschaum" "${os}/python/${sites[$os]}"
done

