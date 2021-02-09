#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

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

declare -A sites
sites["WINDOWS"]="Lib/site-packages"
sites["LINUX"]="lib/python3.9/site-packages"
sites["MACOS"]="lib/python3.9/site-packages"

declare -A roots
roots["WINDOWS"]="root"
roots["LINUX"]="root"
roots["MACOS"]="root"

declare -A launchers
launchers["WINDOWS_file"]="mrsm.bat"
launchers["WINDOWS_text"]="@ECHO OFF
SET MRSM_ROOT_DIR=${roots["WINDOWS"]}
SET MRSM_RUNTIME=portable
python\python.exe -m meerschaum %*"
launchers["LINUX_file"]="mrsm"
launchers["LINUX_text"]='#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
TERMINFO_DIRS=/etc/terminfo:/lib/terminfo:/usr/share/terminfo \
MRSM_ROOT_DIR="$DIR"/'"${roots["LINUX"]}"' \
MRSM_RUNTIME=portable \
"$DIR"/python/bin/python3.9 -m meerschaum $@'
launchers["MACOS_file"]="mrsm"
launchers["MACOS_text"]='#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
TERMINFO_DIRS=/usr/share/terminfo \
MRSM_ROOT_DIR="$DIR"/'"${roots["MACOS"]}"' \
MRSM_RUNTIME=portable \
"$DIR"/python/bin/python3.9 -m meerschaum $@'

declare -A installers
installers["WINDOWS_file"]="install.bat"
installers["WINDOWS_text"]="@ECHO OFF
cd ..
python\python.exe -m pip install scripts/install[full] --no-warn-script-location
pause"
installers["LINUX_file"]="install.sh"
installers["LINUX_text"]='#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
TERMINFO_DIRS=/etc/terminfo:/lib/terminfo:/usr/share/terminfo \
"$DIR"/../python/bin/python3.9 -m pip install "$DIR"/install[full] --no-warn-script-location'
installers["MACOS_file"]="install.sh"
installers["MACOS_text"]='#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
TERMINFO_DIRS=/usr/share/terminfo \
"$DIR"/../python/bin/python3.9 -m pip install "$DIR"/install[full] --no-warn-script-location'


### Check if zstd is installed.
v=$(zstd -V)
if [ "$?" != 0 ]; then
  echo "Installing zstd..."
  sudo apt-get update && sudo apt-get install zstd -y
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
  mkdir -p "${os}/${roots[$os]}"
  tar -axvf "cache/${tars[$os]}" -C "${os}"
  cd "${os}/python"
  rm -rf $(ls | grep -v install) && mv install ../
  cd ../
  rm -rf python && mv install python
  cd ../
done

for os in "${!sites[@]}"; do
  mkdir -p "${os}/scripts"
  mkdir -p "${os}/scripts/install"
  cp -r "$PARENT/meerschaum" "${os}/scripts/install"
  cp "$PARENT/setup.py" "${os}/scripts/install"
  touch "${os}/scripts/install/README.md"
  cp -r "$PARENT/meerschaum" "${os}/python/${sites[$os]}"
  file="${launchers["$os""_file"]}"
  text="${launchers["$os""_text"]}"
  echo "$text" > "${os}/$file"
  chmod +x "${os}/$file"

  file="${installers["$os""_file"]}"
  text="${installers["$os""_text"]}"
  echo "$text" > "${os}/scripts/$file"
  chmod +x "${os}/scripts/$file"

done

