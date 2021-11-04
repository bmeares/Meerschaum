#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$DIR/../"

cd "$ROOT"

if [ ! -d "./scripts/_site-packages_original" ]; then
  echo "Backing up site-packages..."
  TERMINFO_DIRS=/usr/share/terminfo \
  ./python/bin/python3.9 -m pip install wheel --no-warn-script-location -q
  cp -r ./python/lib/python3.9/site-packages ./scripts/_site-packages_original
fi

echo "Installing Meerschaum and dependencies (this might take awhile)..."
MRSM_ROOT_DIR="$ROOT"/root/ \
TERMINFO_DIRS=/usr/share/terminfo \
./python/bin/python3.9 -m pip install "$DIR"/install[full] --no-warn-script-location -q

echo Finished installing.
