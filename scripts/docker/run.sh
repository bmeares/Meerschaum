#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/../config.sh
cd "$PARENT"

docker run --rm -it \
  -v "$PARENT"/meerschaum:/home/meerschaum/.local/lib/python3.9/site-packages/meerschaum \
  bmeares/meerschaum:latest \
  $@
