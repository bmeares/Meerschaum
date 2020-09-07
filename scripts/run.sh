#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

### run temporary container and mount src/ to /mnt/
docker run -it --rm -v "$src":/mnt "$image" "$@"
