#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"

root_output_dir=".snakeviz"
prof="$root_output_dir/mrsm.prof"
mkdir -p "$root_output_dir"

python -m cProfile -o "$prof" -m meerschaum $@
python -m snakeviz "$prof"
