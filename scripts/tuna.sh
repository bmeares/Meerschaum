#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"

root_output_dir=".tuna"
import_log="$root_output_dir/import.log"
import_dest="$root_output_dir/import"
runtime_log="$root_output_dir/runtime.log"
runtime_dest="$root_output_dir/runtime"
mkdir -p "$root_output_dir"

# python -X importtime -m meerschaum "$@" 2> "$import_log"
python -X importtime -mcProfile -o "$runtime_log" -m meerschaum "$@" 2> "$import_log"

python -m tuna "$import_log" -o "$import_dest" --no-browser
python -m tuna "$runtime_log" -o "$runtime_dest" --no-browser

python -m webbrowser "$import_dest/index.html"
python -m webbrowser "$runtime_dest/index.html"
