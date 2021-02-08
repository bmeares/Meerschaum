#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"

python -m meerschaum stack up -d db
python -m meerschaum api start -w 1 &
api_pid=$!

python -m pytest

python -m meerschaum stack down
kill "$api_pid"
