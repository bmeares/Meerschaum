#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
test_root="test_root"
mkdir -p "$test_root"
test_port="8989"

MRSM_ROOT_DIR="$test_root" python -m meerschaum stack up -d db
MRSM_ROOT_DIR="$test_root" python -m meerschaum api start -w 1 -p $test_port &
api_pid=$!

python -m pytest --ignore=portable/

MRSM_ROOT_DIR="$test_root" python -m meerschaum stack down
kill "$api_pid"
