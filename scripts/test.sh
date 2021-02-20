#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
test_root="test_root"
mkdir -p "$test_root"

MRSM_ROOT_DIR="$test_root" python -m meerschaum stack up -d db
MRSM_ROOT_DIR="$test_root" python -m meerschaum api start -w 1 &
api_pid=$!

python -m pytest

MRSM_ROOT_DIR="$test_root" python -m meerschaum stack down
kill "$api_pid"
