#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
export MRSM_ROOT_DIR="$PARENT/test_root"
export MRSM_PLUGINS_DIR="$PARENT/tests/plugins"
mkdir -p "$MRSM_ROOT_DIR"

[ -z "$PYTHON_BIN" ] && PYTHON_BIN=python

$PYTHON_BIN -m meerschaum $@ ; rc=$?
exit $rc
