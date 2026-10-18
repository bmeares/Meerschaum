#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
export MRSM_ROOT_DIR="$PARENT/test_root"
export MRSM_PLUGINS_DIR="$PARENT/tests/plugins"
mkdir -p "$MRSM_ROOT_DIR"

[ -z "$PYTHON_BIN" ] && PYTHON_BIN=python

### `mrsm` runs through the extension of this tree (`python -m meerschaum` calls `_native.main`), never
### through a standalone binary found in `target/`: the shared runner checkout keeps the binaries of
### earlier builds, and a stale one made a CI run test old code. A standalone binary is used only when
### `MRSM_BIN` names it explicitly, and then it must be the build of this tree: its `--version` has to
### equal the extension's, and CI refuses a binary older than the checkout's Rust sources.
export PYTHONPATH="$PARENT${PYTHONPATH:+:$PYTHONPATH}"
if [ -n "$MRSM_BIN" ]; then
  $PYTHON_BIN -c '
import sys
parent, binary = sys.argv[1:3]
sys.path.insert(0, parent)
from tests._binary import check_binary, StaleBinaryError
try:
    check_binary(binary)
except (StaleBinaryError, OSError) as e:
    print(f"mrsm.sh: refusing MRSM_BIN: {e}", file=sys.stderr)
    sys.exit(3)
' "$PARENT" "$MRSM_BIN" || exit $?
fi

$PYTHON_BIN -m meerschaum "$@" ; rc=$?
exit $rc
