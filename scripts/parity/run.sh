#!/usr/bin/env bash
# Usage: scripts/parity/run.sh GENERATOR.py [ARGS ...]
#
# Runs a golden-fixture generator (e.g. crates/meerschaum-kernel/tests/fixtures/gen_dtypes.py)
# with the reference interpreter from scripts/parity/setup_reference.sh, so the fixture is
# produced by the released meerschaum 4.1.4 and not by the working tree.
#
# Guarantees, independent of the ambient interpreter (see the ~/.local gotcha in CLAUDE.md):
#   - the interpreter is .venv-ref's python, started with -s -E (no user site-packages, no
#     PYTHONPATH/PYTHONHOME), so neither ~/.local nor PYTHONPATH=$(pwd) can leak in;
#   - `meerschaum` is imported from .venv-ref and its version is asserted *before* the generator
#     runs; because it is then cached in sys.modules, a generator that inserts the repo root into
#     sys.path still gets the reference package;
#   - the generator runs as __main__ from the repository root (so repo-relative output paths work)
#     with its own directory as sys.path[0], exactly like `python GENERATOR.py`.
#
# Environment (defaults keep the reference's v4 venvs apart from the working tree's test_root):
#   MRSM_ROOT_DIR     default .venv-ref/mrsm_root
#   MRSM_PLUGINS_DIR  default tests/plugins
#   MRSM_REF_VERSION  version to assert (default 4.1.4)
#   UV_PYTHON         forced to the reference interpreter (v4's `uv venv` calls omit --python)
# Exported to the generator: MRSM_PARITY_REFERENCE=<version> (lets a generator's own
# "is this the repo's meerschaum?" guard accept the reference interpreter).
set -euo pipefail

if [ $# -lt 1 ]; then
  sed -n '2,23p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
  exit 2
fi

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VENV="$REPO/.venv-ref"
VERSION="${MRSM_REF_VERSION:-4.1.4}"

gen="$1"; shift
[ -f "$gen" ] || { echo "run.sh: no such generator: $gen" >&2; exit 2; }
gen="$(cd "$(dirname "$gen")" && pwd)/$(basename "$gen")"

ref_py="$VENV/bin/python"
[ -x "$ref_py" ] || ref_py="$VENV/Scripts/python.exe"
if [ ! -x "$ref_py" ]; then
  echo "run.sh: reference venv missing; run scripts/parity/setup_reference.sh first." >&2
  exit 2
fi

export MRSM_ROOT_DIR="${MRSM_ROOT_DIR:-$VENV/mrsm_root}"
export MRSM_PLUGINS_DIR="${MRSM_PLUGINS_DIR:-$REPO/tests/plugins}"
# Generators that guard against a stale import (e.g. asserting meerschaum comes from the repo)
# should accept the reference when this is set; its value is the reference version.
export MRSM_PARITY_REFERENCE="$VERSION"
# v4 creates $MRSM_ROOT_DIR/venvs/* with a bare `uv venv`, which picks uv's preferred Python rather
# than the running one; pin it so the reference's own venvs match the reference interpreter.
export UV_PYTHON="$ref_py"
mkdir -p "$MRSM_ROOT_DIR"

cd "$REPO"
exec "$ref_py" -s -E - "$gen" "$VERSION" "$VENV" "$@" <<'PY'
import os, runpy, sys

gen, expected, venv, *args = sys.argv[1:]
# Drop the cwd entry ('' for stdin scripts) so the repository's meerschaum/ is not importable.
sys.path[:] = [p for p in sys.path if p not in ('', os.getcwd())]

import meerschaum
here = os.path.realpath(meerschaum.__file__)
if meerschaum.__version__ != expected or not here.startswith(os.path.realpath(venv) + os.sep):
    sys.exit(
        f"parity/run.sh: refusing to run: imported meerschaum {meerschaum.__version__} from {here}, "
        f"expected {expected} from {venv}"
    )
print(f"[parity] meerschaum {meerschaum.__version__} ({here}) -> {gen}", file=sys.stderr)

sys.argv = [gen, *args]
sys.path.insert(0, os.path.dirname(gen))
runpy.run_path(gen, run_name='__main__')
PY
