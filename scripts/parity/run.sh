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
#   - the reference package's location and version are checked without importing it, so the
#     generator sets its scratch configuration before the first import; import finders pin
#     this process and every reference child to the released package even from the repo root;
#   - the generator runs as __main__ from the repository root (so repo-relative output paths work)
#     with its own directory as sys.path[0], exactly like `python GENERATOR.py`.
#
# Every generator gets a fresh root and empty plugins directory, removed afterwards. Ambient
# MRSM_ROOT_DIR / MRSM_VENVS_DIR belong to the caller's current build and are never reused.
# Reference dependencies come from .venv-ref, whose versions are pinned by setup_reference.sh;
# the reference's "mrsm" venv links to it instead of installing a second unpinned dependency set.
# Every inherited MRSM_* variable is removed (v4 would read most as connectors), except
# MRSM_VALKEY_FIXTURE_* for gen_valkey.py.
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

# v4 reads every `MRSM_<TYPE>_<LABEL>` variable as a connector (CI's `MRSM_CI_MIN_FREE_GB` became a
# `ci:min_free_gb` connector in recorded `/connectors` and shell completions), so drop every
# inherited MRSM_* variable except the harness's own.
for var in $(compgen -e | grep '^MRSM_' || true); do
  case "$var" in
    MRSM_REF_VERSION|MRSM_VALKEY_FIXTURE_*) ;;
    *) unset "$var" ;;
  esac
done

unset TMUX TMUX_PANE PYTHONPATH PYTHONHOME VIRTUAL_ENV NO_COLOR FORCE_COLOR
export PYTHONNOUSERSITE=1 PYTHONSAFEPATH=1
tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/mrsm-parity-root.XXXXXX")"
trap 'rm -rf "$tmp_root"' EXIT
export MRSM_ROOT_DIR="$tmp_root"
export MRSM_PLUGINS_DIR="$tmp_root/plugins"
export MRSM_VENVS_DIR="$VENV/parity_venvs"
mkdir -p "$MRSM_PLUGINS_DIR" "$MRSM_VENVS_DIR"
if [ ! -e "$MRSM_VENVS_DIR/mrsm" ]; then
  ln -s "$VENV" "$MRSM_VENVS_DIR/mrsm"
fi
export PATH="$(dirname "$ref_py"):$PATH"
# Generators that guard against a stale import (e.g. asserting meerschaum comes from the repo)
# should accept the reference when this is set; its value is the reference version.
export MRSM_PARITY_REFERENCE="$VERSION"
# v4 creates $MRSM_ROOT_DIR/venvs/* with a bare `uv venv`, which picks uv's preferred Python rather
# than the running one; pin it so the reference's own venvs match the reference interpreter.
export UV_PYTHON="$ref_py"
mkdir -p "$MRSM_ROOT_DIR"

cd "$REPO"
# Not `exec`: the EXIT trap must still remove a temporary root.
"$ref_py" -s -E - "$gen" "$VERSION" "$VENV" "$@" <<'PY'
import importlib.abc, importlib.util, os, re, runpy, sys

gen, expected, venv, *args = sys.argv[1:]
# Drop the cwd entry ('' for stdin scripts) so the repository's meerschaum/ is not importable.
sys.path[:] = [p for p in sys.path if p not in ('', os.getcwd())]

# Locate the reference package WITHOUT importing it. Importing meerschaum reads the config
# registry at import time, and generators isolate their environment (MRSM_ROOT_DIR, MRSM_CONFIG,
# MRSM_PLUGINS_DIR) only when they start; a pre-import would freeze the wrong config.
venv_real = os.path.realpath(venv) + os.sep
spec = importlib.util.find_spec('meerschaum')
origin = os.path.realpath(spec.origin) if spec and spec.origin else ''
version = ''
if origin:
    with open(os.path.join(os.path.dirname(origin), 'config', '_version.py'), encoding='utf-8') as f:
        m = re.search(r"__version__\s*=\s*['\"]([^'\"]+)['\"]", f.read())
        version = m.group(1) if m else ''
if version != expected or not origin.startswith(venv_real):
    sys.exit(
        f"parity/run.sh: refusing to run: meerschaum {version or '?'} at {origin or '?'}, "
        f"expected {expected} under {venv}"
    )
print(f"[parity] meerschaum {version} ({origin}) -> {gen}", file=sys.stderr)


class _RefOnly(importlib.abc.MetaPathFinder):
    """Refuse any `meerschaum` import that would not come from the reference venv (a generator
    that inserts the repository root into sys.path must still get the reference package)."""

    def find_spec(self, name, path=None, target=None):
        if name != 'meerschaum' and not name.startswith('meerschaum.'):
            return None
        for finder in sys.meta_path:
            if finder is self:
                continue
            found = getattr(finder, 'find_spec', lambda *a, **k: None)(name, path, target)
            if found is None:
                continue
            found_origin = os.path.realpath(found.origin) if found.origin else ''
            if found_origin and not found_origin.startswith(venv_real):
                raise ImportError(
                    f"parity/run.sh: {name} resolved to {found_origin}, outside the reference venv {venv}"
                )
            return found
        return None


sys.meta_path.insert(0, _RefOnly())

sys.argv = [gen, *args]
sys.path.insert(0, os.path.dirname(gen))
runpy.run_path(gen, run_name='__main__')
PY
