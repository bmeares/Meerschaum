#!/usr/bin/env bash
# Usage: scripts/transition/run.sh [--flavors sqlite,postgis,timescaledb,mssql,mysql,mariadb,oracle] [--wheel PATH]
#
# Takes state that released meerschaum 4.1.4 created (a root, instance tables, synced data, plugins with their
# venvs, users, tokens, jobs -- one of them still running --, a compose project that is up) and continues it
# with the working tree's v5. Every difference is printed by report.py as PASS / FAIL / DIFF / INFO.
#
#   --flavors   instances to run against (default sqlite). The server flavors use the repository's disposable
#               test containers (tests/docker-compose.yaml) and only objects named `trn*`, dropped afterwards.
#   --wheel     also upgrade a scratch venv from 4.1.4 to this v5 wheel (pip hygiene), then run sqlite with the
#               INSTALLED wheel instead of PYTHONPATH=<repo>.
#
# Environment:
#   MRSM_TRANSITION_DIR     scratch directory (default ~/.cache/mrsm-transition); never inside the repository
#   MRSM_TRANSITION_V4_PY   interpreter for v4 (default: <scratch>/venv-v4, created here with
#                           `pip install meerschaum==4.1.4`, so v4 installs its dependencies like a user's)
#   MRSM_TRANSITION_V4_API_PY  interpreter for the v4 API baseline (default: .venv-ref/bin/python when present)
# Needs: uv, network access (PyPI), the built extension (scripts/rust/build_py.sh) and, for the standalone binary
# checks, target/debug/mrsm. Every process gets an explicit scratch MRSM_ROOT_DIR / MRSM_PLUGINS_DIR and a scratch
# sqlite default instance; v4 runs under `env -i`-style clean environments.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
SCRATCH="${MRSM_TRANSITION_DIR:-$HOME/.cache/mrsm-transition}"
export MRSM_TRANSITION_DIR="$SCRATCH"
flavors="sqlite"
wheel=""
while [ $# -gt 0 ]; do
  case "$1" in
    --flavors) flavors="$2"; shift ;;
    --wheel) wheel="$2"; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done
case "$SCRATCH" in "$REPO"*) echo "run.sh: the scratch directory must be outside the repository" >&2; exit 2;; esac
mkdir -p "$SCRATCH/tmp"
export TMPDIR="$SCRATCH/tmp"

# the user's interpreter: a venv with only `meerschaum==4.1.4` from PyPI
if [ -z "${MRSM_TRANSITION_V4_PY:-}" ] && [ ! -x "$SCRATCH/venv-v4/bin/python" ]; then
  uv venv --seed --python python3 "$SCRATCH/venv-v4"
  uv pip install --python "$SCRATCH/venv-v4/bin/python" 'meerschaum==4.1.4'
fi
PY="${PYTHON:-python3}"
failed=0
first=1
for f in ${flavors//,/ }; do
  extra=""
  [ $first -eq 1 ] && extra="--extras"
  first=0
  "$PY" "$HERE/run_all.py" "run-$f" "$f" $extra || failed=1
done
if [ -n "$wheel" ]; then
  "$PY" "$HERE/run_pip.py" "$wheel" || failed=1
  MRSM_TRANSITION_V5_PY="$SCRATCH/venv-up/bin/python" MRSM_TRANSITION_V5_WHEEL=1 \
    "$PY" "$HERE/run_all.py" run-wheel sqlite --extras || failed=1
fi
"$PY" "$HERE/report.py" || failed=1
exit $failed
