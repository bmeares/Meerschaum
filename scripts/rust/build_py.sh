#!/usr/bin/env bash
# Usage: scripts/rust/build_py.sh [--release | --debug] [--no-polars] [--no-check] [-- MATURIN_ARGS...]
#
# Builds the PyO3 extension (crates/meerschaum-py) with maturin and places it in the working
# tree at meerschaum/_core/_native.abi3.so, where `meerschaum._core` loads it from. Nothing is
# installed into any environment; the root pyproject.toml (the release maturin project) is not
# involved: this builds from crates/meerschaum-py/pyproject.toml.
#
#   --release    Optimized build (default). Small .so (~70 MB), slow first build.
#   --debug      Unoptimized build, stripped of symbols (unstripped it is ~1 GB of debuginfo).
#   --no-polars  Build without the pyo3-polars path (`_core.polars_roundtrip`, crate feature
#                `polars`), which also skips compiling Polars (~12 MB .so).
#   --no-check   Skip the post-build import check.
#
# The extension always includes the `duckdb` and `oracle` flavors (what the wheels ship). DuckDB is not
# compiled in: its own shared library is loaded at run time (`MRSM_DUCKDB_LIB`, `<root>/drivers`, or a
# checked download on first use), so only `oracle` is a cargo feature.
#
# Requires maturin (`uv tool install maturin`) and a Python >= 3.11 (`PYTHON`, default python3).
# MATURIN_BIN may select a specific maturin executable; otherwise use the current PATH lookup.
# Never set CARGO_TARGET_DIR: builds share the workspace `target/` (see crates/README.md).
#
# Concurrent builds corrupt each other (they swap the same file), so the script takes the tree's lock,
# `target/.build-py.lock`, itself and waits for it: callers must NOT wrap it in `flock`. The lock is
# per working tree (two checkouts build in parallel) and is released when the script exits.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"

export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$HOME/.local/bin:$PATH"
PYTHON="${PYTHON:-python3}"
MATURIN_BIN="${MATURIN_BIN:-maturin}"

profile="release"
polars=1
check=1
extra=()
while [ $# -gt 0 ]; do
  case "$1" in
    --release) profile="release" ;;
    --debug) profile="debug" ;;
    --no-polars) polars=0 ;;
    --no-check) check=0 ;;
    --) shift; extra+=("$@"); break ;;
    -h|--help) sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "build_py.sh: unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

if [ -n "${CARGO_TARGET_DIR:-}" ]; then
  echo "build_py.sh: CARGO_TARGET_DIR is set ($CARGO_TARGET_DIR); unset it to use the shared target/." >&2
  exit 2
fi
if ! maturin_executable="$(command -v "$MATURIN_BIN")" || [ ! -x "$maturin_executable" ]; then
  echo "build_py.sh: maturin executable '$MATURIN_BIN' not found. Install it with: uv tool install maturin" >&2
  exit 1
fi
if ! command -v "$PYTHON" >/dev/null 2>&1; then
  echo "build_py.sh: Python interpreter '$PYTHON' not found (set PYTHON=...)." >&2
  exit 1
fi

# One build per tree at a time. `flock` is absent on macOS: there the lock is skipped.
mkdir -p "$REPO/target"
if command -v flock >/dev/null 2>&1; then
  exec 9>"$REPO/target/.build-py.lock"
  if ! flock -n 9; then
    echo "==> Waiting for another extension build in this tree (target/.build-py.lock)"
    flock 9
  fi
fi

# Build a wheel into a scratch directory first, so the extension already in the tree keeps
# working (for pytest runs in progress) during the slow compile; it is swapped in at the end.
# A unique directory per build, so concurrent builds never collide.
mkdir -p "$REPO/target"
wheel_dir="$(mktemp -d "$REPO/target/build_py_wheel.XXXXXX")"
args=(build --manifest-path crates/meerschaum-py/Cargo.toml --auditwheel skip --out "$wheel_dir")
if [ "$profile" = "release" ]; then
  args+=(--release)
else
  args+=(--strip)
fi
# A command-line `--features` REPLACES the `features` list in pyproject.toml (it does not add to
# it), so always pass the full set. Without `extension-module` the module links libpython.
features="pyo3/abi3-py311,extension-module,oracle"
if [ "$polars" -eq 1 ]; then
  features="$features,polars"
fi
args+=(--features "$features" -i "$(command -v "$PYTHON")")

echo "==> $MATURIN_BIN ${args[*]} ${extra[*]:-}"
# Unset VIRTUAL_ENV so an active venv is not inspected.
env -u VIRTUAL_ENV PYO3_PYTHON="$(command -v "$PYTHON")" "$maturin_executable" "${args[@]}" "${extra[@]}"

shopt -s nullglob
wheels=("$wheel_dir"/*.whl)
shopt -u nullglob
if [ "${#wheels[@]}" -ne 1 ]; then
  echo "build_py.sh: expected one wheel in $wheel_dir, found ${#wheels[@]}." >&2
  exit 1
fi
"$PYTHON" -c '
import sys, zipfile
wheel, dest = sys.argv[1:]
with zipfile.ZipFile(wheel) as zf:
    names = [n for n in zf.namelist()
             if n.startswith("meerschaum/_core/_native") and n.endswith((".so", ".pyd"))]
    if len(names) != 1:
        sys.exit(f"build_py.sh: expected one extension in {wheel}, found {names}")
    zf.extract(names[0], dest)
' "${wheels[0]}" "$wheel_dir/x"

# Remove stale builds (e.g. a non-abi3 `_core.cpython-3XX-*.so`) so only the new one can load,
# then move the new one in (same filesystem, so the swap is a rename).
"$REPO/scripts/rust/clean_py.sh" --quiet
mv "$wheel_dir"/x/meerschaum/_core/_native* meerschaum/_core/
rm -rf "$wheel_dir"

shopt -s nullglob
built=(meerschaum/_core/_native*.so meerschaum/_core/_native*.pyd)
shopt -u nullglob
if [ "${#built[@]}" -gt 0 ] && command -v readelf >/dev/null 2>&1 \
  && readelf -d "${built[0]}" | grep -q 'NEEDED.*libpython'; then
  echo "build_py.sh: ${built[0]} links libpython; it was built without extension-module." >&2
  exit 1
fi
shopt -s nullglob
shopt -u nullglob
if [ "${#built[@]}" -eq 0 ]; then
  echo "build_py.sh: maturin finished but no extension was found in meerschaum/_core/." >&2
  exit 1
fi
echo "==> Built ${built[*]} ($(du -h "${built[0]}" | cut -f1))"

if [ "$check" -eq 1 ]; then
  # Importing `meerschaum._core` runs `meerschaum/__init__.py`: isolate it from real config and
  # plugins with an empty, throwaway root and plugins directory.
  scratch="$(mktemp -d)"
  trap 'rm -rf "$scratch"' EXIT
  mkdir -p "$scratch/root" "$scratch/plugins"
  PYTHONPATH="$REPO" MRSM_ROOT_DIR="$scratch/root" \
    MRSM_PLUGINS_DIR="$scratch/plugins" "$PYTHON" - <<'EOF'
import importlib, sys
core = importlib.import_module('meerschaum._core')
if not core.AVAILABLE:
    sys.exit(f"build_py.sh: meerschaum._core did not load the extension: {core.LOAD_ERROR!r}")
info = core.build_info()
print(f"==> meerschaum._core {core.__version__} ({info['profile']}, {info['python_abi']}, "
      f"features={','.join(info['features'])}, static_config={info['static_config']}) "
      f"from {core.EXTENSION_PATH}")
EOF
fi
