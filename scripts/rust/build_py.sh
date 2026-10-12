#!/usr/bin/env bash
# Usage: scripts/rust/build_py.sh [--release | --debug] [--no-polars] [--no-check] [-- MATURIN_ARGS...]
#
# Builds the PyO3 extension (crates/meerschaum-py) with maturin and places it in the working
# tree at meerschaum/_core/_core.abi3.so, where `meerschaum._core` loads it from. Nothing is
# installed into any environment; the root pyproject.toml (setuptools) is not involved.
#
#   --release    Optimized build (default). Small .so (~70 MB), slow first build.
#   --debug      Unoptimized build, stripped of symbols (unstripped it is ~1 GB of debuginfo).
#   --no-polars  Build without the pyo3-polars path (`_core.polars_roundtrip`, crate feature
#                `polars`), which also skips compiling Polars (~12 MB .so).
#   --no-check   Skip the post-build import check.
#
# Requires maturin (`uv tool install maturin`) and a Python >= 3.11 (`PYTHON`, default python3).
# Never set CARGO_TARGET_DIR: builds share the workspace `target/` (see crates/README.md).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"

export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$HOME/.local/bin:$PATH"
PYTHON="${PYTHON:-python3}"

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
    -h|--help) sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "build_py.sh: unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

if [ -n "${CARGO_TARGET_DIR:-}" ]; then
  echo "build_py.sh: CARGO_TARGET_DIR is set ($CARGO_TARGET_DIR); unset it to use the shared target/." >&2
  exit 2
fi
if ! command -v maturin >/dev/null 2>&1; then
  echo "build_py.sh: maturin not found. Install it with: uv tool install maturin" >&2
  exit 1
fi
if ! command -v "$PYTHON" >/dev/null 2>&1; then
  echo "build_py.sh: Python interpreter '$PYTHON' not found (set PYTHON=...)." >&2
  exit 1
fi

# Remove stale builds (e.g. a non-abi3 `_core.cpython-3XX-*.so`) so only the new one can load.
"$REPO/scripts/rust/clean_py.sh" --quiet

args=(develop --skip-install --manifest-path crates/meerschaum-py/Cargo.toml)
if [ "$profile" = "release" ]; then
  args+=(--release)
else
  args+=(--strip)
fi
# A command-line `--features` REPLACES the `features` list in pyproject.toml (it does not add to
# it), so always pass the full set. Without `extension-module` the module links libpython.
features="pyo3/abi3-py311,extension-module"
if [ "$polars" -eq 1 ]; then
  features="$features,polars"
fi
args+=(--features "$features")

echo "==> maturin ${args[*]} ${extra[*]:-}"
# `--skip-install` builds in place for the mixed layout (python-source = repository root) and
# never touches an environment. Unset VIRTUAL_ENV so an active venv is not inspected.
env -u VIRTUAL_ENV PYO3_PYTHON="$(command -v "$PYTHON")" maturin "${args[@]}" "${extra[@]}"

shopt -s nullglob
built=(meerschaum/_core/_core*.so meerschaum/_core/_core*.pyd)
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
  PYTHONPATH="$REPO" MRSM_CORE=python MRSM_ROOT_DIR="$scratch/root" \
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
