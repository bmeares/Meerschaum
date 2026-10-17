#!/usr/bin/env bash
# Usage: scripts/rust/clean_py.sh [--cargo] [--quiet]
#
# Removes the built `meerschaum._core` extension from the working tree
# (meerschaum/_core/_core*.so / .pyd), after which `meerschaum._core.AVAILABLE` is False.
#
#   --cargo   Also run `cargo clean -p meerschaum-py` (this crate's artifacts only; the shared
#             Arrow/Polars builds in target/ are kept for the other crates).
#   --quiet   Print nothing unless something fails.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"

cargo_clean=0
quiet=0
for arg in "$@"; do
  case "$arg" in
    --cargo) cargo_clean=1 ;;
    --quiet) quiet=1 ;;
    -h|--help) sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "clean_py.sh: unknown argument: $arg" >&2; exit 2 ;;
  esac
done

say() { [ "$quiet" -eq 1 ] || echo "$@"; }

shopt -s nullglob
built=(meerschaum/_core/_core*.so meerschaum/_core/_core*.pyd meerschaum/_core/_core*.dylib)
shopt -u nullglob
if [ "${#built[@]}" -gt 0 ]; then
  rm -f -- "${built[@]}"
  say "==> Removed ${built[*]}"
else
  say "==> No built extension in meerschaum/_core/"
fi

if [ "$cargo_clean" -eq 1 ]; then
  export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
  if [ -n "${CARGO_TARGET_DIR:-}" ]; then
    echo "clean_py.sh: CARGO_TARGET_DIR is set ($CARGO_TARGET_DIR); unset it." >&2
    exit 2
  fi
  say "==> cargo clean -p meerschaum-py"
  cargo clean -p meerschaum-py
fi
