#!/usr/bin/env bash
# Usage: scripts/rust/duckdb_lib.sh
#
# Prints the path of DuckDB's official shared library (the version and SHA-256 pinned in
# crates/meerschaum-connector-sql/src/duckdb_lib), downloading it into a persistent cache the first
# time. Meerschaum does not compile DuckDB in: tests (and CI) point `MRSM_DUCKDB_LIB` at this file, so
# a run with `MRSM_NO_AUTO_INSTALL=1` still has DuckDB and nothing is downloaded per run.
#
# The cache is `${MRSM_CI_CACHE:-$HOME/.cache/mrsm-ci}/duckdb/<version>/` (outside the workspace, so
# `git clean` and the `target/` wipes never remove it). The pins are read from the Rust source, the single
# source of truth; the zip is checked against its pin before the library is extracted. On
# GitHub, `MRSM_DUCKDB_LIB` is also exported to later steps.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SRC="$REPO/crates/meerschaum-connector-sql/src/duckdb_lib"
PYTHON="${PYTHON_BIN:-python3}"

version="$(sed -n 's/^pub const VERSION: &str = "\(.*\)";/\1/p' "$SRC/mod.rs")"
[ -n "$version" ] || { echo "duckdb_lib.sh: cannot read VERSION from $SRC/mod.rs" >&2; exit 1; }

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) plat=linux-amd64; lib=libduckdb.so ;;
  Linux-aarch64|Linux-arm64) plat=linux-arm64; lib=libduckdb.so ;;
  Darwin-*) plat=osx-universal; lib=libduckdb.dylib ;;
  *) echo "duckdb_lib.sh: no DuckDB library for $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac
# musl hosts need the musl build.
if [ "$plat" != osx-universal ] && (ldd --version 2>&1 || true) | grep -qi musl; then plat="$plat-musl"; fi

sha="$("$PYTHON" -I - "$SRC/install.rs" "$plat" <<'PY'
import re, sys
text = open(sys.argv[1]).read()
m = re.search(r'plat:\s*"%s",\s*sha256:\s*"([0-9a-f]{64})"' % re.escape(sys.argv[2]), text)
print(m.group(1) if m else "")
PY
)"
[ -n "$sha" ] || { echo "duckdb_lib.sh: no pinned checksum for $plat" >&2; exit 1; }

dir="${MRSM_CI_CACHE:-$HOME/.cache/mrsm-ci}/duckdb/$version"
out="$dir/$lib"
if [ ! -f "$out" ] || [ "$(cat "$dir/.zip-sha256" 2>/dev/null)" != "$sha" ]; then
  mkdir -p "$dir"
  tmp="$(mktemp -d "$dir/.tmp.XXXXXX")"
  trap 'rm -rf "$tmp"' EXIT
  url="https://github.com/duckdb/duckdb/releases/download/v$version/libduckdb-$plat.zip"
  echo "duckdb_lib.sh: downloading $url" >&2
  curl -fsSL --retry 3 -o "$tmp/lib.zip" "$url"
  got="$(sha256sum "$tmp/lib.zip" | cut -d' ' -f1)"
  if [ "$got" != "$sha" ]; then
    echo "duckdb_lib.sh: checksum mismatch for $url (expected $sha, got $got)" >&2
    exit 1
  fi
  "$PYTHON" -I - "$tmp/lib.zip" "$lib" "$tmp/$lib" <<'PY'
import sys, zipfile
zip_path, name, dest = sys.argv[1:4]
with zipfile.ZipFile(zip_path) as z, z.open(name) as src, open(dest, 'wb') as dst:
    while chunk := src.read(1 << 20):
        dst.write(chunk)
PY
  chmod 755 "$tmp/$lib"
  mv -f "$tmp/$lib" "$out"
  echo "$sha" > "$dir/.zip-sha256"
fi

if [ -n "${GITHUB_ENV:-}" ]; then
  echo "MRSM_DUCKDB_LIB=$out" >> "$GITHUB_ENV"
fi
echo "$out"
