#!/usr/bin/env bash
# Build the synthetic native (C ABI) plugin `w7_native_fig` into a plugins directory.
#
#   tests/console/build_native.sh <plugins-dir>
#
# The plugin source (`tests/plugins/w7_native_fig/plugin.rs`) is compiled as a cdylib against the
# already-built `meerschaum-plugin` rlib in the shared `target/debug/deps` (no Cargo workspace
# change, no new target directory). Run `cargo build -p meerschaum-plugin` first.
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dest="${1:?usage: build_native.sh <plugins-dir>}/w7_native_fig"
deps="$repo/target/debug/deps"
rlib="$(ls -t "$deps"/libmeerschaum_plugin-*.rlib 2>/dev/null | head -1 || true)"
[ -n "$rlib" ] && [ -f "$rlib" ] || { echo "build the meerschaum-plugin rlib first (cargo build -p meerschaum-plugin)" >&2; exit 2; }
mkdir -p "$dest"
cp "$repo/tests/plugins/w7_native_fig/plugin.toml" "$dest/plugin.toml"
# `export_plugin!` names and versions a plugin from its Cargo package; plain `rustc` sets neither.
CARGO_PKG_NAME=w7_native_fig CARGO_PKG_VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "$dest/plugin.toml")" \
rustc --edition 2024 --crate-type cdylib --crate-name w7_native_fig \
  -L "dependency=$deps" --extern "meerschaum_plugin=$rlib" \
  -C debuginfo=0 -o "$dest/libw7_native_fig.so" "$repo/tests/plugins/w7_native_fig/plugin.rs"
echo "$dest/libw7_native_fig.so"
