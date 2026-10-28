#!/usr/bin/env bash
# Build the synthetic native (C ABI) plugin `w7_native_fig` into a plugins directory.
#
#   tests/console/build_native.sh <plugins-dir>
#
# CI copies a provenance-verified fixture from the matching CLI producer and never looks for
# an SDK on the consumer. Local runs retain compilation against an already-built debug SDK
# (no Cargo workspace change or new target directory).
set -euo pipefail
unset TMUX TMUX_PANE
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dest="${1:?usage: build_native.sh <plugins-dir>}/w7_native_fig"
if [ -n "${MRSM_TEST_CONSOLE_NATIVE_FIXTURE:-}" ]; then
  [ "${MRSM_TEST_CONSOLE_NATIVE_MANIFEST:?Missing fixture manifest}" = "$MRSM_TEST_CONSOLE_NATIVE_FIXTURE/provenance.json" ] || {
    echo 'Console fixture path and manifest differ.' >&2; exit 2;
  }
  "${PYTHON_BIN:-python3}" "$repo/scripts/ci/console_fixture.py" install \
    --manifest "$MRSM_TEST_CONSOLE_NATIVE_MANIFEST" \
    --binary-manifest "${MRSM_TEST_ARTIFACT_MANIFEST:?Missing matching CLI manifest}" \
    --profile "${MRSM_TEST_CONSOLE_NATIVE_PROFILE:?Missing fixture profile}" \
    --destination "$(dirname "$dest")"
  echo "$dest/libw7_native_fig.so"
  exit 0
fi
if [ -n "${CI:-}${GITHUB_ACTIONS:-}" ]; then
  echo 'CI requires the verified console-native fixture artifact; local SDK fallback is disabled.' >&2
  exit 2
fi
export PATH="$HOME/.cargo/bin:$PATH"
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
