#!/usr/bin/env bash
# Build development test artifacts once, before any backend services are acquired.
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo"
unset TMUX TMUX_PANE
python_bin="$(command -v "${PYTHON_BIN:-python3}")"
artifact_dir="$(mktemp -d "${RUNNER_TEMP:-/tmp}/mrsm-pytest-artifacts.XXXXXX")"
mkdir -p "$artifact_dir/wheel" "$artifact_dir/binary" "$artifact_dir/root" "$artifact_dir/plugins"
export MRSM_ROOT_DIR="$artifact_dir/root" MRSM_PLUGINS_DIR="$artifact_dir/plugins"
export MRSM_NO_AUTO_INSTALL=1 MRSM_NO_UPDATE_CHECK=1
ci_cache="${MRSM_CI_CACHE:-$HOME/.cache/mrsm-ci}"
py_tag="$("$python_bin" -c 'import sys; print(f"py{sys.version_info[0]}.{sys.version_info[1]}")')"
maturin_spec='maturin>=1.9,<2.0'
maturin_venv="$artifact_dir/tools"
if mkdir -p "$ci_cache" 2>/dev/null && [ -w "$ci_cache" ]; then
  maturin_venv="$ci_cache/maturin-venv-$py_tag"
  if command -v flock >/dev/null 2>&1; then
    exec 9>"$ci_cache/.maturin-venv-$py_tag.lock"
    flock 9
  fi
  if [ ! -x "$maturin_venv/bin/maturin" ] || \
     [ "$(cat "$maturin_venv/.spec" 2>/dev/null || true)" != "$maturin_spec" ]; then
    "$python_bin" -m venv --clear "$maturin_venv"
    "$maturin_venv/bin/python" -m pip install --quiet "$maturin_spec"
    printf '%s\n' "$maturin_spec" > "$maturin_venv/.spec"
  fi
  if command -v flock >/dev/null 2>&1; then
    exec 9>&-
  fi
else
  "$python_bin" -m venv "$maturin_venv"
  "$maturin_venv/bin/python" -m pip install --quiet "$maturin_spec"
fi
eval "$(scripts/rust/ci.sh env)"
"$maturin_venv/bin/maturin" build --profile ci --locked --auditwheel skip \
  --out "$artifact_dir/wheel" -i "$python_bin"
"$python_bin" scripts/ci/artifacts.py create --kind wheel --profile ci \
  --target x86_64-unknown-linux-gnu --artifact-dir "$artifact_dir/wheel" --output "$artifact_dir/wheel/provenance.json"
if ! cargo build --profile ci --locked -p meerschaum --bin mrsm --message-format=json-render-diagnostics > "$artifact_dir/cli-build.jsonl"; then
  cat "$artifact_dir/cli-build.jsonl" >&2
  exit 1
fi
cp target/ci/mrsm "$artifact_dir/binary/mrsm"
"$python_bin" scripts/ci/artifacts.py create --kind binary --profile ci \
  --target x86_64-unknown-linux-gnu --artifact-dir "$artifact_dir/binary" --output "$artifact_dir/binary/provenance.json"
if [ "${MRSM_TEST_CONSOLE:-0}" = 1 ]; then
  "$python_bin" scripts/ci/console_fixture.py build --messages "$artifact_dir/cli-build.jsonl" \
    --binary target/ci/mrsm --output "$artifact_dir/console-native-fixture" --profile ci
  "$python_bin" scripts/ci/console_fixture.py record --output "$artifact_dir/console-native-fixture" --profile ci
fi
if [ -n "${GITHUB_ENV:-}" ]; then
  echo "CI_ARTIFACT_DIR=$artifact_dir" >> "$GITHUB_ENV"
fi
printf 'Test artifacts: %s\n' "$artifact_dir"
