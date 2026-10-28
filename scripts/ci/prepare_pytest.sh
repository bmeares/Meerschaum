#!/usr/bin/env bash
# Install a complete verified wheel, preserving its native libraries for source-reading tests.
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo"
unset TMUX TMUX_PANE
python_bin="$(command -v "${PYTHON_BIN:-python3}")"
wheel_profile="${PYTEST_WHEEL_PROFILE:-ci}"
binary_profile="${PYTEST_BINARY_PROFILE:-debug}"
artifact_dir="${PYTEST_ARTIFACT_DIR:?Set PYTEST_ARTIFACT_DIR to downloaded wheel/ and binary/ directories.}"
venv="$repo/.venv-test"
scratch="$(mktemp -d "${RUNNER_TEMP:-/tmp}/mrsm-pytest-env.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/root" "$scratch/plugins"
export MRSM_ROOT_DIR="$scratch/root" MRSM_PLUGINS_DIR="$scratch/plugins" MRSM_NO_AUTO_INSTALL=1 MRSM_NO_UPDATE_CHECK=1
"$python_bin" scripts/ci/artifacts.py verify --manifest "$artifact_dir/wheel/provenance.json" --kind wheel --profile "$wheel_profile" --target x86_64-unknown-linux-gnu
"$python_bin" scripts/ci/artifacts.py verify --manifest "$artifact_dir/binary/provenance.json" --kind binary --profile "$binary_profile" --target x86_64-unknown-linux-gnu
if [ "${MRSM_TEST_CONSOLE:-0}" = 1 ]; then
  "$python_bin" scripts/ci/console_fixture.py verify \
    --manifest "$artifact_dir/console-native-fixture/provenance.json" \
    --binary-manifest "$artifact_dir/binary/provenance.json" --profile "$binary_profile"
fi
wanted="$("$python_bin" -c 'import sys; print(sys.version_info[:2])')"
deps_hash="$({ cat requirements/full.txt requirements/sql-mssql.txt requirements/sql-oracle.txt requirements/sql-duckdb.txt scripts/ci/requirements-browser.txt; \
  printf '%s\n' "pytest pytest-xdist httpx httpx2 uv $wanted"; } | sha256sum | cut -d' ' -f1)"
if [ ! -x "$venv/bin/python" ] || \
   [ "$("$venv/bin/python" -c 'import sys; print(sys.version_info[:2])')" != "$wanted" ] || \
   [ "$(cat "$venv/.deps-hash" 2>/dev/null)" != "$deps_hash" ]; then
  "$python_bin" -m venv --clear "$venv"
  "$venv/bin/python" -m pip install --quiet --upgrade pip
  "$venv/bin/python" -m pip install --quiet -r requirements/full.txt -r requirements/sql-mssql.txt \
    -r requirements/sql-oracle.txt -r requirements/sql-duckdb.txt -r scripts/ci/requirements-browser.txt pytest pytest-xdist httpx httpx2 uv
  echo "$deps_hash" > "$venv/.deps-hash"
fi
if [ "${MRSM_TEST_CONSOLE:-0}" = 1 ]; then
  "$venv/bin/python" -m playwright install chromium
  "$venv/bin/python" - <<'PY'
from playwright.sync_api import sync_playwright
with sync_playwright() as instance:
    browser = instance.chromium.launch(headless=True)
    browser.close()
print('Required console browser is ready.')
PY
fi
"$venv/bin/python" -m pip install --quiet --no-deps --force-reinstall "$artifact_dir/wheel/"*.whl
(
  cd "$scratch"
  unset PYTHONPATH
  "$venv/bin/python" -I - <<'PY'
from pathlib import Path
import sys
import meerschaum
from meerschaum import _core
assert Path(meerschaum.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
assert _core.AVAILABLE, _core.LOAD_ERROR
assert Path(_core.EXTENSION_PATH).resolve().is_relative_to(Path(sys.prefix).resolve())
print('Installed wheel and native payload:', meerschaum.__version__, _core.EXTENSION_PATH)
PY
)
"$python_bin" scripts/ci/artifacts.py overlay-wheel --manifest "$artifact_dir/wheel/provenance.json" --profile "$wheel_profile"
binary="$("$python_bin" - "$artifact_dir/binary" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
manifest = json.loads((root / 'provenance.json').read_text())
programs = [root / name for name in manifest['files'] if name.startswith('mrsm') and not name.endswith('.sha256')]
assert len(programs) == 1, programs
print(programs[0])
PY
)"
chmod +x "$binary"
scripts/parity/setup_reference.sh
if [ -n "${GITHUB_ENV:-}" ]; then
  {
    echo "PYTHON_BIN=$venv/bin/python"
    echo "MRSM_PYTHON=$venv/bin/python"
    echo "MRSM_TEST_BIN=$binary"
    echo "MRSM_TEST_ARTIFACT_MANIFEST=$artifact_dir/binary/provenance.json"
    echo "MRSM_TEST_BIN_PROFILE=$binary_profile"
    echo "MRSM_TEST_SKIP_INSTALL=1"
    if [ "${MRSM_TEST_CONSOLE:-0}" = 1 ]; then
      echo "MRSM_TEST_CONSOLE_NATIVE_FIXTURE=$artifact_dir/console-native-fixture"
      echo "MRSM_TEST_CONSOLE_NATIVE_MANIFEST=$artifact_dir/console-native-fixture/provenance.json"
      echo "MRSM_TEST_CONSOLE_NATIVE_PROFILE=$binary_profile"
    fi
  } >> "$GITHUB_ENV"
fi
printf 'Prepared verified wheel and CLI for backend tests.\n'
