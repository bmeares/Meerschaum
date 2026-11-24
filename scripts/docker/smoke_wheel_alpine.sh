#!/bin/sh
# Usage (inside an `alpine` container, as the `smoke-musl` job of .github/workflows/wheels.yaml runs it):
#
#   docker run --rm -v "$PWD/dist:/dist:ro" \
#     -v "$PWD/scripts/ci/upgrade_state.py:/upgrade_state.py:ro" \
#     -v "$PWD/docs/zensical/changelog/v5.md:/changelog.md:ro" \
#     -v "$PWD/scripts/docker/smoke_wheel_alpine.sh:/smoke.sh:ro" \
#     -v "$PWD/scripts/docker/smoke_facade.py:/smoke_facade.py:ro" alpine:3.22 sh /smoke.sh
#
# Installs the musllinux wheel(s) in /dist with Alpine's own Python (pip, no index, no extras) and checks
# the wheel alone works on musl libc: no dependency beyond the packages Alpine's pip already has, the
# extension loads, `mrsm` and `python -m meerschaum` run, a tiny pipe syncs on SQLite, and the Python API
# smoke (`smoke_facade.py`, installs disabled) passes. POSIX sh: Alpine has no bash.
set -eux

apk add --no-cache python3 py3-pip
python3 --version

python3 -m pip list --format=freeze | sed 's/==.*//' | sort > /tmp/before.txt
python3 -m pip install --break-system-packages --no-index --find-links /dist meerschaum
python3 -m pip list --format=freeze | sed 's/==.*//' | sort > /tmp/after.txt
# Alpine ships its own pip dependencies (packaging, pyparsing, ...): only `meerschaum` may be new.
added="$(comm -13 /tmp/before.txt /tmp/after.txt | grep -vx meerschaum || true)"
if [ -n "$added" ]; then echo "unexpected dependencies: $added" >&2; exit 1; fi

export MRSM_ROOT_DIR=/tmp/mrsm-root MRSM_PLUGINS_DIR=/tmp/mrsm-plugins MRSM_NO_AUTO_INSTALL=1
mkdir -p "$MRSM_ROOT_DIR" "$MRSM_PLUGINS_DIR"
cd /tmp
python3 -c 'import meerschaum, meerschaum._core as core; assert core.AVAILABLE, core.LOAD_ERROR; print("meerschaum", meerschaum.__version__, "core", core.__version__, core.EXTENSION_PATH)'
mrsm --version
mrsm show version
mrsm show connectors
python3 -m meerschaum --version

export MRSM_SQL_SMOKE=sqlite:////tmp/smoke.db
mrsm register pipe -i sql:smoke -c sql:smoke -m tiny --noask \
  --params '{"columns": {"primary": "id"}, "fetch": {"definition": "SELECT 1 AS id, 2 AS val"}}'
mrsm sync pipes -i sql:smoke -c sql:smoke -m tiny
mrsm show data -i sql:smoke -c sql:smoke -m tiny --nopretty | tee /tmp/tiny.txt
python3 -I /smoke_facade.py --cli-data /tmp/tiny.txt

export MRSM_ROOT_DIR=/tmp/mrsm-root-api MRSM_PLUGINS_DIR=/tmp/mrsm-plugins-api
mkdir -p "$MRSM_ROOT_DIR" "$MRSM_PLUGINS_DIR"
export MRSM_SQL_SMOKE=sqlite:////tmp/api-smoke.db
export MRSM_CONFIG='{"meerschaum": {"instance": "sql:smoke"}}'
python3 -I /smoke_facade.py | tee /tmp/api-smoke.txt
grep -q '"install_requests": \[\]' /tmp/api-smoke.txt
version=$(python3 -I -c 'import meerschaum; print(meerschaum.__version__)')
previous=$(python3 /upgrade_state.py previous --version "$version" --changelog /changelog.md)
python3 -m venv /tmp/before
/tmp/before/bin/pip install --only-binary=:all: "meerschaum==$previous"
export MRSM_ROOT_DIR=/tmp/upgrade MRSM_PLUGINS_DIR=/tmp/upgrade/plugins
export MRSM_SQL_UPGRADE=sqlite:////tmp/upgrade/state.db
export MRSM_CONFIG='{"meerschaum":{"instance":"sql:upgrade","executor":"local"}}'
mkdir -p "$MRSM_ROOT_DIR" "$MRSM_PLUGINS_DIR"
/tmp/before/bin/python -I /upgrade_state.py seed
python3 -I /upgrade_state.py verify
python3 -I /upgrade_state.py reopen
echo "musllinux smoke OK"
