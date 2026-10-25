#!/usr/bin/env bash
# Usage: scripts/docker/smoke_image.sh [--full-image IMAGE] [--slim-image IMAGE]
#                                    [--rust-image IMAGE] [--rust-full-image IMAGE] [--image IMAGE ...]
#
# Smoke test of built Meerschaum images (run by scripts/docker/buildx.sh after a local build):
#   - every image has `curl` (the stack's API healthcheck is
#     `["CMD", "curl", "--fail", "http://localhost:<port>/healthcheck"]`) a `python` command (scripts and plugins call `python`, not only `python3`) and a working `mrsm`;
#   - the full image also has `git` (plugins may require `git+https://...` packages) and `ssh`;
#   - the full image runs that healthcheck for real: `mrsm start api --production` on a scratch SQLite
#     instance (with the web console, no terminal, a random port) and `curl --fail` against it in the same
#     container, which must answer within 90 seconds.
#
#   --full-image IMAGE   checked as a full image (curl, git, ssh, mrsm, the API healthcheck).
#   --slim-image IMAGE   checked as a slim image (curl, mrsm).
#   --image IMAGE        checked like a slim image (e.g. the `minimal` dep group).
#   An image given as "" is skipped.
#
# The containers run with `--network none` (loopback only), `--rm`, and a unique name
# (`$SMOKE_PREFIX-<random>`, default prefix `mrsm-image-smoke`) that is force-removed on exit.
set -euo pipefail

full_images=()
slim_images=()
rust_images=()
rust_full_images=()
while [ $# -gt 0 ]; do
  case "$1" in
    --full-image) [ -n "$2" ] && full_images+=("$2"); shift 2 ;;
    --rust-image) [ -n "$2" ] && rust_images+=("$2"); shift 2 ;;
    --rust-full-image) [ -n "$2" ] && rust_full_images+=("$2"); shift 2 ;;
    --slim-image|--image) [ -n "$2" ] && slim_images+=("$2"); shift 2 ;;
    -h|--help) sed -n '2,18p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "smoke_image.sh: unknown argument '$1'" >&2; exit 2 ;;
  esac
done
if [ "${#full_images[@]}" -eq 0 ] && [ "${#slim_images[@]}" -eq 0 ] && [ "${#rust_images[@]}" -eq 0 ] && [ "${#rust_full_images[@]}" -eq 0 ]; then
  echo "smoke_image.sh: no image given." >&2
  exit 2
fi

prefix="${SMOKE_PREFIX:-mrsm-image-smoke}"
names=()
cleanup() {
  local n
  for n in "${names[@]}"; do
    docker rm -f "$n" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

failures=0
check() {
  ### check <label> <image> <command...>: run a command in a throwaway container.
  local label="$1" image="$2"; shift 2
  local name="$prefix-$RANDOM$RANDOM" out
  names+=("$name")
  if out="$(docker run --rm --name "$name" --network none --entrypoint "$1" "$image" "${@:2}" 2>&1)"; then
    echo "  ok:   $label: $(printf '%s' "$out" | head -n 1)"
  else
    echo "  FAIL: $label"
    printf '%s\n' "$out" | tail -n 20 | sed 's/^/    | /'
    failures=$((failures + 1))
  fi
}

### The API healthcheck, as the stack runs it, inside the full image.
api_healthcheck_script='
set -u
port=$((20000 + RANDOM % 20000))
root="$(mktemp -d)"
mkdir -p "$root/root" "$root/plugins"
if [ "${1:-}" = blocked ]; then
  # Exercise the private socket fallback, including when smoke tests run as root.
  mkdir -p "$root/root/api"
  touch "$root/root/api/host"
fi
export MRSM_ROOT_DIR="$root/root" MRSM_PLUGINS_DIR="$root/plugins" MRSM_NO_AUTO_INSTALL=1
export MRSM_SQL_SMOKE="sqlite:///$root/smoke.db"
mrsm start api --production --port "$port" -i sql:smoke --no-webterm >"$root/api.log" 2>&1 &
api=$!
for _ in $(seq 1 90); do
  if curl --fail --silent "http://localhost:$port/healthcheck" 2>/dev/null; then
    if ! curl --fail --silent "http://localhost:$port/dash/_dash-layout" >"$root/layout.json" 2>/dev/null; then
      sleep 1
      continue
    fi
    if ! python -c "import json,sys; data=json.load(open(sys.argv[1])); assert isinstance(data,dict) and data.get(\"props\")" "$root/layout.json"; then
      break
    fi
    echo
    echo "API healthcheck and Dash layout answered without network access"
    kill "$api" 2>/dev/null; wait "$api" 2>/dev/null
    exit 0
  fi
  kill -0 "$api" 2>/dev/null || break
  sleep 1
done
echo "the API did not answer its healthcheck:"
tail -n 40 "$root/api.log"
kill "$api" 2>/dev/null
exit 1
'

### All core operations here must work in a filesystem with no Python executable.
native_script=$(cat <<'NATIVE_SMOKE'
set -euo pipefail
if command -v python || command -v python3; then echo "native images must not contain Python"; exit 1; fi
root="$(mktemp -d)"
trap 'rm -rf "$root"' EXIT
export MRSM_ROOT_DIR="$root/root" MRSM_PLUGINS_DIR="$root/plugins" MRSM_NO_AUTO_INSTALL=1 MRSM_NO_UPDATE_CHECK=1
mkdir -p "$MRSM_ROOT_DIR" "$MRSM_PLUGINS_DIR"
mrsm --version
if mrsm upgrade mrsm -y > "$root/upgrade.log" 2>&1; then
  echo "a container must not mutate its installed engine"; exit 1
fi
grep -q "Docker image" "$root/upgrade.log"
mrsm sql local "CREATE TABLE src (id INTEGER PRIMARY KEY, value INTEGER)"
mrsm sql local "INSERT INTO src VALUES (1, 42)"
mrsm register pipes -c sql:local -m native_only -i sql:local --params '{"columns":{"primary":"id"},"fetch":{"definition":"SELECT * FROM src"}}'
mrsm sync pipes -c sql:local -m native_only -i sql:local
mrsm show data -c sql:local -m native_only -i sql:local --nopretty | grep -q 42
mrsm pass -d --name native-smoke -y
for _ in $(seq 1 30); do
  mrsm show jobs --nopretty > "$root/jobs.txt"
  grep -q native-smoke "$root/jobs.txt" && break
  sleep 1
done
grep -q native-smoke "$root/jobs.txt"
port=$((20000 + RANDOM % 20000))
mrsm start api -i sql:local --host 127.0.0.1 --port "$port" --no-auth --no-dash --no-webterm >"$root/api.log" 2>&1 &
api=$!
trap 'kill "$api" 2>/dev/null || true; wait "$api" 2>/dev/null || true; rm -rf "$root"' EXIT
for _ in $(seq 1 30); do
  if curl --fail --silent "http://127.0.0.1:$port/healthcheck"; then
    curl --fail --silent -H 'Content-Type: application/json' \
      -d '{"jsonrpc":"2.0","id":7,"method":"initialize","params":{}}' \
      "http://127.0.0.1:$port/mcp" > "$root/mcp.json"
    grep -q '"result"' "$root/mcp.json"
    echo "native API and MCP are ready"
    exit 0
  fi
  kill -0 "$api" 2>/dev/null || { cat "$root/api.log"; exit 1; }
  sleep 1
done
cat "$root/api.log"
exit 1
NATIVE_SMOKE
)

toolchain_script=$(cat <<'TOOLCHAIN_SMOKE'
set -eu
cargo --version
rustc --version
cc --version
root="$(mktemp -d)"
trap 'rm -rf "$root"' EXIT
cargo init --quiet --lib --name mrsm_smoke "$root"
cargo check --offline --manifest-path "$root/Cargo.toml"
export MRSM_ROOT_DIR="$root/root" MRSM_PLUGINS_DIR="$root/plugins" MRSM_NO_AUTO_INSTALL=1 MRSM_NO_UPDATE_CHECK=1 CARGO_NET_OFFLINE=true
mkdir -p "$MRSM_ROOT_DIR" "$MRSM_PLUGINS_DIR"
printf '%s\n' '#[make_action]' 'fn plugin_smoke() { println!("offline Rust plugin works"); }' > "$MRSM_PLUGINS_DIR/plugin_smoke.rs"
mrsm plugin_smoke > "$root/plugin.log" 2>&1 || { cat "$root/plugin.log"; exit 1; }
grep -q "offline Rust plugin works" "$root/plugin.log"
TOOLCHAIN_SMOKE
)

for image in "${rust_images[@]}" "${rust_full_images[@]}"; do
  echo "== $image (native, no Python)"
  check "native SQL, jobs and API without Python" "$image" bash -c "$native_script"
done
for image in "${rust_images[@]}"; do
  check "runtime has no toolchain" "$image" bash -c '! command -v cargo && ! command -v rustc'
done
for image in "${rust_full_images[@]}"; do
  check "Rust build toolchain" "$image" bash -c "$toolchain_script"
done

for image in "${slim_images[@]}"; do
  echo "== $image"
  check "curl" "$image" curl --version
  check "Vim editor" "$image" vim --version
  check "python" "$image" python --version
  check "mrsm" "$image" mrsm --version
done
for image in "${full_images[@]}"; do
  echo "== $image (full)"
  check "Rust build toolchain" "$image" bash -c "$toolchain_script"
  check "curl" "$image" curl --version
  check "Vim editor" "$image" vim --version
  check "python" "$image" python --version
  check "PostgreSQL client" "$image" psql --version
  check "pgcli" "$image" pgcli --version
  check "git" "$image" git --version
  check "ssh" "$image" ssh -V
  check "mrsm" "$image" mrsm --version
  check "Python full dependencies" "$image" python -c "import meerschaum, dash, dash_bootstrap_components, dash_ace, dash_extensions, fastapi, uvicorn, a2wsgi, pandas, pyarrow, pyodbc, oracledb, duckdb, duckdb_engine, sqlalchemy_cockroachdb, webview; print(meerschaum.__version__)"
  check "Native DuckDB offline" "$image" bash -c '
    set -euo pipefail
    root="$(mktemp -d)"
    cleanup_root() { rm -rf "$root"; }
    trap cleanup_root EXIT
    mkdir -p "$root/root" "$root/plugins"
    export MRSM_ROOT_DIR="$root/root" MRSM_PLUGINS_DIR="$root/plugins" MRSM_NO_AUTO_INSTALL=1 MRSM_NO_UPDATE_CHECK=1
    export MRSM_SQL_SMOKE="duckdb:///$root/smoke.db"
    mrsm sql smoke "SELECT 42 AS value" -i sql:smoke --nopretty > "$root/query.log" 2>&1 || { cat "$root/query.log"; exit 1; }
    cat "$root/query.log"
    grep -Fq "[[42]]" "$root/query.log"
  '
  check "API healthcheck and Dash" "$image" bash -c "$api_healthcheck_script"
  check "Dash with blocked root socket directory" "$image" bash -c "$api_healthcheck_script" -- blocked
done

if [ "$failures" -gt 0 ]; then
  echo "smoke_image: $failures check(s) failed." >&2
  exit 1
fi
echo "smoke_image: all checks passed."
