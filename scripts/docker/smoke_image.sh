#!/usr/bin/env bash
# Usage: scripts/docker/smoke_image.sh [--full-image IMAGE] [--slim-image IMAGE] [--image IMAGE ...]
#
# Smoke test of built Meerschaum images (run by scripts/docker/buildx.sh after a local build):
#   - every image has `curl` (the stack's API healthcheck is
#     `["CMD", "curl", "--fail", "http://localhost:<port>/healthcheck"]`) and a working `mrsm`;
#   - the full image also has `git` (plugins may require `git+https://...` packages) and `ssh`;
#   - the full image runs that healthcheck for real: `mrsm start api --production` on a scratch SQLite
#     instance (no web console, no terminal, a random port) and `curl --fail` against it in the same
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
while [ $# -gt 0 ]; do
  case "$1" in
    --full-image) [ -n "$2" ] && full_images+=("$2"); shift 2 ;;
    --slim-image|--image) [ -n "$2" ] && slim_images+=("$2"); shift 2 ;;
    -h|--help) sed -n '2,18p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "smoke_image.sh: unknown argument '$1'" >&2; exit 2 ;;
  esac
done
if [ "${#full_images[@]}" -eq 0 ] && [ "${#slim_images[@]}" -eq 0 ]; then
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
export MRSM_ROOT_DIR="$root/root" MRSM_PLUGINS_DIR="$root/plugins" MRSM_NO_AUTO_INSTALL=1
export MRSM_SQL_SMOKE="sqlite:///$root/smoke.db"
mrsm start api --production --port "$port" -i sql:smoke --no-dash --no-webterm >"$root/api.log" 2>&1 &
api=$!
for _ in $(seq 1 90); do
  if curl --fail --silent "http://localhost:$port/healthcheck" 2>/dev/null; then
    echo
    echo "curl --fail http://localhost:$port/healthcheck answered"
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

for image in "${slim_images[@]}"; do
  echo "== $image"
  check "curl" "$image" curl --version
  check "mrsm" "$image" mrsm --version
done
for image in "${full_images[@]}"; do
  echo "== $image (full)"
  check "curl" "$image" curl --version
  check "git" "$image" git --version
  check "ssh" "$image" ssh -V
  check "mrsm" "$image" mrsm --version
  check "API healthcheck" "$image" bash -c "$api_healthcheck_script"
done

if [ "$failures" -gt 0 ]; then
  echo "smoke_image: $failures check(s) failed." >&2
  exit 1
fi
echo "smoke_image: all checks passed."
