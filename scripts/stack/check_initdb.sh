#!/usr/bin/env bash
# Usage: scripts/stack/check_initdb.sh [--keep] [scenario ...]
#
# Checks the stack's database setup against real containers: the `db` service of the compose file
# `mrsm stack` generates (its image, environment, command, healthcheck and the setup script mounted
# as /docker-entrypoint-initdb.d/100_meerschaum.sql), started on a fresh volume per scenario.
#
# Scenarios (default: all):
#   ha-file     timescale/timescaledb-ha:pg18 with the default file mount: the image's own setup runs
#               (telemetry off, timescaledb-tune, extensions in template1) and then ours (the four
#               extensions; pg_stat_statements preloaded and queryable).
#   ha-legacy   the same image with the v4 directory mount (`<root>/db/initdb:/docker-entrypoint-initdb.d`,
#               which hides the image's scripts): the four extensions, pg_stat_statements queryable.
#   timescaledb timescale/timescaledb:latest-pg18 (no PostGIS, no Toolkit): the first start succeeds,
#               the missing extensions are skipped with a NOTICE.
#   postgres    postgres:18 (no TimescaleDB): only pg_stat_statements.
#   postgis     postgis/postgis:17-3.5: PostGIS and pg_stat_statements, TimescaleDB skipped.
#   slow        ha-file plus a mounted setup step that sleeps: the healthcheck stays unhealthy until the
#               setup is done (the v4 check, without `-h`, reported healthy during the setup).
#
# Every container is started with `docker compose -p <prefix>-<scenario>-<random>` from the generated
# compose file plus an override (no published ports, the scenario's image and volumes), and removed
# with `down -v` (its volumes and network) when the scenario ends, even on failure. Nothing else is
# touched. The images must already be pulled (the script never pulls).
#
# Environment:
#   MRSM_CHECK_BIN     the `mrsm` command that generates the stack files (default: target/debug/mrsm,
#                      else `python -m meerschaum`). It runs with a temporary root and no `docker` on
#                      PATH, so `mrsm stack` writes its files and stops before Docker.
#   MRSM_CHECK_PREFIX  the compose project prefix (default: mrsm-initdb-check).
#   MRSM_CHECK_DIR     the parent of the scratch directory (default: ~/.cache).
#
#   --keep   leave the scratch directory (the generated root) for inspection; containers are still removed.
set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
REPO="$(dirname "$(dirname "$DIR")")"

keep=""
scenarios=()
for arg in "$@"; do
  case "$arg" in
    --keep) keep=1 ;;
    -h|--help) sed -n '2,36p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    ha-file|ha-legacy|timescaledb|postgres|postgis|slow) scenarios+=("$arg") ;;
    *) echo "check_initdb.sh: unknown argument '$arg'" >&2; exit 2 ;;
  esac
done
[ "${#scenarios[@]}" -eq 0 ] && scenarios=(ha-file ha-legacy timescaledb postgres postgis slow)

prefix="${MRSM_CHECK_PREFIX:-mrsm-initdb-check}"
### Never the default stack's project name.
case "$prefix" in
  ""|mrsm) echo "check_initdb.sh: refusing the project prefix '$prefix'." >&2; exit 2 ;;
esac

docker_bin="$(command -v docker)" || { echo "check_initdb.sh: docker is not installed." >&2; exit 2; }
compose_version="$(docker compose version --short 2>/dev/null || true)"
case "$compose_version" in
  2.2[4-9]*|2.[3-9]*|[3-9].*|[1-9][0-9].*) ;;
  *) echo "check_initdb.sh: Docker Compose >= 2.24 is required (found '${compose_version:-none}')." >&2; exit 2 ;;
esac

scratch_parent="${MRSM_CHECK_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}}"
mkdir -p "$scratch_parent"
scratch="$(mktemp -d "$scratch_parent/mrsm-initdb-check.XXXXXX")"
root="$scratch/root"
plugins="$scratch/plugins"
mkdir -p "$root" "$plugins"

projects=()
cleanup() {
  local p
  for p in "${projects[@]}"; do
    "$docker_bin" compose -p "$p" -f "$scratch/compose.yaml" -f "$scratch/$p.override.yaml" \
      down -v --remove-orphans --timeout 5 >/dev/null 2>&1 \
      || echo "check_initdb.sh: could not remove the project '$p'." >&2
  done
  if [ -z "$keep" ]; then
    rm -rf -- "$scratch"
  else
    echo "Kept $scratch"
  fi
}
trap cleanup EXIT

### 1. Generate the stack files the way `mrsm stack` does (Docker is hidden from PATH, so the action
###    writes the files and stops at "Failed to connect to the Docker engine").
if [ -n "${MRSM_CHECK_BIN:-}" ]; then
  read -r -a mrsm_cmd <<< "$MRSM_CHECK_BIN"
elif [ -x "$REPO/target/debug/mrsm" ]; then
  mrsm_cmd=("$REPO/target/debug/mrsm")
else
  mrsm_cmd=("$(command -v python3)" -P -m meerschaum)
fi
env -i HOME="$scratch" PATH="/nonexistent" LANG=C.UTF-8 PYTHONPATH="$REPO" \
  MRSM_ROOT_DIR="$root" MRSM_PLUGINS_DIR="$plugins" MRSM_NO_AUTO_INSTALL=1 \
  "${mrsm_cmd[@]}" stack config >"$scratch/generate.log" 2>&1 || true
compose_file="$root/stack/docker-compose.yaml"
setup_file="$root/db/initdb/create-extensions.sql"
for f in "$compose_file" "$setup_file"; do
  [ -f "$f" ] || { cat "$scratch/generate.log" >&2; echo "check_initdb.sh: '$f' was not generated." >&2; exit 1; }
done
cp "$compose_file" "$scratch/compose.yaml"
grep -q '/db/initdb/create-extensions.sql:/docker-entrypoint-initdb.d/100_meerschaum.sql:z,ro' "$compose_file" \
  || { echo "check_initdb.sh: the compose file does not mount the setup file." >&2; exit 1; }

export POSTGRES_USER=mrsm POSTGRES_DB=meerschaum
POSTGRES_PASSWORD="check$RANDOM$RANDOM"
export POSTGRES_PASSWORD
export VALKEY_PASSWORD="$POSTGRES_PASSWORD"

failures=0
fail() { echo "  FAIL: $*"; failures=$((failures + 1)); }
pass() { echo "  ok:   $*"; }

### override <project> <image> <data target> <volume lines...>
override() {
  local project="$1" image="$2" data="$3"; shift 3
  {
    echo "services:"
    echo "  db:"
    echo "    image: $image"
    echo "    ports: !reset []"
    echo "    volumes: !override"
    echo "      - meerschaum_db_data:$data"
    local v
    for v in "$@"; do echo "      - $v"; done
  } > "$scratch/$project.override.yaml"
}

### up <project>: start the `db` service and print its container id (runs in a subshell).
up() {
  local project="$1"
  "$docker_bin" compose -p "$project" -f "$scratch/compose.yaml" -f "$scratch/$project.override.yaml" \
    up -d --pull never --no-deps db >"$scratch/$project.up.log" 2>&1 || return 1
  "$docker_bin" compose -p "$project" -f "$scratch/compose.yaml" -f "$scratch/$project.override.yaml" ps -q db
}

start() {
  local project="$1"
  cid="$(up "$project")" && [ -n "$cid" ] && return 0
  fail "docker compose up ($project)"
  sed 's/^/    | /' "$scratch/$project.up.log"
  return 1
}

down() {
  local project="$1" p rest=()
  "$docker_bin" compose -p "$project" -f "$scratch/compose.yaml" -f "$scratch/$project.override.yaml" \
    down -v --remove-orphans --timeout 5 >/dev/null 2>&1 || fail "docker compose down ($project)"
  for p in "${projects[@]}"; do [ "$p" = "$project" ] || rest+=("$p"); done
  projects=("${rest[@]}")
}

### logs <container>: the container's log (captured first: `grep -q` closing a pipe early would make
### `docker logs` fail under `pipefail`).
logs() { "$docker_bin" logs "$1" 2>&1 || true; }

health() { "$docker_bin" inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null || echo gone; }

wait_healthy() {
  local cid="$1" limit="${2:-180}" i status
  for ((i = 0; i < limit; i++)); do
    status="$(health "$cid")"
    [ "$status" = healthy ] && { pass "healthy after ~${i}s"; return 0; }
    sleep 1
  done
  fail "not healthy after ${limit}s (status: $status)"
  logs "$cid" | tail -n 40 | sed 's/^/    | /'
  return 1
}

sql() {
  local cid="$1" db="$2" query="$3"
  "$docker_bin" exec -e PGPASSWORD="$POSTGRES_PASSWORD" "$cid" \
    psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$db" -XtAq -v ON_ERROR_STOP=1 -c "$query" 2>&1
}

expect_sql() {
  local cid="$1" db="$2" query="$3" want="$4" label="$5" got
  got="$(sql "$cid" "$db" "$query" || true)"
  if [ "$got" = "$want" ]; then pass "$label = $got"; else fail "$label: expected '$want', got '$got'"; fi
}

extensions_query="SELECT coalesce(string_agg(extname, ',' ORDER BY extname), '') FROM pg_extension
  WHERE extname IN ('timescaledb', 'postgis', 'timescaledb_toolkit', 'pg_stat_statements')"

expect_statements() {
  local cid="$1" got
  if got="$(sql "$cid" "$POSTGRES_DB" "SELECT count(*) >= 0 FROM pg_stat_statements")" && [ "$got" = t ]; then
    pass "pg_stat_statements is queryable"
  else
    fail "pg_stat_statements is not queryable: $got"
  fi
}

expect_notice() {
  local cid="$1" ext="$2"
  if logs "$cid" | grep -q "Meerschaum: skipping the extension \"$ext\""; then
    pass "NOTICE: skipped $ext"
  else
    fail "no NOTICE for the skipped extension $ext"
  fi
}

all_four="pg_stat_statements,postgis,timescaledb,timescaledb_toolkit"
mount_file="$setup_file:/docker-entrypoint-initdb.d/100_meerschaum.sql:z,ro"
mount_dir="$root/db/initdb:/docker-entrypoint-initdb.d:z,ro"

### tune_ran <container>: timescaledb-tune uncomments and sets its settings in postgresql.conf
### (the image's sample file only has them commented out).
tune_ran() {
  local cid="$1" conf
  conf="$(sql "$cid" "$POSTGRES_DB" "SHOW config_file")"
  "$docker_bin" exec "$cid" grep -Eq '^(timescaledb\.max_background_workers|effective_cache_size) *=' "$conf"
}

scenario_ha_file() {
  local project="$1" cid conf
  override "$project" timescale/timescaledb-ha:pg18 /home/postgres/pgdata "$mount_file"
  start "$project" || return 0
  wait_healthy "$cid" || return 0
  expect_sql "$cid" "$POSTGRES_DB" "SHOW timescaledb.telemetry_level" off "timescaledb.telemetry_level"
  expect_sql "$cid" "$POSTGRES_DB" "$extensions_query" "$all_four" "extensions in $POSTGRES_DB"
  expect_sql "$cid" template1 "SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'" 1 \
    "timescaledb in template1"
  expect_statements "$cid"
  expect_sql "$cid" "$POSTGRES_DB" \
    "SELECT 'pg_stat_statements' = ANY (string_to_array(replace(current_setting('shared_preload_libraries'), ' ', ''), ','))" \
    t "pg_stat_statements preloaded"
  if tune_ran "$cid"; then
    pass "timescaledb-tune ran (postgresql.conf has its settings)"
  else
    fail "timescaledb-tune did not run"
  fi
}

scenario_ha_legacy() {
  local project="$1" cid
  override "$project" timescale/timescaledb-ha:pg18 /home/postgres/pgdata "$mount_dir"
  start "$project" || return 0
  wait_healthy "$cid" || return 0
  expect_sql "$cid" "$POSTGRES_DB" "$extensions_query" "$all_four" "extensions in $POSTGRES_DB"
  expect_statements "$cid"
  ### The directory mount hides the image's scripts (why new stacks mount the file instead).
  if tune_ran "$cid"; then
    fail "timescaledb-tune ran although the directory mount hides it (is the tune check wrong?)"
  else
    pass "timescaledb-tune did not run (hidden by the directory mount)"
  fi
  expect_sql "$cid" "$POSTGRES_DB" "SHOW timescaledb.telemetry_level" basic \
    "timescaledb.telemetry_level (the image's opt-out is hidden too)"
}

scenario_timescaledb() {
  local project="$1" cid
  override "$project" timescale/timescaledb:latest-pg18 /var/lib/postgresql "$mount_file"
  start "$project" || return 0
  wait_healthy "$cid" || return 0
  expect_sql "$cid" "$POSTGRES_DB" "$extensions_query" "pg_stat_statements,timescaledb" "extensions in $POSTGRES_DB"
  expect_notice "$cid" postgis
  expect_notice "$cid" timescaledb_toolkit
  expect_statements "$cid"
}

scenario_postgres() {
  local project="$1" cid
  override "$project" postgres:18 /var/lib/postgresql "$mount_file"
  start "$project" || return 0
  wait_healthy "$cid" || return 0
  expect_sql "$cid" "$POSTGRES_DB" "$extensions_query" "pg_stat_statements" "extensions in $POSTGRES_DB"
  expect_notice "$cid" timescaledb
  expect_notice "$cid" postgis
  expect_statements "$cid"
}

scenario_postgis() {
  local project="$1" cid
  override "$project" postgis/postgis:17-3.5 /var/lib/postgresql/data "$mount_file"
  start "$project" || return 0
  wait_healthy "$cid" || return 0
  expect_sql "$cid" "$POSTGRES_DB" "$extensions_query" "pg_stat_statements,postgis" "extensions in $POSTGRES_DB"
  expect_notice "$cid" timescaledb
  expect_statements "$cid"
}

scenario_slow() {
  local project="$1" cid i status done_setup old_ok=0 early=0 old_check new_check log
  printf 'echo "check_initdb: slow setup step"\nsleep 30\n' > "$scratch/050_slow.sh"
  override "$project" timescale/timescaledb-ha:pg18 /home/postgres/pgdata "$mount_file" \
    "$scratch/050_slow.sh:/docker-entrypoint-initdb.d/050_slow.sh:z,ro"
  start "$project" || return 0
  old_check="pg_isready -d $POSTGRES_DB -U $POSTGRES_USER"
  new_check="$("$docker_bin" inspect -f '{{index .Config.Healthcheck.Test 1}}' "$cid")"
  echo "  healthcheck: $new_check"
  for ((i = 0; i < 180; i++)); do
    done_setup=0
    log="$(logs "$cid")"
    case "$log" in *"init process complete"*) done_setup=1 ;; esac
    status="$(health "$cid")"
    if [ "$done_setup" = 0 ]; then
      [ "$status" = healthy ] && early=1
      if [[ "$log" == *"check_initdb: slow setup step"* ]] \
        && "$docker_bin" exec "$cid" sh -c "$old_check" >/dev/null 2>&1; then
        old_ok=$((old_ok + 1))
      fi
    elif [ "$status" = healthy ]; then
      break
    fi
    sleep 1
  done
  if [ "$early" = 0 ]; then pass "never healthy during the setup"; else fail "healthy before the setup finished"; fi
  if [ "$old_ok" -gt 0 ]; then
    pass "the v4 check ($old_check) passed $old_ok time(s) during the setup"
  else
    echo "  note: the v4 check never passed during the setup"
  fi
  if [ "$status" = healthy ]; then pass "healthy after the setup (~${i}s)"; else fail "not healthy after the setup (status: $status)"; fi
  expect_statements "$cid"
}

echo "Generated by: ${mrsm_cmd[*]} (root $root)"
for scenario in "${scenarios[@]}"; do
  project="$prefix-$scenario-$RANDOM"
  projects+=("$project")
  echo "== $scenario (project $project)"
  "scenario_${scenario//-/_}" "$project"
  down "$project"
done

if [ "$failures" -gt 0 ]; then
  echo "check_initdb.sh: $failures check(s) failed."
  exit 1
fi
echo "check_initdb.sh: all checks passed."
