#!/usr/bin/env bash
# Usage: scripts/rust/ci.sh [STEP ...]
#
# Runs the same commands as .github/workflows/rust.yaml so every CI job can be reproduced locally.
# With no STEP, runs: fmt clippy test build.
#
# Steps:
#   setup        Ensure rustup/cargo exist (installs rustup non-interactively if ~/.cargo/bin/cargo
#                is missing) and install the toolchain pinned by rust-toolchain.toml.
#   disk-guard   Run `cargo clean` when free space on the target dir's filesystem is below
#                MRSM_CI_MIN_FREE_GB (default 10). Self-hosted runner safety valve.
#   fmt          cargo fmt --all --check
#   clippy       cargo clippy --all-targets -- -D warnings
#   doc          RUSTDOCFLAGS="-D warnings" cargo doc --workspace --exclude meerschaum-py --no-deps --all-features
#                (broken or private intra-doc links fail here instead of on docs.rs after a publish)
#   test         cargo nextest run --workspace (when nextest is installed: `ci_tools.sh`, self-hosted runner) plus
#                cargo test --workspace --doc; plain `cargo test --workspace` without nextest. With DuckDB's shared
#                library from the persistent cache (`duckdb-lib`) and the DuckDB integration tests enabled
#                (MRSM_TEST_DUCKDB_PATH). MRSM_CI_NEXTEST=0 forces `cargo test`. Creates `.venv-ref/` first (the
#                cross-implementation tests need it) unless MRSM_TEST_PYTHON is set.
#   duckdb-platform  The DuckDB loader on the platform running it (the hosted Linux arm64 / macOS / Windows
#                legs): the loader unit tests, then `--test duckdb_install` (downloads the pinned library of
#                this platform with the installer, SHA-256 verified, into a temporary root, loads it, runs a
#                query) and `--test duckdb_library` (every type, parameter and timezone check, the library
#                downloaded on first use into MRSM_ROOT_DIR or a temporary root). Needs network access.
#   duckdb-lib   Print the path of DuckDB's official shared library, downloading it (checksum from the
#                Rust source) into `${MRSM_CI_CACHE:-$HOME/.cache/mrsm-ci}` on the first run only; on
#                GitHub it exports MRSM_DUCKDB_LIB to later steps. (scripts/rust/duckdb_lib.sh)
#   generated    Fail when native/packages_table.rs or native/bootstrap_templates.json differ from
#                their generators (scripts/rust/gen_*.py; reads the Python sources as text).
#   bin-debug    cargo build -p meerschaum --bin mrsm (target/debug/mrsm: the standalone binary of this tree, which
#                the pytest job passes to the tests that need one as MRSM_TEST_BIN; see tests/_binary.py)
#   build        cargo build --release -p meerschaum --bin mrsm (the standalone `mrsm`, what `cargo install meerschaum` builds)
#   check        cargo check --workspace            (the hosted ubuntu/windows/macos legs)
#   extension    Build the `meerschaum._core._native` extension into the working tree (debug profile; maturin in
#                a scratch venv, scripts/rust/build_py.sh, which takes the tree's build lock) for the cargo tests that start
#                real Python workers (they import `meerschaum`, which requires it). PYTHON_BIN picks the
#                interpreter (default python3); on GitHub it also exports MRSM_PYTHON to later steps.
#   env          Print `export` lines for the build accelerators (below) for a workflow step that runs cargo or
#                maturin itself: `eval "$(scripts/rust/ci.sh env)"`.
#   frontcrate   Compile the `meerschaum` front crate with `--no-default-features`, with each of
#                `sql cli valkey api engine server plugin` alone (`cli` builds the `mrsm` binary), and
#                `cargo test -p meerschaum --features full` (the `meerschaum` features are otherwise only built together by the workspace).
#   package      cargo package --workspace --no-verify (the crates.io tarballs: manifests, `include` lists,
#                lockstep versions) then scripts/rust/check_packages.py (size < 10 MB, no tests/fixtures/
#                benches). Fast: nothing is compiled. MRSM_PACKAGE_ALLOW_DIRTY=1 allows a dirty tree.
#   publish-dry-run  cargo publish --workspace --dry-run: also builds every tarball against crates.io
#                (without the root `[patch]`, so plain sqlx) in target/package/. Slow; no credentials
#                needed, nothing is uploaded. The real `cargo publish` is never run by CI.
#   parity       Regenerate every crates/*/tests/fixtures/gen_*.py with the v4.1.4 reference
#                interpreter (scripts/parity/) and fail if the fixtures differ from git. Detailed
#                generator output is saved separately; a failed generator also prints its full log.
#
# Build accelerators (SELF-HOSTED Linux runners only: `RUNNER_ENVIRONMENT=self-hosted`, or MRSM_CI_ACCEL=1 to force
# them elsewhere, MRSM_CI_ACCEL=0 to disable; GitHub-hosted runners use Swatinem/rust-cache instead):
#   sccache   RUSTC_WRAPPER with an on-disk cache shared by every runner of the host (one server per runner, on a
#             port derived from RUNNER_NAME, because a runner kills the processes of its job when the job ends):
#             SCCACHE_DIR=${MRSM_CI_CACHE:-$HOME/.cache/mrsm-ci}/sccache, SCCACHE_CACHE_SIZE=${MRSM_CI_SCCACHE_SIZE:-20G}.
#   mold      OPT-IN (MRSM_CI_MOLD=1): `-C link-arg=-fuse-ld=mold` appended to RUSTFLAGS. Measured on the debug
#             `mrsm` (280 MB): rustc's default lld links in 1.3 s, mold in 0.45 s, GNU ld in 12 s. Rust >= 1.90
#             already links with lld on x86_64 Linux, so the gain is under a second per binary, and the RUSTFLAGS
#             change makes the first build after enabling it a cold one: off by default.
#   nextest   used by the `test` step when installed.
# `scripts/rust/ci_tools.sh` installs the pinned versions into ${MRSM_CI_CACHE:-$HOME/.cache/mrsm-ci}/bin (no sudo);
# ci.sh runs it once when a tool is missing (MRSM_CI_TOOLS_INSTALL=0 skips that).
#
# Never set CARGO_TARGET_DIR: the shared `target/` from .cargo/config.toml is the build cache.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO"

CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
export PATH="$CARGO_HOME/bin:$PATH"

if [ -n "${CARGO_TARGET_DIR:-}" ]; then
  echo "ci.sh: CARGO_TARGET_DIR is set ($CARGO_TARGET_DIR); unset it to use the shared target/." >&2
  exit 2
fi

log() { printf '\n==> %s\n' "$*"; }
logerr() { printf '\n==> %s\n' "$*" >&2; }

CI_CACHE="${MRSM_CI_CACHE:-$HOME/.cache/mrsm-ci}"
CI_ACCEL=0
case "${MRSM_CI_ACCEL:-auto}" in
  1) CI_ACCEL=1 ;;
  0) CI_ACCEL=0 ;;
  *) [ "${RUNNER_ENVIRONMENT:-}" = "self-hosted" ] && CI_ACCEL=1 ;;
esac
[ "$(uname -s)" = Linux ] || CI_ACCEL=0

# Put the accelerators in the environment of every cargo/maturin process this script starts.
ci_accel() {
  [ "$CI_ACCEL" -eq 1 ] || return 0
  if [ "${MRSM_CI_TOOLS_INSTALL:-1}" != 0 ]; then
    local missing=()
    [ -x "$CI_CACHE/bin/sccache" ] || missing+=(sccache)
    [ -x "$CI_CACHE/bin/cargo-nextest" ] || missing+=(nextest)
    [ "${MRSM_CI_MOLD:-0}" != 1 ] || [ -x "$CI_CACHE/bin/mold" ] || missing+=(mold)
    if [ "${#missing[@]}" -gt 0 ]; then
      MRSM_CI_CACHE="$CI_CACHE" "$REPO/scripts/rust/ci_tools.sh" "${missing[@]}" \
        || echo "ci.sh: could not install the build accelerators; building without them." >&2
    fi
  fi
  export PATH="$CI_CACHE/bin:$PATH"
  if [ "${MRSM_CI_SCCACHE:-1}" != 0 ] && command -v sccache >/dev/null 2>&1; then
    export RUSTC_WRAPPER="$(command -v sccache)"
    export SCCACHE_DIR="${SCCACHE_DIR:-$CI_CACHE/sccache}"
    export SCCACHE_CACHE_SIZE="${SCCACHE_CACHE_SIZE:-${MRSM_CI_SCCACHE_SIZE:-20G}}"
    # One sccache server per runner: the runner kills the processes a job started when the job ends, which
    # would cut a server another runner's compile is using. The servers share the disk cache, not a port.
    if [ -z "${SCCACHE_SERVER_PORT:-}" ]; then
      SCCACHE_SERVER_PORT=$((43000 + $(printf '%s' "${RUNNER_NAME:-$(id -un)}" | cksum | cut -d' ' -f1) % 1000))
      export SCCACHE_SERVER_PORT
    fi
    mkdir -p "$SCCACHE_DIR"
  fi
  if [ "${MRSM_CI_MOLD:-0}" = 1 ] && command -v mold >/dev/null 2>&1; then
    case " ${RUSTFLAGS:-} " in
      *" -C link-arg=-fuse-ld=mold "*) ;;
      *) export RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-C link-arg=-fuse-ld=mold" ;;
    esac
  fi
}

ci_have_nextest() {
  [ "${MRSM_CI_NEXTEST:-1}" != 0 ] && cargo nextest --version >/dev/null 2>&1
}

ci_accel_stats() {
  if [ -n "${RUSTC_WRAPPER:-}" ] && [ -x "$RUSTC_WRAPPER" ]; then
    logerr "sccache statistics (cache $SCCACHE_DIR, limit $SCCACHE_CACHE_SIZE)"
    "$RUSTC_WRAPPER" --show-stats 2>&1 | grep -E "Compile requests|Cache hits|Cache misses|Cache hits rate|Cache size|Max cache" >&2 || true
  fi
}

step_setup() {
  if [ ! -x "$CARGO_HOME/bin/cargo" ] && ! command -v cargo >/dev/null 2>&1; then
    log "Installing rustup (non-interactive)"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain none --no-modify-path
    # Later workflow steps run in fresh shells; make the new cargo visible to them.
    if [ -n "${GITHUB_PATH:-}" ]; then
      echo "$CARGO_HOME/bin" >> "$GITHUB_PATH"
    fi
  fi
  log "Installing the toolchain from rust-toolchain.toml"
  # rustup >= 1.28 no longer installs on `rustup show`; `toolchain install` with no
  # argument installs the toolchain (and components) named by rust-toolchain.toml.
  rustup toolchain install || rustup show
  rustup show active-toolchain
  cargo --version
  rustc --version
}

target_directory() {
  # Runners may keep target/ on a different filesystem behind a symlink.
  mkdir -p "$REPO/target"
  (cd "$REPO/target" && pwd -P)
}

free_gb() {
  # Measure the filesystem Cargo writes to, not the checkout's filesystem.
  df -Pk "$(target_directory)" | awk 'NR==2 { printf "%d", $4 / 1048576 }'
}

step_disk_guard() {
  local min="${MRSM_CI_MIN_FREE_GB:-10}" free target
  target="$(target_directory)"
  free="$(free_gb)"
  log "Free space: ${free} GiB (threshold ${min} GiB); $target is $(du -sh "$target" 2>/dev/null | cut -f1 || echo 0)"
  if [ "$free" -lt "$min" ]; then
    # Drop the cheapest-to-rebuild layers first: the optimized profiles (`release` is only built by the
    # dispatch/tag jobs, `ci` by pytest), then everything.
    log "Below ${min} GiB: cargo clean --release, then the ci profile"
    cargo clean --release || true
    cargo clean --profile ci || true
    free="$(free_gb)"
    log "Free space after dropping release and ci: ${free} GiB"
    if [ "$free" -lt "$min" ]; then
      log "Still below ${min} GiB: running cargo clean"
      cargo clean
      log "Free space after clean: $(free_gb) GiB"
    fi
  fi
}

step_fmt()    { log "cargo fmt --all --check"; cargo fmt --all --check; }
step_clippy() { log "cargo clippy --all-targets -- -D warnings"; cargo clippy --all-targets -- -D warnings; }
step_doc() {
  log "cargo doc --workspace --no-deps --all-features (-D warnings)"
  RUSTDOCFLAGS="${RUSTDOCFLAGS:+$RUSTDOCFLAGS }-D warnings" \
    cargo doc --workspace --exclude meerschaum-py --no-deps --all-features
}
step_duckdb_platform() {
  local root
  root="${MRSM_ROOT_DIR:-}"
  if [ -z "$root" ]; then
    root="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/mrsm-duckdb-root.XXXXXX")"
  fi
  mkdir -p "$root"
  # The library comes from the pinned download, never from the host: drop any override.
  unset MRSM_DUCKDB_LIB MRSM_NO_AUTO_INSTALL
  log "DuckDB loader unit tests"
  cargo test -p meerschaum-connector-sql --lib duckdb
  log "DuckDB: download, verify, load (root $root)"
  MRSM_TEST_DUCKDB_DOWNLOAD=1 MRSM_ROOT_DIR="$root" \
    cargo test -p meerschaum-connector-sql --test duckdb_install
  log "DuckDB: the type, parameter and timezone tests against the downloaded library"
  MRSM_TEST_DUCKDB_DOWNLOAD=1 MRSM_ROOT_DIR="$root" \
    cargo test -p meerschaum-connector-sql --test duckdb_library
}
step_duckdb_lib() { log "DuckDB shared library"; scripts/rust/duckdb_lib.sh; }
step_test()   {
  # Test processes need an isolated root; ordinary cargo commands use normal runtime paths.
  local test_scratch
  test_scratch="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/mrsm-rust-tests.XXXXXX")"
  trap "rm -rf '$test_scratch'" EXIT
  if [ -z "${MRSM_ROOT_DIR+x}" ]; then
    mkdir -p "$test_scratch/root"
    export MRSM_ROOT_DIR="$test_scratch/root"
  fi
  if [ -z "${MRSM_PLUGINS_DIR+x}" ]; then
    mkdir -p "$test_scratch/plugins"
    export MRSM_PLUGINS_DIR="$test_scratch/plugins"
  fi
  # DuckDB is not compiled in: its own library is loaded at run time. Tests run with
  # `MRSM_NO_AUTO_INSTALL=1` in CI, so provide it from the persistent cache (one download per
  # DuckDB version on the runner, never per run) unless the caller already set MRSM_DUCKDB_LIB.
  if [ -z "${MRSM_DUCKDB_LIB:-}" ]; then
    MRSM_DUCKDB_LIB="$(scripts/rust/duckdb_lib.sh)"
    export MRSM_DUCKDB_LIB
  fi
  if [ -z "${MRSM_TEST_DUCKDB_PATH+x}" ]; then
    MRSM_TEST_DUCKDB_PATH="$test_scratch/duckdb"
    mkdir -p "$MRSM_TEST_DUCKDB_PATH"
    export MRSM_TEST_DUCKDB_PATH
  fi
  # The cross-implementation tests (worker `plugins`, `compose_execution`, `wiring_parity`, ...) run the
  # released v4 from `.venv-ref/`, which exists only on a runner that already ran the `parity` job: each
  # runner has its own workspace, so build it here (idempotent: a no-op when it is current) instead of
  # depending on which runner picked the job. MRSM_TEST_PYTHON pins another interpreter explicitly.
  if [ -z "${MRSM_TEST_PYTHON:-}" ]; then
    "$REPO/scripts/parity/setup_reference.sh"
  fi
  if ci_have_nextest; then
    # nextest runs every test in its own process (and does not run doc tests: those follow).
    log "cargo nextest run --workspace --no-fail-fast (DuckDB library: $MRSM_DUCKDB_LIB)"
    cargo nextest run --workspace --no-fail-fast
    log "cargo test --workspace --doc"
    cargo test --workspace --doc
  else
    log "cargo test --workspace (DuckDB library: $MRSM_DUCKDB_LIB)"
    cargo test --workspace
  fi
}
step_build()  { log "cargo build --release -p meerschaum --bin mrsm"; cargo build --release -p meerschaum --bin mrsm; }
step_bin_debug() { log "cargo build -p meerschaum --bin mrsm"; cargo build -p meerschaum --bin mrsm; }
step_generated() { log "generated tables match their generators"; cargo test -p meerschaum-cli --test generated_tables; }
step_extension() {
  local python scratch
  python="$(command -v "${PYTHON_BIN:-python3}")"
  scratch="$(mktemp -d "${RUNNER_TEMP:-$HOME/.cache}/mrsm-ext-build.XXXXXX")"
  trap "rm -rf '$scratch'" EXIT
  local profile="${MRSM_CI_EXT_PROFILE:-debug}" profile_args
  case "$profile" in
    debug) profile_args=(--debug) ;;
    release) profile_args=(--release) ;;
    *) profile_args=(--profile "$profile") ;;
  esac
  log "Building the native extension ($profile) with $python"
  "$python" -m venv "$scratch/tools"
  "$scratch/tools/bin/python" -m pip install --quiet 'maturin>=1.9,<2.0'
  # Scratch root and plugins for build_py.sh's import check (never the runner's config).
  mkdir -p "$scratch/root" "$scratch/plugins"
  # `build_py.sh` takes the tree's build lock (target/.build-py.lock) itself. `--debug`: the same
  # profile `cargo test --workspace` compiles, so most dependencies are shared (the release build
  # would add a second, optimized copy of every one); the debug extension keeps the oracle flavor (DuckDB's
  # own library is loaded at run time).
  PYTHON="$python" MATURIN_BIN="$scratch/tools/bin/maturin" PYTHONPATH="$REPO" \
    MRSM_ROOT_DIR="$scratch/root" MRSM_PLUGINS_DIR="$scratch/plugins" \
    scripts/rust/build_py.sh "${profile_args[@]}"
  if [ -n "${GITHUB_ENV:-}" ] && [ "${MRSM_CI_EXPORT_PYTHON:-1}" != 0 ]; then
    echo "MRSM_PYTHON=$python" >> "$GITHUB_ENV"
  fi
}
step_frontcrate() {
  log "cargo check -p meerschaum --no-default-features --all-targets"
  cargo check -p meerschaum --no-default-features --all-targets
  local feature
  for feature in sql cli valkey api engine server plugin; do
    log "cargo check -p meerschaum --no-default-features --all-targets --features $feature"
    cargo check -p meerschaum --no-default-features --all-targets --features "$feature"
  done
  log "cargo test -p meerschaum --features full"
  cargo test -p meerschaum --features full
}
step_package() {
  log "cargo package --workspace --no-verify"
  local flags=(--workspace --exclude meerschaum-py --no-verify --locked)
  [ -n "${MRSM_PACKAGE_ALLOW_DIRTY:-}" ] && flags+=(--allow-dirty)
  find "$REPO/target/package" -maxdepth 1 -name "*.crate" -delete 2>/dev/null || true
  cargo package "${flags[@]}"
  python3 "$REPO/scripts/rust/check_packages.py"
}
step_publish_dry_run() {
  log "cargo publish --workspace --dry-run (builds the tarballs; never uploads)"
  # Registry crates are fingerprinted by name and version only: a previous run's temporary registry or
  # its compiled `tmp-crate` artifacts would be reused for the same version, so a later crate would
  # verify against a stale build of its workspace dependencies. Start both from scratch.
  find "$REPO/target/package" -maxdepth 1 -name "*.crate" -delete 2>/dev/null || true
  # Cargo also extracts the overlay registry's crates into `$CARGO_HOME/registry/src/-<hash>/` (the hash
  # of the unchanged tmp-registry path) and never re-extracts them: remove those sources too.
  find "$CARGO_HOME/registry/src" -mindepth 2 -maxdepth 2 -path "$CARGO_HOME/registry/src/-*" \
    -name "meerschaum*" -exec rm -rf {} + 2>/dev/null || true
  # The overlay crates' compiled units are cached in the shared `target/debug` under the same name and
  # version (a registry source is fingerprinted by version only): drop every unit whose dep-info
  # points into the extracted overlay sources.
  python3 - "$REPO/target" <<'PY'
import pathlib, sys
target = pathlib.Path(sys.argv[1])
for profile in ("debug", "release"):
    deps = target / profile / "deps"
    if not deps.is_dir():
        continue
    for info in deps.glob("*.d"):
        try:
            if "/registry/src/-" not in info.read_text(errors="ignore"):
                continue
        except OSError:
            continue
        unit_hash = info.stem.rsplit("-", 1)[-1]
        for path in deps.glob(f"*-{unit_hash}.*"):
            path.unlink(missing_ok=True)
        fingerprints = target / profile / ".fingerprint"
        for path in fingerprints.glob(f"*-{unit_hash}"):
            for child in path.iterdir():
                child.unlink(missing_ok=True)
            path.rmdir()
PY
  for d in tmp-registry tmp-crate; do
    [ -d "$REPO/target/package/$d" ] && find "$REPO/target/package/$d" -delete
  done
  local flags=(--workspace --exclude meerschaum-py --dry-run --locked)
  [ -n "${MRSM_PACKAGE_ALLOW_DIRTY:-}" ] && flags+=(--allow-dirty)
  cargo publish "${flags[@]}"
}
step_check()  { log "cargo check --workspace"; cargo check --workspace; }

step_parity() {
  shopt -s nullglob
  local gens=(crates/*/tests/fixtures/gen_*.py)
  shopt -u nullglob
  if [ "${#gens[@]}" -eq 0 ]; then
    log "No crates/*/tests/fixtures/gen_*.py generators yet; nothing to check."
    return 0
  fi
  "$REPO/scripts/parity/setup_reference.sh"
  local gen gen_log status logs_dir
  mkdir -p "$REPO/target"
  logs_dir="$(mktemp -d "$REPO/target/parity-logs.XXXXXX")"
  log "Detailed generator output: $logs_dir"
  if [ -n "${GITHUB_ENV:-}" ]; then
    printf 'MRSM_PARITY_LOGS_DIR=%s\n' "$logs_dir" >> "$GITHUB_ENV"
  fi
  for gen in "${gens[@]}"; do
    log "Regenerating with the reference interpreter: $gen"
    gen_log="$logs_dir/${gen//\//__}.log"
    if "$REPO/scripts/parity/run.sh" "$gen" > "$gen_log" 2>&1; then
      printf '[parity] passed: %s (detailed output retained)\n' "$gen"
    else
      status=$?
      printf '[parity] failed: %s\n' "$gen" >&2
      cat "$gen_log" >&2
      return "$status"
    fi
  done
  log "Checking that the regenerated fixtures match git"
  local dirs=()
  for gen in "${gens[@]}"; do dirs+=("$(dirname "$gen")"); done
  mapfile -t dirs < <(printf '%s\n' "${dirs[@]}" | sort -u)
  local rc=0 clean=1
  # Recorded fixtures must never carry machine-specific or sensitive content.
  "$REPO/scripts/parity/check_fixture_hygiene.sh" "${dirs[@]}" || { rc=1; clean=0; }
  if ! git diff --stat --exit-code -- "${dirs[@]}"; then
    rc=1
    # Print the (truncated) diff so a runner-only difference can be diagnosed from the log, but
    # only when the regenerated files passed the hygiene check (never echo a leak into the log).
    if [ "$clean" -eq 1 ]; then
      git diff -- "${dirs[@]}" | head -n 400
    fi
  fi
  local untracked
  untracked="$(git ls-files --others --exclude-standard -- "${dirs[@]}")"
  if [ -n "$untracked" ]; then
    echo "Untracked fixture files were generated (commit them or stop generating them):"
    echo "$untracked"
    rc=1
  fi
  if [ "$rc" -ne 0 ]; then
    echo "Parity fixtures differ from the v4.1.4 reference. Regenerate with scripts/parity/run.sh and commit." >&2
  fi
  return "$rc"
}

steps=("$@")
[ "${#steps[@]}" -eq 0 ] && steps=(fmt clippy test build)

ci_accel

for s in "${steps[@]}"; do
  case "$s" in
    setup)      step_setup ;;
    disk-guard) step_disk_guard ;;
    fmt)        step_fmt ;;
    clippy)     step_clippy ;;
    doc)        step_doc ;;
    test)       step_test ;;
    duckdb-lib) step_duckdb_lib ;;
    duckdb-platform) step_duckdb_platform ;;
    build)      step_build ;;
    bin-debug)  step_bin_debug ;;
    generated)  step_generated ;;
    check)      step_check ;;
    extension)  step_extension ;;
    frontcrate) step_frontcrate ;;
    package)    step_package ;;
    publish-dry-run) step_publish_dry_run ;;
    parity)     step_parity ;;
    env)
      for v in PATH RUSTC_WRAPPER SCCACHE_DIR SCCACHE_CACHE_SIZE SCCACHE_SERVER_PORT RUSTFLAGS; do
        [ -n "${!v:-}" ] && printf 'export %s=%q\n' "$v" "${!v}"
      done
      ;;
    -h|--help)  sed -n '2,65p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "ci.sh: unknown step '$s' (see --help)" >&2; exit 2 ;;
  esac
done

[ "${steps[*]}" = env ] || ci_accel_stats
