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
#   test         cargo test --workspace, with DuckDB's shared library from the persistent cache
#                (`duckdb-lib`) and the DuckDB integration tests enabled (MRSM_TEST_DUCKDB_PATH).
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
#                interpreter (scripts/parity/) and fail if the fixtures differ from git.
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

free_gb() {
  # Free space (GiB, rounded down) on the filesystem holding the repository.
  df -Pk "$REPO" | awk 'NR==2 { printf "%d", $4 / 1048576 }'
}

step_disk_guard() {
  local min="${MRSM_CI_MIN_FREE_GB:-10}" free
  free="$(free_gb)"
  log "Free space: ${free} GiB (threshold ${min} GiB); target/ is $(du -sh target 2>/dev/null | cut -f1 || echo 0)"
  if [ "$free" -lt "$min" ]; then
    log "Below ${min} GiB: running cargo clean"
    cargo clean
    log "Free space after clean: $(free_gb) GiB"
  fi
}

step_fmt()    { log "cargo fmt --all --check"; cargo fmt --all --check; }
step_clippy() { log "cargo clippy --all-targets -- -D warnings"; cargo clippy --all-targets -- -D warnings; }
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
  # DuckDB is not compiled in: its own library is loaded at run time. Tests run with
  # `MRSM_NO_AUTO_INSTALL=1` in CI, so provide it from the persistent cache (one download per
  # DuckDB version on the runner, never per run) unless the caller already set MRSM_DUCKDB_LIB.
  if [ -z "${MRSM_DUCKDB_LIB:-}" ]; then
    MRSM_DUCKDB_LIB="$(scripts/rust/duckdb_lib.sh)"
    export MRSM_DUCKDB_LIB
  fi
  if [ -z "${MRSM_TEST_DUCKDB_PATH+x}" ]; then
    MRSM_TEST_DUCKDB_PATH="$(mktemp -d "${RUNNER_TEMP:-$HOME/.cache}/mrsm-duckdb-tests.XXXXXX")"
    export MRSM_TEST_DUCKDB_PATH
    trap "rm -rf '$MRSM_TEST_DUCKDB_PATH'" EXIT
  fi
  log "cargo test --workspace (DuckDB library: $MRSM_DUCKDB_LIB)"
  cargo test --workspace
}
step_build()  { log "cargo build --release -p meerschaum --bin mrsm"; cargo build --release -p meerschaum --bin mrsm; }
step_bin_debug() { log "cargo build -p meerschaum --bin mrsm"; cargo build -p meerschaum --bin mrsm; }
step_generated() { log "generated tables match their generators"; cargo test -p meerschaum-cli --test generated_tables; }
step_extension() {
  local python scratch
  python="$(command -v "${PYTHON_BIN:-python3}")"
  scratch="$(mktemp -d "${RUNNER_TEMP:-$HOME/.cache}/mrsm-ext-build.XXXXXX")"
  trap "rm -rf '$scratch'" EXIT
  log "Building the native extension (debug) with $python"
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
    scripts/rust/build_py.sh --debug
  if [ -n "${GITHUB_ENV:-}" ]; then
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
  local gen
  for gen in "${gens[@]}"; do
    log "Regenerating with the reference interpreter: $gen"
    "$REPO/scripts/parity/run.sh" "$gen"
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

for s in "${steps[@]}"; do
  case "$s" in
    setup)      step_setup ;;
    disk-guard) step_disk_guard ;;
    fmt)        step_fmt ;;
    clippy)     step_clippy ;;
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
    -h|--help)  sed -n '2,34p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "ci.sh: unknown step '$s' (see --help)" >&2; exit 2 ;;
  esac
done
