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
#   test         cargo test --workspace
#   build        cargo build --release -p meerschaum-cli
#   check        cargo check --workspace            (the hosted ubuntu/windows/macos legs)
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
step_test()   { log "cargo test --workspace"; cargo test --workspace; }
step_build()  { log "cargo build --release -p meerschaum-cli"; cargo build --release -p meerschaum-cli; }
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
    build)      step_build ;;
    check)      step_check ;;
    parity)     step_parity ;;
    -h|--help)  sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "ci.sh: unknown step '$s' (see --help)" >&2; exit 2 ;;
  esac
done
