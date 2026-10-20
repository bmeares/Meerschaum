#!/usr/bin/env bash
# Usage: scripts/rust/ci_tools.sh [sccache] [nextest] [mold]      (no argument: sccache and nextest)
#
# Installs the CI build accelerators of the SELF-HOSTED Linux runner into
# `${MRSM_CI_CACHE:-$HOME/.cache/mrsm-ci}/bin` (user level: no sudo, outside the workspace, so `git
# clean` never removes them). Every download is pinned to a version and checked against the SHA-256
# recorded below; a tool is installed once per version. `scripts/rust/ci.sh` calls this on a
# self-hosted runner and uses what is there; nothing here is needed on a GitHub-hosted runner (those
# use Swatinem/rust-cache, and nextest and sccache stay off) or on a developer machine.
#
#   sccache   mozilla/sccache: a rustc wrapper with a local on-disk cache shared by every runner of this
#             host (`SCCACHE_DIR`, capped by `SCCACHE_CACHE_SIZE`; ci.sh sets both).
#   nextest   nextest-rs/nextest: `cargo nextest run`, a faster test runner (doc tests still run with
#             `cargo test --doc`).
#   mold      rui314/mold: a faster linker (`-C link-arg=-fuse-ld=mold`); opt-in, `MRSM_CI_MOLD=1` makes ci.sh
#             install and use it (rustc's default lld is within a second per link: see ci.sh).
#
# Bumping a version: change VERSION and both SHA-256 values (x86_64 and aarch64) together, from the
# release's own checksum files or `sha256sum` of the downloaded archives. Latest stable releases only.
set -euo pipefail

SCCACHE_VERSION="0.18.0"
SCCACHE_SHA_X86_64="45f1447fbe231e3037bde351ef70677dd212216c8d62ae7ca409fecc4d6acc89"
SCCACHE_SHA_AARCH64="2b3284d5da3b46a47dc4229e75bb7b88ac4aa99c8d754fb7d2f84997e5a4354a"

NEXTEST_VERSION="0.9.148"
NEXTEST_SHA_X86_64="223ba936714cc861a9cdedad5a8eb664dfdd69d6fbdf6cea6aa8184f523290dc"
NEXTEST_SHA_AARCH64="81469eb963ef883fd590e60189e77d6aba3d91b418591d3391f50fe3e9480d2b"

MOLD_VERSION="3.0.0"
MOLD_SHA_X86_64="6c90d4a474c7c0409dfb575be03a5345878ac14fdba18de8b40fa58c60121189"
MOLD_SHA_AARCH64="52c759d3689babaea4c42af2f31062a74ab83e17ffb5a8090232534c67b70577"

CACHE="${MRSM_CI_CACHE:-$HOME/.cache/mrsm-ci}"
BIN="$CACHE/bin"

if [ "$(uname -s)" != Linux ]; then
  echo "ci_tools.sh: Linux only (the self-hosted runner); nothing to install on $(uname -s)." >&2
  exit 0
fi
case "$(uname -m)" in
  x86_64) arch=x86_64 ;;
  aarch64|arm64) arch=aarch64 ;;
  *) echo "ci_tools.sh: unsupported architecture $(uname -m)." >&2; exit 0 ;;
esac
if (ldd --version 2>&1 || true) | grep -qi musl; then
  echo "ci_tools.sh: musl hosts are not supported (the pinned archives are glibc/static builds)." >&2
  exit 0
fi

mkdir -p "$BIN"
SCRATCH=()
cleanup() { [ "${#SCRATCH[@]}" -eq 0 ] || rm -rf "${SCRATCH[@]}"; }
trap cleanup EXIT

# fetch <url> <sha256> <destination file>
fetch() {
  local url="$1" want="$2" dest="$3" got
  echo "ci_tools.sh: downloading $url" >&2
  curl -fsSL --retry 3 -o "$dest" "$url"
  got="$(sha256sum "$dest" | cut -d' ' -f1)"
  if [ "$got" != "$want" ]; then
    echo "ci_tools.sh: checksum mismatch for $url (expected $want, got $got)" >&2
    return 1
  fi
}

sha_for() { local v="$1_SHA_$(printf '%s' "$arch" | tr a-z A-Z)"; printf '%s' "${!v}"; }

install_sccache() {
  local stamp="$BIN/.sccache-$SCCACHE_VERSION"
  [ -f "$stamp" ] && [ -x "$BIN/sccache" ] && return 0
  local triple="$arch-unknown-linux-musl" tmp
  tmp="$(mktemp -d "$CACHE/.tmp.XXXXXX")"
  SCRATCH+=("$tmp")
  fetch "https://github.com/mozilla/sccache/releases/download/v$SCCACHE_VERSION/sccache-v$SCCACHE_VERSION-$triple.tar.gz" \
    "$(sha_for SCCACHE)" "$tmp/a.tar.gz"
  tar -xzf "$tmp/a.tar.gz" -C "$tmp"
  install -m 0755 "$tmp/sccache-v$SCCACHE_VERSION-$triple/sccache" "$BIN/sccache.new"
  mv -f "$BIN/sccache.new" "$BIN/sccache"
  rm -f "$BIN"/.sccache-*
  : > "$stamp"
}

install_nextest() {
  local stamp="$BIN/.nextest-$NEXTEST_VERSION"
  [ -f "$stamp" ] && [ -x "$BIN/cargo-nextest" ] && return 0
  local triple="$arch-unknown-linux-gnu" tmp
  tmp="$(mktemp -d "$CACHE/.tmp.XXXXXX")"
  SCRATCH+=("$tmp")
  fetch "https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-$NEXTEST_VERSION/cargo-nextest-$NEXTEST_VERSION-$triple.tar.gz" \
    "$(sha_for NEXTEST)" "$tmp/a.tar.gz"
  tar -xzf "$tmp/a.tar.gz" -C "$tmp"
  install -m 0755 "$tmp/cargo-nextest" "$BIN/cargo-nextest.new"
  mv -f "$BIN/cargo-nextest.new" "$BIN/cargo-nextest"
  rm -f "$BIN"/.nextest-*
  : > "$stamp"
}

install_mold() {
  local stamp="$BIN/.mold-$MOLD_VERSION"
  [ -f "$stamp" ] && [ -x "$BIN/mold" ] && return 0
  local dir="mold-$MOLD_VERSION-$arch-linux" tmp
  tmp="$(mktemp -d "$CACHE/.tmp.XXXXXX")"
  SCRATCH+=("$tmp")
  fetch "https://github.com/rui314/mold/releases/download/v$MOLD_VERSION/$dir.tar.gz" \
    "$(sha_for MOLD)" "$tmp/a.tar.gz"
  tar -xzf "$tmp/a.tar.gz" -C "$tmp"
  rm -rf "$CACHE/mold" "$CACHE/mold.new"
  mv "$tmp/$dir" "$CACHE/mold.new"
  mv "$CACHE/mold.new" "$CACHE/mold"
  # `gcc -fuse-ld=mold` runs `ld.mold` found on PATH; mold locates its own helper files from the
  # path of the executable, so link to the extracted tree rather than copy the binary.
  ln -sfn "$CACHE/mold/bin/mold" "$BIN/mold"
  ln -sfn "$CACHE/mold/bin/mold" "$BIN/ld.mold"
  rm -f "$BIN"/.mold-*
  : > "$stamp"
}

tools=("$@")
[ "${#tools[@]}" -eq 0 ] && tools=(sccache nextest)
for tool in "${tools[@]}"; do
  case "$tool" in
    sccache) install_sccache ;;
    nextest) install_nextest ;;
    mold) install_mold ;;
    *) echo "ci_tools.sh: unknown tool '$tool' (sccache, nextest, mold)" >&2; exit 2 ;;
  esac
done
echo "ci_tools.sh: $(ls "$BIN" | tr '\n' ' ')(in $BIN)" >&2
