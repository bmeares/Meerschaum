#!/usr/bin/env bash
# Usage: scripts/parity/check_fixture_hygiene.sh [PATH ...]
#        scripts/parity/check_fixture_hygiene.sh --public [PATH ...]
#
# Fails if any committed fixture under crates/*/tests/fixtures (or the given paths) contains
# machine-specific or sensitive content: process environments captured from v4 job metadata,
# Claude Code session variables, home directories, hostnames, private repository names, or
# anything that looks like a credential. Run by the Rust CI parity and guardrails jobs and before every commit
# that touches fixtures. Fixture generators must scrub these at the source (docs/design/v5-build.md
# "Fixture hygiene").
#
# --public scans docs and code (Markdown and Python included; default: every tracked file) for the
# private names, home directories, e-mail handle and credential shapes. It skips the captured-
# environment variable names, which docs and code legitimately mention (`claude_code_command`,
# the variables a fixture generator scrubs); only recorded data must never contain them.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

public=0
if [ "${1:-}" = --public ]; then
  public=1
  shift
fi
paths=("$@")
if [ ${#paths[@]} -eq 0 ] && [ $public -eq 1 ]; then
  # Tracked files that still exist: grep exits 2, hiding its matches, when one path is missing.
  mapfile -t paths < <(git ls-files | while IFS= read -r path; do [ -e "$path" ] && printf '%s\n' "$path"; done)
elif [ ${#paths[@]} -eq 0 ]; then
  mapfile -t paths < <(ls -d crates/*/tests/fixtures 2>/dev/null || true)
fi
[ ${#paths[@]} -eq 0 ] && { echo "check_fixture_hygiene: no fixture directories found"; exit 0; }

# Three pattern sets. In the default mode, Python helpers under fixtures/ (generators and their
# support modules) and Markdown are excluded because they legitimately name the things they scrub;
# only recorded data is scanned. Keep the private-name rule in sync with CODING_STANDARDS.md.
env_patterns=(           # case-insensitive; captured process environments (default mode only)
  'CLAUDE_CODE'
  'CODEX_COMPANION'
  'MESSAGING_TOKEN'
  'XDG_SESSION'
  'DBUS_SESSION'
)
ci_patterns=(            # case-insensitive
  'swamp ?rabbit'
  'trail-counter'
  'bikewalk'
  'greenville'
  'BEGIN (RSA|OPENSSH|EC|PGP) PRIVATE KEY'
)
# JWT shapes are deliberately not scanned: auth fixtures carry deterministic test vectors signed
# with a fixture-owned secret, and a shape match cannot tell those from a real token. Generators
# must replace credentials minted during a run with placeholders (see gen_contract.py).
cs_patterns=(            # case-sensitive (paths, handles, token shapes)
  '/home/bmeares'
  'bmeares@'
  '/Users/[a-z][a-z0-9_-]*/'
  'AKIA[0-9A-Z]{16}'
  'ghp_[A-Za-z0-9]{20,}'
  'sk-[A-Za-z0-9_-]{20,}'
)

# Allowed by maintainer decision, scoped to one pattern and one file each (never a whole file or
# pattern): the maintainer's original asciinema recordings under docs/zensical/assets/casts, kept
# verbatim (jobs.cast names the private city, demo.cast a home path), and the public docs example
# "Greenville weather" in docs/zensical/reference/pipes/tags.md (which the generated
# docs/zensical/llms-full.txt copies) and in the OpenAPI example of meerschaum/api/models/_pipes.py. In --public mode the scanner skips its own pattern table.
allowed_for() {
  case "$1" in
    greenville) printf '%s\n' --exclude=jobs.cast --exclude=llms-full.txt --exclude=tags.md --exclude=_pipes.py ;;
    /home/bmeares) printf '%s\n' --exclude=demo.cast ;;
  esac
}
if [ $public -eq 1 ]; then
  skipped=(--exclude=check_fixture_hygiene.sh)
else
  skipped=(--exclude='*.py' --exclude='*.md')
fi

status=0
scan() {
  local flags=$1 pattern=$2 hits
  local allowed=()
  mapfile -t allowed < <(allowed_for "$pattern")
  if hits=$(grep -rIl $flags -E "${skipped[@]}" ${allowed[@]+"${allowed[@]}"} -- "$pattern" "${paths[@]}" 2>/dev/null) && [ -n "$hits" ]; then
    echo "check_fixture_hygiene: pattern '$pattern' found in:"
    echo "$hits" | sed 's/^/  /'
    # For path patterns, show where they occur (line number and up to 80 characters BEFORE the
    # match, never the match or anything after it) so CI logs can locate the leaking field.
    case "$pattern" in
      /*)
        grep -rIn $flags -o -E "${skipped[@]}" ${allowed[@]+"${allowed[@]}"} -- ".{0,80}$pattern" "${paths[@]}" 2>/dev/null \
          | sed -E "s#$pattern\$#<REDACTED>#" | head -n 20 | sed 's/^/    /' || true
        ;;
    esac
    status=1
  fi
}
if [ $public -eq 0 ]; then
  for pattern in "${env_patterns[@]}"; do scan -i "$pattern"; done
fi
for pattern in "${ci_patterns[@]}"; do scan -i "$pattern"; done
for pattern in "${cs_patterns[@]}"; do scan "" "$pattern"; done

if [ $status -ne 0 ]; then
  if [ $public -eq 1 ]; then
    echo "check_fixture_hygiene: FAILED. Remove these from the public files."
  else
    echo "check_fixture_hygiene: FAILED. Scrub the generator, regenerate, and never commit these."
  fi
  exit 1
fi
if [ $public -eq 1 ]; then
  echo "check_fixture_hygiene: clean (${#paths[@]} paths)"
else
  echo "check_fixture_hygiene: clean (${#paths[@]} fixture directories)"
fi
