#!/usr/bin/env bash
# Usage: scripts/parity/check_fixture_hygiene.sh [PATH ...]
#
# Fails if any committed fixture under crates/*/tests/fixtures (or the given paths) contains
# machine-specific or sensitive content: process environments captured from v4 job metadata,
# Claude Code session variables, home directories, hostnames, private repository names, or
# anything that looks like a credential. Run by the Rust CI parity job and before every commit
# that touches fixtures. Fixture generators must scrub these at the source (see crates/README.md).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

paths=("$@")
if [ ${#paths[@]} -eq 0 ]; then
  mapfile -t paths < <(ls -d crates/*/tests/fixtures 2>/dev/null || true)
fi
[ ${#paths[@]} -eq 0 ] && { echo "check_fixture_hygiene: no fixture directories found"; exit 0; }

# Two pattern sets. Python helpers under fixtures/ (generators and their support modules) are
# excluded because they legitimately name the things they scrub; only recorded data is scanned. Keep the private-name rule in sync with crates/README.md.
ci_patterns=(            # case-insensitive
  'CLAUDE_CODE'
  'CODEX_COMPANION'
  'MESSAGING_TOKEN'
  'XDG_SESSION'
  'DBUS_SESSION'
  'swamp ?rabbit'
  'trail-counter'
  'bikewalk'
  'greenville'
  'BEGIN (RSA|OPENSSH|EC|PGP) PRIVATE KEY'
)
cs_patterns=(            # case-sensitive (paths, handles, token shapes)
  '/home/bmeares'
  'bmeares@'
  '/Users/[a-z][a-z0-9_-]*/'
  'AKIA[0-9A-Z]{16}'
  'ghp_[A-Za-z0-9]{20,}'
  'sk-[A-Za-z0-9_-]{20,}'
  'eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.'
)

status=0
scan() {
  local flags=$1 pattern=$2 hits
  if hits=$(grep -rIl $flags -E --exclude='*.py' --exclude='*.md' -- "$pattern" "${paths[@]}" 2>/dev/null) && [ -n "$hits" ]; then
    echo "check_fixture_hygiene: pattern '$pattern' found in:"
    echo "$hits" | sed 's/^/  /'
    status=1
  fi
}
for pattern in "${ci_patterns[@]}"; do scan -i "$pattern"; done
for pattern in "${cs_patterns[@]}"; do scan "" "$pattern"; done

if [ $status -ne 0 ]; then
  echo "check_fixture_hygiene: FAILED. Scrub the generator, regenerate, and never commit these."
  exit 1
fi
echo "check_fixture_hygiene: clean (${#paths[@]} fixture directories)"
