#!/usr/bin/env bash
# Usage: scripts/rust/check_crate_names.sh [--markdown] [NAME ...]
#
# Reports whether each crate name is free or taken on crates.io (docs/design/v5.0.0.md §15,
# "Crate names on crates.io"). With no NAME, checks the v5 workspace names plus `meerschaum` and
# `mrsm`. crates.io treats `-` and `_` as the same name, and the API lookup is canonical, so a
# hit on either spelling is reported as taken. One request per second (crates.io crawler policy).
#
#   --markdown   print a Markdown table instead of aligned text.
#
# Exit status: 0 if the check ran (whatever the results), 1 on a network/API error.
set -euo pipefail

markdown=""
if [ "${1:-}" = "--markdown" ]; then markdown=1; shift; fi

names=("$@")
if [ "${#names[@]}" -eq 0 ]; then
  names=(
    meerschaum meerschaum-core meerschaum-kernel meerschaum-connector meerschaum-connector-sql
    meerschaum-connector-valkey meerschaum-connector-api meerschaum-worker meerschaum-engine meerschaum-jobs meerschaum-compose meerschaum-server meerschaum-web
    meerschaum-plugin meerschaum-cli mrsm
  )
fi

UA="meerschaum-v5-crate-name-check (https://github.com/bmeares/Meerschaum)"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

if [ -n "$markdown" ]; then
  echo "| Crate | Status | Details |"
  echo "|---|---|---|"
fi

errors=0
first=1
for name in "${names[@]}"; do
  [ -z "$first" ] && sleep 1
  first=""
  code="$(curl -sS -o "$tmp" -w '%{http_code}' -A "$UA" "https://crates.io/api/v1/crates/$name" || echo 000)"
  case "$code" in
    200)
      status="taken"
      details="$(python3 -I -c '
import json, sys
c = json.load(open(sys.argv[1]))["crate"]
created = (c.get("created_at") or "")[:10]
repo = c.get("repository") or "none"
print("as %s v%s, created %s, %s downloads; repo: %s" % (
    c["name"], c.get("max_version"), created, c.get("downloads", 0), repo))
' "$tmp")"
      ;;
    404) status="free"; details="" ;;
    *)   status="error"; details="HTTP $code"; errors=$((errors + 1)) ;;
  esac
  if [ -n "$markdown" ]; then
    echo "| \`$name\` | $status | $details |"
  else
    printf '%-20s %-6s %s\n' "$name" "$status" "$details"
  fi
done

[ "$errors" -eq 0 ]
