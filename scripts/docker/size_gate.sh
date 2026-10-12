#!/usr/bin/env bash
# Usage: scripts/docker/size_gate.sh [--warn-only] [--slim-max MB] [--full-max MB]
#                                    [--slim-image IMAGE] [--full-image IMAGE]
#
# Docker image size gate (docs/design/v5.0.0.md §12.1): fails if the `slim` image exceeds
# --slim-max (default 200) MB or the `full` image exceeds --full-max (default 900) MB.
# Sizes are the uncompressed image size from `docker image inspect` in decimal MB (10^6 bytes),
# the same unit `docker images` prints. The images must already be loaded locally.
#
#   --warn-only    report and emit warnings, but always exit 0 (CI starts here because `full`
#                  is ~1.9 GB until the v5 dependency cuts land).
#   --slim-image   default bmeares/meerschaum:slim
#   --full-image   default bmeares/meerschaum:full
#   An image given as "" (e.g. --full-image "") is skipped.
#
# Writes a Markdown table to $GITHUB_STEP_SUMMARY when it is set.
set -euo pipefail

warn_only=""
slim_max=200
full_max=900
slim_image="bmeares/meerschaum:slim"
full_image="bmeares/meerschaum:full"

while [ $# -gt 0 ]; do
  case "$1" in
    --warn-only)  warn_only=1; shift ;;
    --slim-max)   slim_max="$2"; shift 2 ;;
    --full-max)   full_max="$2"; shift 2 ;;
    --slim-image) slim_image="$2"; shift 2 ;;
    --full-image) full_image="$2"; shift 2 ;;
    -h|--help)    sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "size_gate.sh: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

failures=0
rows=()

check() {
  local label="$1" image="$2" max="$3" bytes mb status
  [ -z "$image" ] && return 0
  if ! bytes="$(docker image inspect --format '{{.Size}}' "$image" 2>/dev/null)"; then
    echo "size_gate: $label image '$image' not found locally" >&2
    rows+=("| $label | \`$image\` | missing | $max MB | FAIL |")
    failures=$((failures + 1))
    return 0
  fi
  mb="$(awk -v b="$bytes" 'BEGIN { printf "%.1f", b / 1000000 }')"
  if awk -v m="$mb" -v x="$max" 'BEGIN { exit !(m > x) }'; then
    status="FAIL"
    failures=$((failures + 1))
    if [ -n "${GITHUB_ACTIONS:-}" ]; then
      echo "::warning title=Docker size gate::$label image $image is $mb MB (limit $max MB)"
    fi
  else
    status="ok"
  fi
  printf '%-5s %-45s %9s MB  (limit %s MB)  %s\n' "$label" "$image" "$mb" "$max" "$status"
  rows+=("| $label | \`$image\` | $mb MB | $max MB | $status |")
}

check slim "$slim_image" "$slim_max"
check full "$full_image" "$full_max"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "### Docker size gate${warn_only:+ (warn-only)}"
    echo
    echo "| Target | Image | Size | Limit | Result |"
    echo "|---|---|---|---|---|"
    printf '%s\n' "${rows[@]}"
  } >> "$GITHUB_STEP_SUMMARY"
fi

if [ "$failures" -gt 0 ]; then
  if [ -n "$warn_only" ]; then
    echo "size_gate: $failures image(s) over the limit (warn-only: not failing)."
    exit 0
  fi
  echo "size_gate: $failures image(s) over the limit." >&2
  exit 1
fi
echo "size_gate: all images within limits."
