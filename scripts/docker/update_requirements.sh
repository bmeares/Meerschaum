#! /usr/bin/env bash
# Usage: scripts/docker/update_requirements.sh
#
# Regenerates requirements/<dep_group>.txt for every dep group in $tags (scripts/config.sh) from
# `python -m meerschaum show packages <group>`, writing a file only when its contents change.
set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# shellcheck source=scripts/config.sh
. "$DIR"/../config.sh
cd "$PARENT"

mkdir -p requirements
for t in "${tags[@]}"; do
  new_reqs=$(python -m meerschaum show packages "$t" --nopretty)
  old_reqs=$(cat requirements/"$t".txt 2>/dev/null || true)
  if [ -z "$old_reqs" ] || [ "$new_reqs" != "$old_reqs" ]; then
    echo "$new_reqs" > requirements/"$t".txt
  fi
done
