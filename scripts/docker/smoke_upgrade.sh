#!/usr/bin/env bash
# Replace a released full image on its data volume and reopen it after another restart.
set -euo pipefail
candidate=${1:?candidate full image digest}
previous=${2:?previous full image}
volume=$(docker volume create)
trap 'docker volume rm "$volume" >/dev/null' EXIT
for stage in seed verify reopen; do
  image="$candidate"
  if [[ "$stage" = seed ]]; then image="$previous"; fi
  docker run --rm --network none --mount "type=volume,src=$volume,dst=/meerschaum" \
    -v "$PWD/scripts/ci/upgrade_state.py:/upgrade_state.py:ro" \
    -e MRSM_ROOT_DIR=/meerschaum -e MRSM_PLUGINS_DIR=/meerschaum/plugins \
    -e MRSM_SQL_UPGRADE=sqlite:////meerschaum/state.db -e MRSM_NO_AUTO_INSTALL=1 \
    -e 'MRSM_CONFIG={"meerschaum":{"instance":"sql:upgrade","executor":"local"}}' \
    --entrypoint bash "$image" -ec '
      mkdir -p "$MRSM_PLUGINS_DIR"
      if [[ "$1" = seed ]]; then
        mkdir -p "$MRSM_ROOT_DIR/config"
        printf "%s\n" "upgrade_probe: MRSM{meerschaum:instance}" >> "$MRSM_ROOT_DIR/config/stack.yaml"
      fi
      python /upgrade_state.py "$1"
      mrsm show jobs --nopretty | grep -F retained-command
      grep -Fq "MRSM{meerschaum:instance}" "$MRSM_ROOT_DIR/config/stack.yaml"
      touch "$MRSM_ROOT_DIR/executors/.upgrade-write" && rm "$MRSM_ROOT_DIR/executors/.upgrade-write"
    ' bash "$stage"
done
