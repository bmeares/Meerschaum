#!/usr/bin/env bash
# Run the console suite's Chromium in its own network namespace (see `browser_isolation()` in
# conftest.py). Chromium fails every request still waiting for a connection with
# `ERR_NETWORK_CHANGED` whenever the host's interfaces change, and on a CI runner another job's
# Docker network coming up or down is such a change: the Dash callbacks queued behind the six
# connections per host were dropped, and their outputs never rendered. In a namespace of its own
# Chromium sees only its loopback and pasta's interface; pasta forwards the namespace's loopback
# ports `$MRSM_TEST_CHROMIUM_PORTS` to the host's, where the test servers listen.
#
#   MRSM_TEST_CHROMIUM=<browser executable> MRSM_TEST_CHROMIUM_PORTS=18400-19999 \
#     chromium_netns.sh <browser arguments>
#
# Playwright talks to the browser over inherited pipes (fds 3 and 4), which `nsenter` keeps.
# The namespace's holder dies with this script (`--pdeathsig`), and pasta exits with the holder.
set -u
real="${MRSM_TEST_CHROMIUM:?Set MRSM_TEST_CHROMIUM to the browser executable.}"
ports="${MRSM_TEST_CHROMIUM_PORTS:?Set MRSM_TEST_CHROMIUM_PORTS to the forwarded port range.}"
unset TMUX TMUX_PANE

unshare --user --map-current-user --net setpriv --pdeathsig KILL sleep infinity &
holder=$!
own_net="$(readlink /proc/self/ns/net)"
for _ in $(seq 1 250); do
  # Ready once `unshare` has executed `sleep` inside the new namespace.
  if [ "$(readlink "/proc/$holder/ns/net" 2>/dev/null)" != "$own_net" ] \
    && [ "$(readlink "/proc/$holder/exe" 2>/dev/null)" = "$(readlink -f "$(command -v sleep)")" ]; then
    break
  fi
  sleep 0.02
done
if ! pasta --quiet --config-net --no-map-gw --dns none -t none -u none -U none -T "$ports" "$holder"; then
  kill "$holder" 2>/dev/null
  echo 'chromium_netns.sh: pasta could not set up the namespace.' >&2
  exit 70
fi
nsenter --preserve-credentials --user --net --target "$holder" -- "$real" "$@"
status=$?
kill "$holder" 2>/dev/null
exit "$status"
