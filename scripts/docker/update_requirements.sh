#! /usr/bin/env bash
# Usage: scripts/docker/update_requirements.sh
#
# Regenerates requirements/<extra>.txt for every extra in the root pyproject.toml (docs/design/
# v5.0.0.md D20, §12.1; `requirements/minimal.txt` is the empty base wheel) and the table of
# extras compiled into the `mrsm` binary (crates/meerschaum-cli/src/extras.rs), writing a file
# only when its contents change. `python scripts/check_packaging.py` fails when they drift.
set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# shellcheck source=scripts/config.sh
. "$DIR"/../config.sh
cd "$PARENT"

"${PYTHON_BIN:-python}" scripts/check_packaging.py --write
