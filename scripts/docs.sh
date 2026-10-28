#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
. "$DIR/config.sh"
PYTHON_BIN="${PYTHON_BIN:-python}"
site_only=0
case "${1:-}" in
  --site-only) site_only=1 ;;
  '') ;;
  *) echo 'Usage: scripts/docs.sh [--site-only]' >&2; exit 2 ;;
esac
cd "$PARENT"

# Site tooling is pinned independently of runtime extras and the caller's interpreter.
if [ -z "${DOCS_PYTHON_BIN:-}" ]; then
  docs_key="$("$PYTHON_BIN" - "$PARENT/docs/requirements.txt" <<'PY'
import hashlib, pathlib, sys
print(f'{sys.version_info.major}.{sys.version_info.minor}-' + hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest()[:16])
PY
)"
  docs_tools="${XDG_CACHE_HOME:-$HOME/.cache}/mrsm-docs/$docs_key"
  if [ ! -x "$docs_tools/bin/python" ] || [ ! -f "$docs_tools/.ready" ]; then
    "$PYTHON_BIN" -m venv "$docs_tools"
    "$docs_tools/bin/python" -m pip install --quiet -r "$PARENT/docs/requirements.txt"
    touch "$docs_tools/.ready"
  fi
  DOCS_PYTHON_BIN="$docs_tools/bin/python"
fi

if [ "$site_only" -eq 0 ]; then
  cat docs/zensical/news/changelog.md > CHANGELOG.md
  cat docs/zensical/news/acknowledgements.md > ACKNOWLEDGEMENTS.md
  # Documentation imports never read the maintainer's config or installed plugins.
  docs_scratch="$(mktemp -d "${TMPDIR:-/tmp}/mrsm-docs.XXXXXX")"
  trap 'rm -rf "$docs_scratch"' EXIT
  mkdir -p "$docs_scratch/root" "$docs_scratch/plugins"
  while IFS= read -r docs_mrsm_key; do unset "$docs_mrsm_key"; done < <(compgen -v MRSM_ || true)
  export MRSM_ROOT_DIR="$docs_scratch/root" MRSM_PLUGINS_DIR="$docs_scratch/plugins"
  export MRSM_NO_AUTO_INSTALL=1 MRSM_NO_UPDATE_CHECK=1
  unset TMUX TMUX_PANE
  "$PYTHON_BIN" scripts/generate_mcp_docs.py
  PDOC_ALLOW_EXEC=1 "$PYTHON_BIN" -m pdoc -o docs/pdoc -d numpy -n \
    --favicon https://meerschaum.io/assets/logo.ico --logo https://meerschaum.io/assets/logo_48x48.png ./meerschaum/
  "$PYTHON_BIN" scripts/gen_llms_txt.py
fi

cd "$PARENT/docs"
"$DOCS_PYTHON_BIN" -m zensical build --strict
