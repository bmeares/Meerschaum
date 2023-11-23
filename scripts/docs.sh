#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

[ -z "$PYTHON_BIN" ] && PYTHON_BIN="python"

cd "$PARENT"

### Update the root changelog to match the mkdocs changelog.
cat "$PARENT"/docs/mkdocs/news/changelog.md > "$PARENT"/CHANGELOG.md

### Update the root acknowledgements to match the mkdocs acknowledgements.
cat "$PARENT"/docs/mkdocs/news/acknowledgements.md > "$PARENT"/ACKNOWLEDGEMENTS.md

### Build technical documentation.
PDOC_ALLOW_EXEC=1 $PYTHON_BIN -m pdoc -o docs/pdoc -d numpy -n --favicon https://meerschaum.io/assets/logo.ico --logo https://meerschaum.io/assets/logo_48x48.png ./meerschaum/

### Build general documentation.
cd "$PARENT"/docs
$PYTHON_BIN -m mkdocs build
