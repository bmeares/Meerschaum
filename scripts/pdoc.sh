#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

[ -z "$PYTHON_BIN" ] && PYTHON_BIN='python'
[ -z "$PDOC_ALLOW_EXEC" ] && export PDOC_ALLOW_EXEC=1

### Build technical documentation
cd "$PARENT"
$PYTHON_BIN -m pdoc -d numpy -n --favicon https://meerschaum.io/assets/logo.ico --logo https://meerschaum.io/assets/logo_48x48.png ./meerschaum/
