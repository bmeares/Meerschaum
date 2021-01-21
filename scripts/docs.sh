#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

### Build technical documentation
cd "$PARENT"
python -m pdoc --html --output-dir docs/pdoc meerschaum --force

### Build general documentation
cd "$PARENT"/docs
python -m mkdocs build
