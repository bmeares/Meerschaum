#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"

### Update the root changelog to match the mkdocs changelog.
cat "$PARENT"/docs/mkdocs/news/changelog.md > "$PARENT"/CHANGELOG.md

### Update the root acknowledgements to match the mkdocs acknowledgements.
cat "$PARENT"/docs/mkdocs/news/acknowledgements.md > "$PARENT"/ACKNOWLEDGEMENTS.md

### Build technical documentation.
python -m pdoc --html --output-dir docs/pdoc meerschaum -f

### Build general documentation.
cd "$PARENT"/docs
python -m mkdocs build
