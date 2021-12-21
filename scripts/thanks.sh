#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"

python -m meerschaum thanks > "$PARENT"/docs/mkdocs/news/acknowledgements.md
