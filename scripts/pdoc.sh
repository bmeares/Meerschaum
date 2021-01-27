#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

### Build technical documentation
cd "$PARENT"
python -m pdoc --http : meerschaum

