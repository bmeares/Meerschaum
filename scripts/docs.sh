#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
### Install docs dependencies.
# python -m meerschaum upgrade packages docs -y

### Jump into the Meerschaum virtual environment.
# source ~/.config/meerschaum/venvs/mrsm/bin/activate

### Build technical documentation.
python -m pdoc --html --output-dir docs/pdoc meerschaum --force

### Build general documentation.
cd "$PARENT"/docs
python -m mkdocs build
