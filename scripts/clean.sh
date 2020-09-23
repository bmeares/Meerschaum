#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$DIR"/../
python setup.py clean --all
rm -rf build dist meerschaum.egg-info
### delete configuration from development environment
python -m meerschaum delete config --force
