#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

### experimental features must be enabled
docker build --squash -t "$image" . || exit 1

### build the pip package
cd "$DIR"/../src
python setup.py sdist

