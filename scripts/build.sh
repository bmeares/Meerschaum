#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

### delete leftover files before building
scripts/clean.sh

### build documentation
scripts/docs.sh

### experimental features must be enabled
docker pull "$python_image"
docker build --squash -t "$image" . || exit 1

### build the pip package
cd "$PARENT"
python setup.py sdist

