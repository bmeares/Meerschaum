#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh
cd "$PARENT"

[ "$1" == "--push" ] && push="--push"

### Delete leftover files before building.
./scripts/clean.sh

### Build documentation.
./scripts/docs.sh

### Update the requirements text files.
./scripts/docker/update_requirements.sh

### Build the pip package for uploading to PyPI.
python -m build

