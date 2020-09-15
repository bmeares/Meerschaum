#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

### publish to Docker Hub
docker push "$image"

### publish to PyPy
cd "$DIR"/../src
twine upload dist/*
