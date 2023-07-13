#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
branch=$( git rev-parse --abbrev-ref HEAD )

if [ "$branch" != "$publish_branch" ]; then
  echo "Invalid branch '$branch'. You must be on '$publish_branch' to publish Meerschaum."
  exit 1
fi

if [[ -n $(git status --porcelain) ]]; then
  echo "Branch '$branch' is dirty. Push changes before publishing."
  exit 1
fi

### Publish to Docker Hub.
# docker push "$image"

### Publish to Python Package Index.
cd "$PARENT"
python -m twine upload dist/*
