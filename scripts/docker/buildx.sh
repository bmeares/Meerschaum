#! /usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/../config.sh
cd "$PARENT"

[ "$1" == "--push" ] && push="--push" || push="--load"
[ -z "$PYTHON_BIN" ] && PYTHON_BIN="python"

### Experimental features must be enabled and docker buildx must be installed.
### Run setup.sh to ensure everything is set up.

mrsm_version=$($PYTHON_BIN -m meerschaum -V | sed 's/Meerschaum v//g')
mrsm_uid=$(id -u $USER)
mrsm_gid=$(id -g $USER)

./scripts/docker/update_requirements.sh

docker pull "$python_image"
for tag in "${tags[@]}"; do
  [ "$latest_alias" == "$tag" ] && tag_latest="-t $image:latest -t $image:$mrsm_version" || unset tag_latest

  docker buildx build ${push:-} \
    --build-arg dep_group="$tag" \
    --build-arg mrsm_uid="$mrsm_uid" \
    --build-arg mrsm_gid="$mrsm_gid" \
    --platform "$platforms" \
    -t "$image:$tag" -t "$image:$mrsm_version-$tag" ${tag_latest:-} \
    . || exit 1

done
