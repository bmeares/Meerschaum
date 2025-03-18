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

  specific_tags=("$image:$mrsm_version-$tag")
  if [[ "$mrsm_version" != *dev* && "$mrsm_version" != *rc* ]]; then
    specific_tags+=("$image:$tag")
    if [ "$latest_alias" == "$tag" ]; then
      specific_tags+=("$image:latest" "$image:$mrsm_version")
    fi
  fi

  echo "Building tags: ${specific_tags[@]}"

  docker buildx build --progress plain ${push:-} \
    --build-arg dep_group="$tag" \
    --build-arg mrsm_uid="$mrsm_uid" \
    --build-arg mrsm_gid="$mrsm_gid" \
    --platform "$platforms" \
    $(printf -- "-t %s " "${specific_tags[@]}") \
    . || exit 1

done
