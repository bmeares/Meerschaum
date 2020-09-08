#! /bin/sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

docker tag "$image" "$dockerhub_user"/"$image"
docker push "$dockerhub_user"/"$image"

