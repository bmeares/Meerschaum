#! /usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh
cd "$PARENT"

### Install python packages.

reqs_file="/tmp/mrsm_dev_setup_reqs.txt"
python -m meerschaum install package wheel --venv None --debug
python -m meerschaum show packages dev-tools --nopretty > "$reqs_file"
python -m meerschaum show packages docs --nopretty >> "$reqs_file"
python -m pip install --upgrade -r "$reqs_file" || exit 1
### Install pdoc3 and docker-compose outside of the declared docs dependencies.
python -m pip install pdoc3 || exit 1
python -m pip install docker-compose || exit 1
rm -f "$reqs_file"

### Install Meerschaum plugins.
python -m meerschaum install plugin thanks

### Enable docker buildx.

if [ "$MRSM_SKIP_DOCKER_EXPERIMENTAL" != "1" ]; then
  is_experimental=$( grep "experimental" /etc/docker/daemon.json | grep "true" )
  daemon_json="{
    \"experimental\": true
  }"
  if [ -z "$is_experimental" ]; then
    echo "Experimental mode is not enabled. Would you like to overwrite /etc/docker/daemon.json?"
    select yn in "Y" "N"; do
      case "$yn" in
        Y ) echo "$daemon_json" | sudo tee /etc/docker/daemon.json && sudo systemctl restart docker
          break ;;
        N ) echo "Please enable experimental mode." && exit 1 ;;
      esac
    done
  fi
fi
docker buildx ls ; rc="$?"
if [[ "$rc" != "0" ]]; then
  echo "Installing docker buildx..."
  export DOCKER_BUILDKIT=1
  docker build --platform=local -o /tmp git://github.com/docker/buildx
  mkdir -p ~/.docker/cli-plugins
  mv /tmp/buildx ~/.docker/cli-plugins/docker-buildx
fi

### Create and use builder.

builder_name="multiarch_builder"
[ -z "$(docker buildx ls | grep "$builder_name")" ] && docker buildx create --name "$builder_name" --use
echo "Reset qemu architectures..."
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes


