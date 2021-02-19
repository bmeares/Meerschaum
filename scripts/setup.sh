#! /usr/bin/env bash

### Set up the development environment.

is_experimental=$( cat /etc/docker/daemon.json | grep "experimental" | grep "true" )
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
docker buildx ls ; rc="$?"
if [[ "$rc" != "0" ]]; then
  echo "Installing docker buildx..."
  export DOCKER_BUILDKIT=1
  docker build --platform=local -o /tmp git://github.com/docker/buildx
  mkdir -p ~/.docker/cli-plugins
  mv /tmp/buildx ~/.docker/cli-plugins/docker-buildx
fi
builder_name="multiarch_builder"
[ -z "$(docker buildx ls | grep "$builder_name")" ] && docker buildx create --name "$builder_name" --use
echo "Reset qemu architectures..."
docker run --rm --privileged multiarch/qemu-user-static --reset -p yes


