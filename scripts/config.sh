# !/bin/bash

### cd into the parent directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PARENT="$(dirname "$DIR")"
. "$DIR"/working_dir.sh

export dockerhub_user="bmeares"
# export tag="latest"
export latest_alias="api"
export tags=( "api" "minimal" )

export base_name="meerschaum"
export image="$dockerhub_user/$base_name"
export python_image="python:3.7-slim-buster"
# export platforms="linux/amd64"
# export platforms="linux/amd64,linux/arm64"
export platforms="linux/amd64,linux/arm64,linux/arm/v7"
export DOCKER_CLI_EXPERIMENTAL=enabled
# export platforms="linux/amd64,linux/arm64,linux/riscv64,linux/ppc64le,linux/386,linux/arm/v7,linux/arm/v6,linux/s390x"
export src="$PARENT"
export port="8000"
export publish_branch="master"
export remote_docs_user="meerschaum"
export remote_docs_host="docs.meerschaum.io"
export remote_docs_dir="~/docs"

# . parse_yaml.sh
# eval $(parse_yaml config.yaml)

