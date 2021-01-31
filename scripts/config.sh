#! /bin/bash

### cd into the parent directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PARENT="$(dirname "$DIR")"
. "$DIR"/working_dir.sh

export dockerhub_user="bmeares"
export tag="latest"
export base_name="meerschaum"
export image="$dockerhub_user/$base_name:$tag"
export python_image="python:3.8-slim-buster"
export src="$PARENT"
export port="8000"
export publish_branch="master"
export remote_docs_user="meerschaum"
export remote_docs_host="docs.meerschaum.io"
export remote_docs_dir="~/docs"

# . parse_yaml.sh
# eval $(parse_yaml config.yaml)

