#! /bin/bash

### cd into the parent directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PARENT="$(dirname "$DIR")"
. "$DIR"/working_dir.sh

export dockerhub_user="bmeares"
export tag="latest"
export base_name="meerschaum"
export image="$dockerhub_user/$base_name:$tag"
export src="$PARENT"
export port="8000"

# . parse_yaml.sh
# eval $(parse_yaml config.yaml)

