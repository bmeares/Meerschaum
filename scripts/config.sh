#! /bin/bash

### cd into the parent directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PARENT="$(dirname "$DIR")"
. "$DIR"/working_dir.sh

export image="meerschaum:latest"
export src="$PARENT/src"


# . parse_yaml.sh
# eval $(parse_yaml config.yaml)

