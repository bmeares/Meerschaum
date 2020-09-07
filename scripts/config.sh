#! /bin/bash

### cd into the parent directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/working_dir.sh

export image="meerschaum:latest"
export src="$(dirname $(readlink -f $0))/src"


# . parse_yaml.sh
# eval $(parse_yaml config.yaml)

