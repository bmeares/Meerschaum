#! /bin/bash

export image="meerschaum:latest"
export src="$(dirname $(readlink -f $0))/src"

. parse_yaml.sh
eval $(parse_yaml config.yaml)

