#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

[ -z "$PYTHON_BIN" ] && export PYTHON_BIN=python

if [ "$1" == "clean" ] || [ ! -d "$PARENT/.venv" ]; then
  rm -rf "$PARENT"/.venv
  $PYTHON_BIN -m venv .venv
fi

source "$PARENT"/.venv/bin/activate
