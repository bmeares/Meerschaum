#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

if [ "$1" == "clean" ] || [ ! -d "$PARENT/.venv" ]; then
  rm -rf "$PARENT"/.venv
  python -m venv .venv
fi

source "$PARENT"/.venv/bin/activate
