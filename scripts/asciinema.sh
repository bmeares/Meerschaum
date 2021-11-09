#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

if [ -n "$1" ]; then
  name="$1"
else
  echo "Enter name for recording:"
  read name
  if [ -z "$name" ]; then
    echo "Invalid name."
    exit 1
  fi
fi

file_path="$PARENT/docs/mkdocs/assets/casts/$name.cast"
cd ~/
python -m asciinema rec "$file_path" --overwrite

