#! /usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="$DIR/../"

cd "$ROOT"

if [ ! -d "./scripts/_site-packages_original" ]; then
  echo "Not installed. Continuing..."
else
  cd ./scripts
  mv _site-packages_original site-packages
  cd "$ROOT"
  rm -rf ./python/lib/python3.11/site-packages
  mv ./scripts/site-packages ./python/python3.11/
fi

cd "$ROOT"
rm -rf root && mkdir root

