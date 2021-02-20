#! /usr/bin/env bash

python -m pip install wheel pip ;
if [ "$dep_group" != "minimal" ]; then
  apt-get update && apt-get install -y --no-install-recommends \
    g++ libpq-dev python3-dev python3-venv && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

  python -m pip install --no-cache-dir --upgrade psycopg2 || exit 1

fi

if [ "$dep_group" != "minimal" ]; then
  apt-get purge -y `apt-get -s purge python3-dev | grep '^ ' | tr -d '*'`
fi


