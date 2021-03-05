#! /usr/bin/env bash

python -m pip install --upgrade wheel pip setuptools;
if [ "$dep_group" != "minimal" ]; then
  apt-get update && apt-get install -y --no-install-recommends \
    g++ make libpq-dev libffi-dev python3-dev && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

  python -m pip install --no-cache-dir --upgrade psycopg2 || exit 1
  python -m pip install pandas || exit 1

fi

if [ "$dep_group" != "minimal" ]; then
  apt-get purge -y `apt-get -s purge python3-dev | grep '^ ' | tr -d '*'`
fi


