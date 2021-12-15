#! /usr/bin/env bash

python -m pip install --upgrade wheel pip setuptools;
if [ "$dep_group" != "minimal" ]; then
  apt-get update && apt-get install -y --no-install-recommends \
    g++ make libpq-dev libffi-dev python3-dev || exit 1

  if [ "$dep_group" == "full" ]; then
    apt-get install -y --no-install-recommends \
      libglib2.0-dev libgirepository1.0-dev libcairo2-dev pkg-config libgtk-3-dev gir1.2-webkit2-4.0 || exit 1
  fi

  apt-get clean && \
    rm -rf /var/lib/apt/lists/*

  python -m pip install --no-cache-dir --upgrade psycopg2 || exit 1
  python -m pip install pandas || exit 1
fi

if [ "$dep_group" != "minimal" ]; then
  apt-get purge -y $(apt-get -s purge python3-dev | grep '^ ' | tr -d '*')
fi
