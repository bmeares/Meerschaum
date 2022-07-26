#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
test_root="$PARENT/test_root"
mkdir -p "$test_root"
test_port="8989"

### Start the test databases.
if [ "$1" == "db" ]; then
  cd tests/
  docker-compose up -d
  echo "Sleeping for 15 seconds..."
  sleep 15
  cd ../
fi

### Install the `stress` plugin.
MRSM_ROOT_DIR="$test_root" python -m meerschaum install plugin stress

### Start the test API.
api_exists=$(MRSM_ROOT_DIR="$test_root" python -m meerschaum show jobs test_api --nopretty)
if [ "$api_exists" != "test_api" ]; then
  MRSM_ROOT_DIR="$test_root" python -m meerschaum start api -w 1 -p $test_port --name test_api -y -d -i sql:memory
else
  MRSM_ROOT_DIR="$test_root" python -m meerschaum start jobs test_api -y
fi
MRSM_ROOT_DIR="$test_root" python -m meerschaum start jobs test_api -y
echo "Sleeping for 4 seconds..."
sleep 4


### Execute the pytest tests.
MRSM_ROOT_DIR="$test_root" python -m pytest \
  --ignore=portable/ --ignore=test_root/ --ignore=tests/data/ --ignore=docs/; rc="$?"

### Cleanup
if [ "$2" == "rm" ]; then
  cd tests/
  docker-compose down -v
  cd ../
  MRSM_ROOT_DIR="$test_root" python -m meerschaum delete job test_api -f -y
fi

exit "$rc"
