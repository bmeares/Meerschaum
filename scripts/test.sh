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
  echo "Sleeping for 5 seconds"
  sleep 5
  cd ../
fi

### Install the `stress` plugin.
MRSM_ROOT_DIR="$test_root" python -m meerschaum install plugin stress

### Start the test API.
MRSM_ROOT_DIR="$test_root" python -m meerschaum start api -w 1 -p $test_port -d --name test_api

### Execute the pytest tests.
MRSM_ROOT_DIR="$test_root" python -m pytest --ignore=portable/ --ignore=test_root/ --ignore=tests/data/

### Cleanup
# if [ "$1" == "db" ]; then
  # cd tests/
  # docker-compose down
  # cd ../
# fi
MRSM_ROOT_DIR="$test_root" python -m meerschaum delete job test_api -f -y
