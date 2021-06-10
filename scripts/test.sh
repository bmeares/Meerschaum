#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
test_root="test_root"
mkdir -p "$test_root"
test_port="8989"

### Start the test databases.
if [ "$1" == "db" ]; then
  cd tests/
  docker-compose up -d
  echo "Sleeping for 10 seconds"
  sleep 10
  cd ../
fi

### Start the test API.
MRSM_ROOT_DIR="$test_root" python -m meerschaum api start -w 1 -p $test_port &
api_pid=$!

### Execute the pytest tests.
python -m pytest --ignore=portable/ --ignore=test_root/ --ignore=tests/data/

### Cleanup
if [ "$1" == "db" ]; then
  cd tests/
  docker-compose down
  cd ../
fi
kill "$api_pid"
