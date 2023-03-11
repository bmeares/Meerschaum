#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
test_root="$PARENT/test_root"
mkdir -p "$test_root"
test_port="8989"
export MRSM_ROOT_DIR=$test_root

[ -z "$PYTHON_BIN" ] && export PYTHON_BIN=python

### Start the test databases.
if [ "$1" == "db" ]; then
  cd tests/
  docker-compose up -d
  cd ../
fi

### Install the `stress` plugin.
$PYTHON_BIN -m meerschaum install plugin stress

### Start the test API.
api_exists=$(MRSM_ROOT_DIR="$test_root" $PYTHON_BIN -m meerschaum show jobs test_api --nopretty)
if [ "$api_exists" != "test_api" ]; then
  $PYTHON_BIN -m meerschaum start api \
    -w 1 -p $test_port --name test_api -y -d -i sql:memory
else
  $PYTHON_BIN -m meerschaum start jobs test_api -y
fi
$PYTHON_BIN -m meerschaum start jobs test_api -y
MRSM_API_TEST=http://user:pass@localhost:$test_port $PYTHON_BIN -m meerschaum start connectors api:test
$PYTHON_BIN -m meerschaum start jobs test_api -y

### This is necessary to trigger installations in a clean environment.
$PYTHON_BIN -c "
from tests.connectors import conns
[print((conn.engine if conn.type == 'sql' else conn)) for conn in conns.values()]
"

MRSM_CONNS=$($PYTHON_BIN -c "
from tests.connectors import conns
print(' '.join([str(c) for c in conns.values()]))")

MRSM_URIS=$($PYTHON_BIN -c "
from tests.connectors import conns
print(' '.join([
  'MRSM_' + c.type.upper() + '_' + c.label.upper() + '=' + c.URI
  for c in conns.values()
]))")
export $MRSM_URIS

$PYTHON_BIN -m meerschaum show connectors
$PYTHON_BIN -m meerschaum start connectors $MRSM_CONNS

export ff=""
if [ "$2" == "--ff" ]; then
  export ff="--ff"
fi

### Execute the pytest tests.
$PYTHON_BIN -m pytest \
  --durations=0 \
  --ignore=portable/ --ignore=test_root/ --ignore=tests/data/ --ignore=docs/ $ff; rc="$?"

### Cleanup
if [ "$2" == "rm" ]; then
  cd tests/
  docker-compose down -v
  cd ../
  $PYTHON_BIN -m meerschaum delete job test_api -f -y
fi

exit "$rc"
