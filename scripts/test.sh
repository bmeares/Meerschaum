#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
test_root="$PARENT/test_root"
test_plugins="$PARENT/tests/plugins"
mkdir -p "$test_root"
test_port="8989"
export MRSM_ROOT_DIR=$test_root
export MRSM_PLUGINS_DIR=$test_plugins

[ -z "$PYTHON_BIN" ] && export PYTHON_BIN=python
[ -z "$MRSM_TEST_FLAVORS" ] && export MRSM_TEST_FLAVORS='api,timescaledb'
if [ "$MRSM_TEST_FLAVORS" = "all" ]; then
  unset MRSM_TEST_FLAVORS
fi
if [ ! -z "$MRSM_TEST_FLAVORS" ]; then
  services=`echo "$MRSM_TEST_FLAVORS" | sed 's/,/ /g'`
  services=`echo "$services" | sed 's/api//g'`
fi

### Start the test databases.
if [ "$1" == "db" ]; then
  cd tests/
  docker-compose up -d $services
  cd ../
fi

rm -rf "$test_root/data"

### Start the test API.
$PYTHON_BIN -m meerschaum delete jobs test_api -y --timeout 10
$PYTHON_BIN -m meerschaum start api \
  -w 1 -p $test_port --name test_api -y -d -i sql:local
MRSM_API_TEST=http://user:pass@localhost:$test_port $PYTHON_BIN -m meerschaum start connectors api:test

### This is necessary to trigger installations in a clean environment.
$PYTHON_BIN -c "
from tests.connectors import conns, get_flavors
flavors = get_flavors()
print(flavors)
for flavor, conn in conns.items():
    if flavor not in flavors:
        continue
    if conn.type == 'sql':
        print(conn.engine)
    else:
        print(conn)
"

MRSM_CONNS=$($PYTHON_BIN -c "
from tests.connectors import conns, get_flavors
flavors = get_flavors()
conns_to_run = [str(conn) for flavor, conn in conns.items() if flavor in flavors]
print(' '.join(conns_to_run))
")

MRSM_URIS=$($PYTHON_BIN -c "
from tests.connectors import conns, get_flavors
flavors = get_flavors()
print(' '.join([
  (
    'MRSM_'
    + c.type.upper()
    + '_'
    + c.label.upper()
    + '='
    + (
      c.URI.replace('postgresql', 'timescaledb')
      if f == 'timescaledb'
      else c.URI
    )
  )
  for f, c in conns.items()
  if f in flavors
]))")
export $MRSM_URIS

$PYTHON_BIN -m meerschaum show connectors
$PYTHON_BIN -m meerschaum start connectors $MRSM_CONNS
$PYTHON_BIN -m pytest \
  --durations=0 \
  --ignore=portable/ \
  --ignore=test_root/ \
  --ignore=tests/data/ \
  --ignore=docs/ \
  --ff \
  --full-trace \
  -v; rc="$?"

### Cleanup
if [ "$2" == "rm" ]; then
  cd tests/
  docker-compose down -v
  cd ../
  $PYTHON_BIN -m meerschaum delete job test_api -f -y
fi

exit "$rc"
