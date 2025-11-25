#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
test_root="$PARENT/test_root"
test_plugins="$PARENT/tests/plugins"
mrsm="$PARENT/scripts/mrsm.sh"
mkdir -p "$test_root"
test_port="8989"
export MRSM_ROOT_DIR=$test_root
export MRSM_PLUGINS_DIR=$test_plugins
export MRSM_API_TEST=http://user:pass@localhost:$test_port
[ -z "$PYTHON_BIN" ] && export PYTHON_BIN=python
[ -z "$MRSM_TEST_FLAVORS" ] && export MRSM_TEST_FLAVORS='api,timescaledb'
if [ "$MRSM_TEST_FLAVORS" = "all" ]; then
  unset MRSM_TEST_FLAVORS
fi
if [ ! -z "$MRSM_TEST_FLAVORS" ]; then
  services=`echo "$MRSM_TEST_FLAVORS" | sed 's/,/ /g'`
  services=`echo "$services" | sed 's/api//g'`
fi
if [ -z "$MRSM_DEBUG" ]; then
  export MRSM_DEBUG='false'
fi

### Start the test databases.
if [ "$1" == "db" ]; then
  cd tests/
  docker compose up --quiet-pull -d $services
  cd ../
fi

$mrsm stop daemon

if [ ! -z "$MRSM_INSTALL_PACKAGES" ]; then
  $mrsm install packages setuptools wheel -y
  $mrsm upgrade packages $MRSM_INSTALL_PACKAGES -y
fi

rm -rf "$test_root/data"
rm -f "$test_root/sqlite/mrsm_local.db"

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

connectors_uri_code="
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
]))
"

$PYTHON_BIN -c "$connectors_uri_code"

MRSM_URIS=$($PYTHON_BIN -c "$connectors_uri_code")
export $MRSM_URIS
export MRSM_SQL_TEST_POSTGRESQL='postgresql://test:test1234@localhost:5559/testdb'

### Start the test API on port 8989.
### For whatever reason, GitHub actions only works
### when the server is started, stopped, and started again.
[ -z "$MRSM_TEST_FLAVORS" ] || [[ "$MRSM_TEST_FLAVORS" =~ "api" ]] && \
  cd tests/ && docker compose up -d postgresql && cd .. \
  $mrsm delete jobs test_api -y && \
  $mrsm start api \
    -w 1 -p $test_port --name test_api -y -d -i sql:test_postgresql --no-webterm --no-dash && \
  $mrsm start connectors api:test && \
  $mrsm stop jobs test_api -y && \
  $mrsm start jobs test_api -y && \
  $mrsm start connectors api:test

true && \
  $mrsm show connectors && \
  $mrsm start connectors $MRSM_CONNS; rc="$?"
[ "$rc" != '0' ] && exit "$rc"

$PYTHON_BIN -m pytest \
  tests/test_users.py; rc="$?"
[ "$rc" != '0' ] && exit "$rc"

$PYTHON_BIN -m pytest \
  --durations=0 \
  --ignore=portable/ \
  --ignore=test_root/ \
  --ignore=tests/data/ \
  --ignore=docs/ \
  --ff \
  -n=auto \
  -v; rc="$?"

### Cleanup
if [ "$2" == "rm" ]; then
  cd tests/
  docker compose down -v
  cd ../
  $mrsm delete job test_api -f -y
fi

exit "$rc"
