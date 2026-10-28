#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
test_root="$PARENT/test_root"
test_plugins="$PARENT/tests/plugins"
mrsm="$PARENT/scripts/mrsm.sh"
mkdir -p "$test_root"
test_port="${MRSM_TEST_API_PORT:-8989}"
export MRSM_TEST_API_PORT="$test_port"
export MRSM_ROOT_DIR=$test_root
export MRSM_PLUGINS_DIR=$test_plugins
export MRSM_API_TEST=http://user:pass@localhost:$test_port
export MRSM_SQL_TEST_POSTGRESQL='postgresql://test:test1234@localhost:5559/testdb'
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

### Parse flags; remaining args forwarded to pytest.
start_db=""
cleanup_on_exit=""
while [ $# -gt 0 ]; do
  case "$1" in
    db)  start_db=1; shift ;;
    rm)  cleanup_on_exit="rm"; shift ;;
    *)   break ;;
  esac
done

### Start the test databases.
if [ -n "$start_db" ]; then
  cd tests/
  docker compose up --quiet-pull -d $services
  cd ../
fi

### The tests never download packages mid-run (`MRSM_NO_AUTO_INSTALL`): install the extras they
### need into the test interpreter up front. `MRSM_INSTALL_PACKAGES` names the extras (the
### `requirements/<extra>.txt` files); `MRSM_TEST_PIP_ARGS` adds `pip install` flags and
### `MRSM_TEST_SKIP_INSTALL=1` skips the step when the interpreter is already provisioned
### (the CI workflow provisions a dedicated venv). It only installs into a virtual environment:
### set `MRSM_TEST_ALLOW_SYSTEM_INSTALL=1` to install into the interpreter itself.
[ -z "$MRSM_INSTALL_PACKAGES" ] && export MRSM_INSTALL_PACKAGES='full sql-mssql sql-oracle sql-duckdb'
if [ -z "$MRSM_TEST_SKIP_INSTALL" ]; then
  if [ -z "$MRSM_TEST_ALLOW_SYSTEM_INSTALL" ] && \
     ! $PYTHON_BIN -c 'import sys; sys.exit(0 if sys.prefix != sys.base_prefix else 1)'; then
    echo "test.sh: '$PYTHON_BIN' is not a virtual environment, so the test dependencies are not" >&2
    echo "  installed into it. Create one (python -m venv .venv-test) and set" >&2
    echo "  PYTHON_BIN=.venv-test/bin/python, provision the interpreter yourself and set" >&2
    echo "  MRSM_TEST_SKIP_INSTALL=1, or set MRSM_TEST_ALLOW_SYSTEM_INSTALL=1." >&2
    exit 2
  fi
  requirement_args=""
  for extra in $MRSM_INSTALL_PACKAGES; do
    requirement_args="$requirement_args -r $PARENT/requirements/$extra.txt"
  done
  ### `httpx2` is the client of `starlette.testclient` in the API host tests (Starlette 1.9 deprecates
  ### `httpx` there and falls back to it with a warning); neither is a dependency of the host itself.
  $PYTHON_BIN -m pip install --quiet $MRSM_TEST_PIP_ARGS $requirement_args httpx httpx2; rc="$?"
  [ "$rc" != '0' ] && exit "$rc"
fi
export MRSM_NO_AUTO_INSTALL=1

### Native-only DuckDB fixtures need a shared library; ordinary SDK tests share the installed
### Python driver's database instance with SQLAlchemy. Keep the native fixture path in a test-only
### variable: MRSM_DUCKDB_LIB is a product override that explicitly selects the native-only driver.
### Suppress the download helper's GITHUB_ENV export so later Python steps keep the default policy.
MRSM_TEST_DUCKDB_LIB="${MRSM_TEST_DUCKDB_LIB:-${MRSM_DUCKDB_LIB:-}}"
if [ -z "$MRSM_TEST_DUCKDB_LIB" ]; then
  if MRSM_TEST_DUCKDB_LIB="$(env -u GITHUB_ENV "$PARENT/scripts/rust/duckdb_lib.sh")"; then
    export MRSM_TEST_DUCKDB_LIB
  else
    unset MRSM_TEST_DUCKDB_LIB
    echo "test.sh: could not provide the DuckDB library; tests that use DuckDB will fail." >&2
  fi
fi
export MRSM_TEST_DUCKDB_LIB
unset MRSM_DUCKDB_LIB

$mrsm stop daemon

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

### `all` was unset above so `get_flavors()` returns every flavor. Export the resolved list: modules
### that choose their own instances (`postgis`, ...) read MRSM_TEST_FLAVORS and would otherwise
### fall back to SQLite only.
MRSM_TEST_FLAVORS=$($PYTHON_BIN -c "
from tests.connectors import get_flavors
print(','.join(get_flavors()))
"); rc="$?"
[ "$rc" != '0' ] && exit "$rc"
[ -z "$MRSM_TEST_FLAVORS" ] && echo "test.sh: no test flavors resolved." >&2 && exit 1
export MRSM_TEST_FLAVORS

### The API suite mutates its pipes and jobs. Give only its broad fixture account explicit
### admin rights on the owned backing instance; registration and the default role stay unchanged.
if [[ ",$MRSM_TEST_FLAVORS," == *,api,* ]]; then
  cd tests/ && docker compose up -d postgresql && cd .. && \
    $mrsm delete jobs test_api -y && \
    $mrsm start connectors sql:test_postgresql; rc="$?"
  [ "$rc" != '0' ] && exit "$rc"
  "$PYTHON_BIN" - <<'API_FIXTURE_SEED'
from tests.connectors import conns
from meerschaum import get_connector
from meerschaum.core import User

api = conns['api']
backing = get_connector('sql', 'test_postgresql')
user = User(api.username, api.password, email='none@none.com', type='admin', instance=backing)
result = (
    backing.edit_user(user)
    if backing.get_user_id(user) is not None
    else backing.register_user(user)
)
assert result[0], 'Could not seed the API integration account.'
assert backing.get_user_type(user) == 'admin', 'The API integration account must be an admin.'
API_FIXTURE_SEED
  rc="$?"
  [ "$rc" != '0' ] && exit "$rc"
  ### Restart once after the first health check, as the integration harness has always done.
  $mrsm start api \
    -w 1 -p $test_port --name test_api -y -d -i sql:test_postgresql --no-webterm --no-dash && \
    $mrsm start connectors api:test && \
    $mrsm stop jobs test_api -y && \
    $mrsm start jobs test_api -y && \
    $mrsm start connectors api:test; rc="$?"
  [ "$rc" != '0' ] && exit "$rc"
  "$PYTHON_BIN" - <<'API_FIXTURE_LOGIN'
import base64
import json
from tests.connectors import conns

api = conns['api']
assert api.login(debug=False), 'The API integration account could not authenticate.'
segment = api._token.split('.')[1]
claims = json.loads(base64.urlsafe_b64decode(segment + '=' * (-len(segment) % 4)))
assert claims.get('scopes') == ['*'], 'The API integration account must receive wildcard scopes.'
assert claims.get('type') == 'admin', 'The API integration login must identify its admin account.'
API_FIXTURE_LOGIN
  rc="$?"
  [ "$rc" != '0' ] && exit "$rc"
fi

true && \
  $mrsm delete cache -y && \
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
  -v \
  "$@"; rc="$?"

### Cleanup
if [ "$cleanup_on_exit" == "rm" ]; then
  cd tests/
  docker compose down -v
  cd ../
  $mrsm delete job test_api -f -y
fi

exit "$rc"
