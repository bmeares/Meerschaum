#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "$DIR"/config.sh

cd "$PARENT"
test_root="$PARENT/test_root"
mkdir -p "$test_root"
test_port="8989"
export MRSM_ROOT_DIR=$test_root

### Start the test databases.
if [ "$1" == "db" ]; then
  cd tests/
  docker-compose up -d
  echo "Sleeping for 15 seconds..."
  sleep 15
  cd ../
fi

### Install the `stress` plugin.
python -m meerschaum install plugin stress

### Start the test API.
api_exists=$(MRSM_ROOT_DIR="$test_root" python -m meerschaum show jobs test_api --nopretty)
if [ "$api_exists" != "test_api" ]; then
  python -m meerschaum start api \
    -w 1 -p $test_port --name test_api -y -d -i sql:memory
else
  python -m meerschaum start jobs test_api -y
fi
python -m meerschaum start jobs test_api -y

### This is necessary to trigger installations in a clean environment.
python -c "
from tests.connectors import conns
[conn.URI for conn in conns]
"

MRSM_CONNS=$(python -c "
from tests.connectors import conns
print(' '.join([str(c) for c in conns.values()]))")

MRSM_URIS=$(python -c "
from tests.connectors import conns
print(' '.join([
  'MRSM_' + c.type.upper() + '_' + c.label.upper() + '=' + c.URI
  for c in conns.values()
]))")
export $MRSM_URIS

python -m meerschaum start connectors $MRSM_CONNS

### Execute the pytest tests.
python -m pytest \
  --durations=0 \
  --ignore=portable/ --ignore=test_root/ --ignore=tests/data/ --ignore=docs/; rc="$?"

### Cleanup
if [ "$2" == "rm" ]; then
  cd tests/
  docker-compose down -v
  cd ../
  python -m meerschaum delete job test_api -f -y
fi

exit "$rc"
