"""The complete pytest backend catalog and per-lane resource budgets.

Lanes are listed longest first: the pytest matrix is created in this order and runs at most
three lanes at once (one per self-hosted runner), so the long core lane never queues behind
the short backend lanes.

`dist` is the pytest-xdist scheduler (default `load`). The core lane uses `loadfile`: `load`
hands each worker one contiguous chunk of the collection up front and never rebalances it,
which put the whole browser suite (tests/console, contiguous in collection order) on one
worker for 18 to 20 minutes while the other three finished in 8 to 11.
"""

SHARDS = {
    'core': {'flavors': ('sqlite',), 'services': (), 'workers': 4, 'dist': 'loadfile'},
    'oracle': {'flavors': ('oracle',), 'services': ('oracle',), 'workers': 8},
    'mssql-valkey': {'flavors': ('mssql', 'valkey'), 'services': ('mssql', 'valkey'), 'workers': 1},
    'postgres': {
        'flavors': ('api', 'postgis', 'timescaledb', 'timescaledb_ha'),
        'services': ('postgresql', 'postgis', 'timescaledb', 'timescaledb_ha'),
        'workers': 2,
    },
    'mysql': {'flavors': ('mariadb', 'mysql'), 'services': ('mariadb', 'mysql'), 'workers': 1},
}
CATALOG = tuple(sorted(flavor for shard in SHARDS.values() for flavor in shard['flavors']))
