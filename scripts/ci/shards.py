"""The complete pytest backend catalog and per-lane resource budgets."""

SHARDS = {
    'core': {'flavors': ('sqlite',), 'services': (), 'workers': 4},
    'postgres': {
        'flavors': ('api', 'postgis', 'timescaledb', 'timescaledb_ha'),
        'services': ('postgresql', 'postgis', 'timescaledb', 'timescaledb_ha'),
        'workers': 2,
    },
    'mysql': {'flavors': ('mariadb', 'mysql'), 'services': ('mariadb', 'mysql'), 'workers': 1},
    'mssql-valkey': {'flavors': ('mssql', 'valkey'), 'services': ('mssql', 'valkey'), 'workers': 1},
    'oracle': {'flavors': ('oracle',), 'services': ('oracle',), 'workers': 8},
}
CATALOG = tuple(sorted(flavor for shard in SHARDS.values() for flavor in shard['flavors']))
