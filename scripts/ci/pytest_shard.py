"""Partition the complete pytest collection by backend, including fixture parameters.

Loaded explicitly by the isolated CI harness; ordinary local pytest collection is unchanged.
Keep backend-dependent tests without a backend parameter in LIVE_TESTS / LIVE_MODULES.
"""

import json
import os
from pathlib import Path

import pytest


from scripts.ci.shards import CATALOG, SHARDS


BACKENDS = {
    **{flavor: shard for shard, spec in SHARDS.items() for flavor in spec['flavors']},
    'timescaledb-ha': 'postgres', 'postgresql': 'postgres', 'citus': 'postgres',
    'cockroachdb': 'postgres', 'duckdb': 'core', 'geopackage': 'core',
    'memory': 'core', 'custom': 'core',
}
BACKEND_PARAMETERS = {'flavor', 'pair', 'instance', 'other_instance', 'chain'}
LIVE_MODULES = {
    'tests/test_valkey_connector.py': 'mssql-valkey',
    'tests/test_w9u_pool_reuse.py': 'postgres',
    'tests/test_index_queries.py': 'postgres',
}
LIVE_TESTS = {
    'tests/test_access.py::test_valkey_teams_locks_and_users': 'mssql-valkey',
    'tests/test_utils_cache.py::test_valkey_backend_is_shared': 'mssql-valkey',
    'tests/test_native_sync.py::test_a_valkey_instance_syncs_natively': 'mssql-valkey',
    'tests/test_native_sync.py::test_postgres_numeric_special_values_read_back_like_v4': 'postgres',
    'tests/test_pipe_methods_native.py::test_remote_sync_time_of_a_valkey_source': 'mssql-valkey',
    'tests/test_facade_sql.py::test_a_roles_search_path_is_the_default_schema_without_sqlalchemy': 'postgres',
    'tests/test_w17b_env_uri.py::test_postgresql_uri_reaches_the_test_container': 'postgres',
}
# This deliberate invalid dialect exercises the error path without a database.
INVALID_BACKENDS = {
    ('tests/test_d14_bindings.py::test_sql_item_name_matches_python', 'flavor', 'nonsense'),
    ('tests/utils/test_sql.py::test_get_numeric_precision_scale', 'flavor', None),
    ('tests/utils/test_sql.py::test_get_numeric_precision_scale', 'flavor', 'not_a_flavor'),
}


def assignment(nodeid, parameters, fixtures=()):
    """Choose one lane, rejecting unknown dialects and unsupported cross-lane cases.

    Every lane provides file / memory instances, so a SQLite source and a server destination
    belong to the server's lane. Two different server families need an explicit new lane.
    """
    test = nodeid.split('[', 1)[0]
    module = test.split('::', 1)[0]
    lanes = set()
    explicit = LIVE_TESTS.get(test, LIVE_MODULES.get(module))
    if explicit:
        lanes.add(explicit)
    for name in BACKEND_PARAMETERS & parameters.keys():
        value = parameters[name]
        if not isinstance(value, str) or value not in BACKENDS:
            if (value is None or isinstance(value, str)) and (test, name, value) in INVALID_BACKENDS:
                continue
            raise ValueError(f'{nodeid}: unknown backend parameter {name}={value!r}')
        lanes.add(BACKENDS[value])
    if 'postgis_instance' in fixtures:
        lanes.add('postgres')
    servers = lanes - {'core'}
    if len(servers) > 1:
        raise ValueError(f'{nodeid}: needs multiple backend shards: {sorted(servers)}')
    return next(iter(servers), 'core')


def pytest_addoption(parser):
    parser.addoption('--mrsm-shard', choices=tuple(SHARDS), default=None)
    parser.addoption('--mrsm-collection-report', type=Path, default=None)


def pytest_configure(config):
    # Set only inside pytest: test.sh's setup must provision the selected runtime backends.
    os.environ['MRSM_TEST_COLLECTION_FLAVORS'] = ','.join(CATALOG)


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
    selected = config.getoption('--mrsm-shard')
    report = config.getoption('--mrsm-collection-report')
    if getattr(config, 'workerinput', {}).get('workerid', 'gw0') != 'gw0':
        report = None  # xdist proves equal collections; one writer keeps the report atomic.
    records, kept, dropped, errors = [], [], [], []
    for item in items:
        params = getattr(getattr(item, 'callspec', None), 'params', {})
        try:
            shard = assignment(item.nodeid, params, getattr(item, 'fixturenames', ()))
        except ValueError as error:
            errors.append(str(error))
            continue
        records.append({'nodeid': item.nodeid, 'shard': shard})
        (kept if selected is None or shard == selected else dropped).append(item)
    if errors:
        raise pytest.UsageError('\n'.join(errors))
    if report:
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(records, indent=2) + '\n')
    if dropped:
        config.hook.pytest_deselected(items=dropped)
        items[:] = kept
