#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test pipe maintenance operations: `get_size()`, `vacuum()`, `analyze()`, and `compress()`,
plus their corresponding actions (`vacuum`, `analyze`, `compress`).
"""

import pytest
from datetime import datetime

import meerschaum as mrsm
from meerschaum.actions import actions
from meerschaum.connectors.sql._maintenance import (
    VACUUMABLE_FLAVORS,
    ANALYZABLE_FLAVORS,
    _FULL_VACUUM_FLAVORS,
)
from meerschaum.connectors.sql._compress import (
    COMPRESSIBLE_FLAVORS,
    _normalize_columnstore_cols,
)

from tests import debug
from tests.connectors import conns, get_flavors

### Flavors with a portable per-table size query in `SQLConnector.get_pipe_size`.
SIZE_FLAVORS = {
    'timescaledb', 'timescaledb-ha', 'postgresql', 'postgis', 'citus', 'cockroachdb',
    'mysql', 'mariadb', 'mssql', 'sqlite', 'geopackage',
}

### Compressible flavors whose compression statement is edition-independent and thus reliable in
### CI. MSSQL `DATA_COMPRESSION` depends on the server edition, so we only smoke-test it.
RELIABLE_COMPRESS_FLAVORS = {'timescaledb', 'timescaledb-ha', 'mysql', 'mariadb'}


def _build_synced_pipe(conn, location: str):
    """Register and sync a small stress pipe used as a maintenance target."""
    pipe = mrsm.Pipe('plugin:stress', 'test', location, instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'plugin:stress', 'test', location,
        instance=conn,
        columns=['datetime', 'id'],
        parameters={'fetch': {'rows': 100, 'id': 3}},
    )
    success, msg = pipe.sync(
        begin=datetime(2023, 1, 1), end=datetime(2023, 1, 2), debug=debug,
    )
    assert success, msg
    return pipe


def _assert_success_tuple(result):
    assert isinstance(result, tuple), result
    assert len(result) == 2, result
    assert isinstance(result[0], bool), result
    assert isinstance(result[1], str), result


@pytest.mark.parametrize("flavor", get_flavors())
def test_get_size(flavor: str):
    """`Pipe.get_size()` returns a positive byte count on size-supporting flavors."""
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    pipe = _build_synced_pipe(conn, 'maintenance_size')
    size = pipe.get_size(debug=debug)
    if conn.flavor in SIZE_FLAVORS:
        assert isinstance(size, int), f"Expected an int size, got {size!r}."
        assert size > 0, f"Expected a positive size, got {size}."
    else:
        assert size is None or isinstance(size, int)


@pytest.mark.parametrize("flavor", get_flavors())
def test_vacuum(flavor: str):
    """`Pipe.vacuum()` reclaims space without dropping rows on vacuumable flavors."""
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    pipe = _build_synced_pipe(conn, 'maintenance_vacuum')
    rowcount_before = pipe.get_rowcount(debug=debug)

    success, msg = pipe.vacuum(debug=debug)
    _assert_success_tuple((success, msg))
    if conn.flavor in VACUUMABLE_FLAVORS:
        assert success, msg
        ### Vacuuming must never lose data.
        assert pipe.get_rowcount(debug=debug) == rowcount_before
    else:
        assert not success, "Vacuuming should be unsupported on this flavor."


@pytest.mark.parametrize("flavor", get_flavors())
def test_vacuum_full(flavor: str):
    """`Pipe.vacuum(full=True)` succeeds on PostgreSQL-family flavors."""
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    if conn.flavor not in _FULL_VACUUM_FLAVORS:
        return
    pipe = _build_synced_pipe(conn, 'maintenance_vacuum_full')
    rowcount_before = pipe.get_rowcount(debug=debug)
    success, msg = pipe.vacuum(full=True, debug=debug)
    assert success, msg
    assert pipe.get_rowcount(debug=debug) == rowcount_before


@pytest.mark.parametrize("flavor", get_flavors())
def test_analyze(flavor: str):
    """`Pipe.analyze()` refreshes planner statistics without touching data."""
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    pipe = _build_synced_pipe(conn, 'maintenance_analyze')
    rowcount_before = pipe.get_rowcount(debug=debug)

    success, msg = pipe.analyze(debug=debug)
    _assert_success_tuple((success, msg))
    if conn.flavor in ANALYZABLE_FLAVORS:
        assert success, msg
        assert pipe.get_rowcount(debug=debug) == rowcount_before
    else:
        assert not success, "Analyzing should be unsupported on this flavor."


@pytest.mark.parametrize("flavor", get_flavors())
def test_compress(flavor: str):
    """`Pipe.compress()` succeeds on reliably-compressible flavors without losing data."""
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    pipe = _build_synced_pipe(conn, 'maintenance_compress')
    rowcount_before = pipe.get_rowcount(debug=debug)

    success, msg = pipe.compress(debug=debug)
    _assert_success_tuple((success, msg))
    if conn.flavor in RELIABLE_COMPRESS_FLAVORS:
        assert success, msg
        assert pipe.get_rowcount(debug=debug) == rowcount_before
    elif conn.flavor not in COMPRESSIBLE_FLAVORS:
        assert not success, "Compression should be unsupported on this flavor."


@pytest.mark.parametrize("flavor", get_flavors())
def test_unsupported_instance_maintenance(flavor: str):
    """Non-SQL instance connectors return a graceful failure rather than raising."""
    conn = conns[flavor]
    if conn.type != 'valkey':
        return
    pipe = mrsm.Pipe('plugin:stress', 'test', 'maintenance_unsupported', instance=conn)
    for method in ('vacuum', 'analyze', 'compress'):
        result = getattr(pipe, method)(debug=debug)
        _assert_success_tuple(result)
        assert not result[0], f"{method} should be unsupported for '{conn.type}'."


### -------------------------------------------------------------------------------------------
### Columnstore (compression) settings — quoting and direction handling. These exercise pure
### query-building logic and do not touch a live database, so they run on every flavor set.
### -------------------------------------------------------------------------------------------

@pytest.mark.parametrize("value,flavor,allow_direction,expected", [
    ### A bare mixed-case identifier must be double-quoted (the reported regression: unquoted
    ### `startTime` is folded to `starttime` by PostgreSQL and rejected by TimescaleDB).
    ('startTime', 'postgresql', False, ['"startTime"']),
    ### Comma-separated string splits into individually-quoted columns.
    ('col1, col2', 'postgresql', False, ['"col1"', '"col2"']),
    ### `ASC` / `DESC` is preserved and upper-cased; the column stays quoted.
    ('startTime desc', 'postgresql', True, ['"startTime" DESC']),
    ('startTime DESC, endTime', 'postgresql', True, ['"startTime" DESC', '"endTime"']),
    ### List form with mixed directions.
    (['startTime desc', 'endTime asc'], 'postgresql', True, ['"startTime" DESC', '"endTime" ASC']),
    ### A list element that is itself comma-separated is flattened.
    (['a, b DESC', 'c'], 'postgresql', True, ['"a"', '"b" DESC', '"c"']),
    ### `NULLS FIRST` / `NULLS LAST` modifiers are preserved.
    ('ts DESC NULLS LAST', 'postgresql', True, ['"ts" DESC NULLS LAST']),
    ('ts NULLS FIRST', 'postgresql', True, ['"ts" NULLS FIRST']),
    ### Empty / whitespace-only entries (e.g. a trailing comma) are dropped.
    ('a, , b', 'postgresql', False, ['"a"', '"b"']),
    ('', 'postgresql', False, []),
    (['', '  '], 'postgresql', True, []),
    ### Flavor-specific quote character.
    ('startTime', 'mysql', False, ['`startTime`']),
    ('startTime', 'mariadb', False, ['`startTime`']),
    ### With direction disallowed (segmentby), a trailing keyword is part of the identifier.
    ('startTime DESC', 'postgresql', False, ['"startTime DESC"']),
    ### An unrecognized trailing token is NOT treated as a direction; the whole entry is quoted
    ### as one identifier rather than guessing where the column ends.
    ('col FOO', 'postgresql', True, ['"col FOO"']),
])
def test_normalize_columnstore_cols(value, flavor, allow_direction, expected):
    """`_normalize_columnstore_cols` quotes identifiers and preserves recognized directions."""
    assert _normalize_columnstore_cols(value, flavor, allow_direction) == expected


def test_compress_settings_default_quoting():
    """A mixed-case datetime column is quoted in the default `orderby` (the reported bug)."""
    conn = conns['sqlite']
    pipe = mrsm.Pipe(
        'plugin:timeline', 'activities', 'compress_quote',
        instance=conn,
        columns={'datetime': 'startTime'},
    )
    settings = conn._get_compress_settings(pipe)
    assert settings['orderby'] == ['"startTime" DESC']

    query = conn._get_columnstore_settings_query(pipe)
    assert 'timescaledb.orderby = \'"startTime" DESC\'' in query
    ### Guard against the lowercase-folding regression returning.
    assert "orderby = 'startTime DESC'" not in query


def test_compress_settings_id_placement():
    """A non-unique `id` goes to `segmentby`; a unique (primary) `id` goes to `orderby`."""
    conn = conns['sqlite']

    non_unique = mrsm.Pipe(
        'plugin:x', 'y', 'seg_nonunique',
        instance=conn,
        columns={'datetime': 'ts', 'id': 'device'},
    )
    settings = conn._get_compress_settings(non_unique)
    assert settings['segmentby'] == ['"device"']
    assert settings['orderby'] == ['"ts" DESC']

    unique = mrsm.Pipe(
        'plugin:x', 'y', 'seg_unique',
        instance=conn,
        columns={'datetime': 'ts', 'id': 'pk', 'primary': 'pk'},
    )
    settings = conn._get_compress_settings(unique)
    assert settings['segmentby'] == []
    assert settings['orderby'] == ['"ts" DESC', '"pk"']


def test_compress_settings_user_overrides():
    """User-supplied `compress` dict specs are split and quoted, with directions preserved."""
    conn = conns['sqlite']
    pipe = mrsm.Pipe(
        'plugin:x', 'y', 'overrides',
        instance=conn,
        columns={'datetime': 'startTime'},
        parameters={'compress': {
            'segmentby': 'col1, col2',
            'orderby': 'startTime DESC, endTime',
        }},
    )
    settings = conn._get_compress_settings(pipe)
    assert settings['segmentby'] == ['"col1"', '"col2"']
    assert settings['orderby'] == ['"startTime" DESC', '"endTime"']


@pytest.mark.parametrize("flavor", get_flavors())
def test_maintenance_actions(flavor: str):
    """The `vacuum`, `analyze`, and `compress` actions dispatch to the instance connector."""
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    pipe = _build_synced_pipe(conn, 'maintenance_actions')
    mrsm_instance = str(pipe.instance_connector)
    keys = dict(
        connector_keys=[pipe.connector_keys],
        metric_keys=[pipe.metric_key],
        location_keys=[pipe.location_key],
        mrsm_instance=mrsm_instance,
        yes=True,
        force=True,
    )

    for action_name, supported in (
        ('vacuum', conn.flavor in VACUUMABLE_FLAVORS),
        ('analyze', conn.flavor in ANALYZABLE_FLAVORS),
        ('compress', conn.flavor in RELIABLE_COMPRESS_FLAVORS),
    ):
        success, msg = actions[action_name](['pipes'], debug=debug, **keys)
        _assert_success_tuple((success, msg))
        if supported:
            assert success, f"`{action_name} pipes` failed: {msg}"
