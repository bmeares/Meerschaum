#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test native range partitioning (`hypertable=True` on PostgreSQL/MySQL/MSSQL) and the
epoch-aligned chunk bounds that coincide with the resulting partition edges.
"""

import pytest
from datetime import datetime, timedelta, timezone

import meerschaum as mrsm
from meerschaum.connectors.sql._partition import (
    PARTITIONABLE_FLAVORS,
    PG_PARTITION_FLAVORS,
    MYSQL_PARTITION_FLAVORS,
)
from meerschaum.utils.sql import _get_create_table_query_from_dtypes

from tests import debug
from tests.connectors import conns, get_flavors

CHUNK_MINUTES = 1440  # one-day partitions
BEGIN = datetime(2023, 1, 1, tzinfo=timezone.utc)
END = datetime(2023, 1, 5, tzinfo=timezone.utc)


def _deterministic_data():
    """
    Build a fixed dataset of hourly rows spanning four one-day partitions
    (2023-01-01 .. 2023-01-04). Deterministic so re-syncs are idempotent — unlike the
    `plugin:stress` fetch, which randomizes `id`/`val` on every call.
    """
    rows = []
    cursor = datetime(2023, 1, 1)
    while cursor < datetime(2023, 1, 5):
        rows.append({'datetime': cursor, 'id': (cursor.day % 3) + 1, 'val': cursor.hour})
        cursor += timedelta(hours=1)
    return rows


def _count_partitions(conn, pipe) -> int:
    """Return the number of native partitions for a pipe's target table (-1 if unknown)."""
    from meerschaum.utils.sql import sql_item_name
    flavor = conn.flavor
    schema = conn.get_pipe_schema(pipe)
    if flavor in PG_PARTITION_FLAVORS:
        full_name = sql_item_name(pipe.target, flavor, schema)
        query = (
            "SELECT COUNT(*) FROM pg_inherits "
            f"WHERE inhparent = '{full_name}'::regclass"
        )
        val = conn.value(query, debug=debug)
        return int(val) if val is not None else -1
    if flavor in MYSQL_PARTITION_FLAVORS:
        query = (
            "SELECT COUNT(*) FROM information_schema.PARTITIONS "
            f"WHERE TABLE_NAME = '{pipe.target}' "
            "AND PARTITION_NAME IS NOT NULL"
        )
        val = conn.value(query, debug=debug)
        return int(val) if val is not None else -1
    return -1


### A mixed-case partition column must be quoted in the generated DDL. An unquoted identifier is
### folded to lowercase by PostgreSQL (and is fragile on the others), so partitioning on a column
### like `startTime` would fail — the same identifier-quoting class of bug fixed in `_compress.py`.
### `_get_create_table_query_from_dtypes` is pure (no database), so this runs on every flavor set.
@pytest.mark.parametrize("flavor,quote", [
    ('postgresql', '"startTime"'),
    ('postgis', '"startTime"'),
    ('mysql', '`startTime`'),
    ('mariadb', '`startTime`'),
    ('mssql', '[startTime]'),
])
def test_partition_ddl_quotes_mixed_case_column(flavor: str, quote: str):
    """The partition column is wrapped via `sql_item_name`, never interpolated bare."""
    dtypes = {'startTime': 'datetime', 'id': 'int', 'val': 'int'}
    ### MySQL/MariaDB declare initial partitions inline and need `(name, literal)` bounds.
    partition_bounds = (
        [('p20230101', "'2023-01-02 00:00:00'")]
        if flavor in MYSQL_PARTITION_FLAVORS
        else None
    )
    queries = _get_create_table_query_from_dtypes(
        dtypes, 'tbl', flavor,
        datetime_column='startTime',
        primary_key='id',
        partition_by_column='startTime',
        partition_bounds=partition_bounds,
        partition_scheme_name='ps_tbl',
    )
    ddl = '\n'.join(queries)
    assert quote in ddl, f"Expected quoted partition column {quote!r} in:\n{ddl}"
    ### The lowercase-folded bare form must never appear in a partition clause.
    assert 'RANGE (startTime' not in ddl and 'COLUMNS (startTime' not in ddl


@pytest.mark.parametrize("flavor", get_flavors())
def test_partitioned_sync_roundtrip(flavor: str):
    """A partitioned pipe round-trips data identically to a plain pipe."""
    conn = conns[flavor]
    if conn.type != 'sql' or conn.flavor not in PARTITIONABLE_FLAVORS:
        return

    data = _deterministic_data()

    part_pipe = mrsm.Pipe('test', 'partition', 'partitioned', instance=conn)
    part_pipe.delete()
    part_pipe = mrsm.Pipe(
        'test', 'partition', 'partitioned',
        instance=conn,
        columns=['datetime', 'id'],
        parameters={
            'hypertable': True,
            'verify': {'chunk_minutes': CHUNK_MINUTES},
        },
    )
    assert conn._should_partition(part_pipe)

    plain_pipe = mrsm.Pipe('test', 'partition', 'unpartitioned', instance=conn)
    plain_pipe.delete()
    plain_pipe = mrsm.Pipe(
        'test', 'partition', 'unpartitioned',
        instance=conn,
        columns=['datetime', 'id'],
        ### Partitioning is on by default for these flavors, so opt out explicitly.
        parameters={'hypertable': False},
    )
    assert not conn._should_partition(plain_pipe)

    for pipe in (part_pipe, plain_pipe):
        success, msg = pipe.sync(data, debug=debug)
        assert success, msg

    ### Spanning four one-day partitions, the partitioned table must hold its data across them.
    assert part_pipe.get_rowcount(debug=debug) == plain_pipe.get_rowcount(debug=debug)

    part_df = part_pipe.get_data(debug=debug)
    plain_df = plain_pipe.get_data(debug=debug)
    assert len(part_df) == len(plain_df)
    assert part_df['datetime'].min() == plain_df['datetime'].min()
    assert part_df['datetime'].max() == plain_df['datetime'].max()

    n_partitions = _count_partitions(conn, part_pipe)
    if n_partitions != -1:
        assert n_partitions > 1, (
            f"Expected multiple partitions over {BEGIN}..{END}, got {n_partitions}."
        )


@pytest.mark.parametrize("flavor", get_flavors())
def test_partitioned_resync_idempotent(flavor: str):
    """Re-syncing the same range into a partitioned pipe does not duplicate rows."""
    conn = conns[flavor]
    if conn.type != 'sql' or conn.flavor not in PARTITIONABLE_FLAVORS:
        return

    data = _deterministic_data()
    pipe = mrsm.Pipe('test', 'partition', 'partition_resync', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'partition', 'partition_resync',
        instance=conn,
        columns=['datetime', 'id'],
        parameters={
            'hypertable': True,
            'verify': {'chunk_minutes': CHUNK_MINUTES},
        },
    )
    success, msg = pipe.sync(data, debug=debug)
    assert success, msg
    rowcount = pipe.get_rowcount(debug=debug)

    success, msg = pipe.sync(data, debug=debug)
    assert success, msg
    assert pipe.get_rowcount(debug=debug) == rowcount


@pytest.mark.parametrize("flavor", get_flavors())
def test_partition_bounds_deterministic(flavor: str):
    """`_partition_bounds` is epoch-anchored and value-independent within an interval."""
    conn = conns[flavor]
    if conn.type != 'sql':
        return

    interval = timedelta(days=1)
    lo_a, hi_a = conn._partition_bounds(datetime(2023, 1, 1, 13, 30, tzinfo=timezone.utc), interval)
    lo_b, hi_b = conn._partition_bounds(datetime(2023, 1, 1, 2, 5, tzinfo=timezone.utc), interval)
    assert (lo_a, hi_a) == (lo_b, hi_b)
    assert lo_a == datetime(2023, 1, 1, tzinfo=timezone.utc)
    assert hi_a == datetime(2023, 1, 2, tzinfo=timezone.utc)

    ### Integer axis (epoch-int datetimes).
    lo_i, hi_i = conn._partition_bounds(1437, 1000)
    assert (lo_i, hi_i) == (1000, 2000)


@pytest.mark.parametrize("flavor", get_flavors())
def test_chunk_interval_aliases_and_range(flavor: str):
    """
    `verify.chunk_minutes` is the authoritative partition width, with `chunk_hours`/`chunk_days`/
    etc. aliases and a `chunk_range` for integer axes. (`get_chunk_interval` is pure, so this runs
    on any flavor.)
    """
    conn = conns[flavor]
    if conn.type != 'sql':
        return

    ### Aliases resolve to the equivalent timedelta.
    days_pipe = mrsm.Pipe(
        'test', 'partition', 'alias_days',
        instance=conn, columns={'datetime': 'ts'},
        parameters={'verify': {'chunk_days': 7}},
    )
    assert days_pipe.get_chunk_interval() == timedelta(days=7)

    ### `chunk_minutes` wins when multiple aliases are present (priority order).
    multi_pipe = mrsm.Pipe(
        'test', 'partition', 'alias_multi',
        instance=conn, columns={'datetime': 'ts'},
        parameters={'verify': {'chunk_minutes': 60, 'chunk_days': 7}},
    )
    assert multi_pipe.get_chunk_interval() == timedelta(minutes=60)

    ### Integer axis: `chunk_range` is used verbatim; otherwise minutes are used verbatim (legacy).
    range_pipe = mrsm.Pipe(
        'test', 'partition', 'alias_range',
        instance=conn, columns={'datetime': 'ts'}, dtypes={'ts': 'int'},
        parameters={'verify': {'chunk_range': 1000, 'chunk_minutes': 5}},
    )
    assert range_pipe.get_chunk_interval() == 1000

    legacy_int = mrsm.Pipe(
        'test', 'partition', 'alias_legacy_int',
        instance=conn, columns={'datetime': 'ts'}, dtypes={'ts': 'int'},
        parameters={'verify': {'chunk_minutes': 500}},
    )
    assert legacy_int.get_chunk_interval() == 500


@pytest.mark.parametrize("flavor", get_flavors())
def test_repartition_roundtrip(flavor: str):
    """`Pipe.repartition()` changes the partition width while preserving the data."""
    conn = conns[flavor]
    if conn.type != 'sql' or conn.flavor not in PARTITIONABLE_FLAVORS:
        return

    data = _deterministic_data()
    pipe = mrsm.Pipe('test', 'partition', 'repartition', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'partition', 'repartition',
        instance=conn,
        columns=['datetime', 'id'],
        parameters={
            'hypertable': True,
            'verify': {'chunk_minutes': CHUNK_MINUTES},  # one-day partitions
        },
    )
    success, msg = pipe.sync(data, debug=debug)
    assert success, msg

    ### The width is the pipe's `verify.chunk_minutes`.
    assert pipe.get_chunk_interval(debug=debug) == timedelta(minutes=CHUNK_MINUTES)
    rowcount_before = pipe.get_rowcount(debug=debug)
    n_before = _count_partitions(conn, pipe)

    ### Repartition to two-day chunks: data preserved, `verify.chunk_minutes` updated, fewer
    ### partitions.
    new_minutes = CHUNK_MINUTES * 2
    success, msg = pipe.repartition(chunk_minutes=new_minutes, debug=debug)
    assert success, msg
    assert pipe.parameters.get('verify', {}).get('chunk_minutes', None) == new_minutes
    assert pipe.get_rowcount(debug=debug) == rowcount_before

    n_after = _count_partitions(conn, pipe)
    if n_before != -1 and n_after != -1:
        assert n_after < n_before, (
            f"Expected fewer partitions after widening ({n_before} -> {n_after})."
        )


@pytest.mark.parametrize("flavor", get_flavors())
def test_get_partition_info(flavor: str):
    """`get_partition_info` reports the partition count and physical width for a partitioned pipe."""
    conn = conns[flavor]
    if conn.type != 'sql' or conn.flavor not in PARTITIONABLE_FLAVORS:
        return

    data = _deterministic_data()
    pipe = mrsm.Pipe('test', 'partition', 'info', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'partition', 'info',
        instance=conn,
        columns=['datetime', 'id'],
        parameters={
            'hypertable': True,
            'verify': {'chunk_minutes': CHUNK_MINUTES},
        },
    )
    success, msg = pipe.sync(data, debug=debug)
    assert success, msg

    info = conn.get_partition_info(pipe, debug=debug)
    assert info['partitioned'] is True
    assert info['interval'] == timedelta(minutes=CHUNK_MINUTES)
    if info['count'] is not None:
        assert info['count'] > 1

    ### An unpartitioned pipe reports `partitioned=False`.
    plain = mrsm.Pipe('test', 'partition', 'info_plain', instance=conn)
    plain.delete()
    plain = mrsm.Pipe(
        'test', 'partition', 'info_plain',
        instance=conn,
        columns=['datetime', 'id'],
        parameters={'hypertable': False},
    )
    success, msg = plain.sync(data, debug=debug)
    assert success, msg
    assert conn.get_partition_info(plain, debug=debug)['partitioned'] is False


@pytest.mark.parametrize("flavor", get_flavors())
def test_repartition_unpartitioned_errors(flavor: str):
    """`Pipe.repartition()` fails clearly on a pipe that is not partitioned."""
    conn = conns[flavor]
    if conn.type != 'sql' or conn.flavor in PARTITIONABLE_FLAVORS:
        return
    ### Skip TimescaleDB, which supports `set_chunk_time_interval` instead.
    if conn.flavor in ('timescaledb', 'timescaledb-ha'):
        return

    pipe = mrsm.Pipe('test', 'partition', 'noop', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe('test', 'partition', 'noop', instance=conn, columns=['datetime', 'id'])
    success, _ = pipe.sync(_deterministic_data(), debug=debug)
    assert success
    repart_success, _ = pipe.repartition(debug=debug)
    assert not repart_success


@pytest.mark.parametrize("flavor", get_flavors())
def test_aligned_chunk_bounds_coincide_with_partitions(flavor: str):
    """`get_chunk_bounds(align=True)` interior edges land on partition boundaries."""
    conn = conns[flavor]
    if conn.type != 'sql':
        return

    pipe = mrsm.Pipe(
        'plugin:stress', 'test', 'partition_align',
        instance=conn,
        columns=['datetime', 'id'],
        parameters={
            'dtypes': {'datetime': 'datetime', 'id': 'int'},
            'verify': {'chunk_minutes': CHUNK_MINUTES},
        },
    )
    interval = timedelta(minutes=CHUNK_MINUTES)

    ### Start off the grid (12:00) to prove only the first edge is clamped to `begin`.
    begin = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
    bounds = pipe.get_chunk_bounds(
        begin=begin, end=END, bounded=True, chunk_interval=interval, align=True, debug=debug,
    )
    assert bounds, "Expected at least one chunk."

    ### First chunk starts exactly at `begin`.
    assert bounds[0][0] == begin

    ### Every interior boundary sits exactly on the epoch-aligned partition grid.
    interior_edges = [hi for (_, hi) in bounds[:-1]]
    for edge in interior_edges:
        part_lo, _ = conn._partition_bounds(edge, interval)
        assert part_lo == edge, f"Interior edge {edge} is not partition-aligned."

    ### Chunks are contiguous and cover the requested range.
    for (_, prev_hi), (next_lo, _) in zip(bounds, bounds[1:]):
        assert prev_hi == next_lo
    assert bounds[-1][1] == END
