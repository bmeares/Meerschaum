#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Sync correctness (5.0.0rc15): in-place rollups re-sync without duplicates (SQLite typed tables),
NULL keys upsert in place, null-like strings stay text on declared text and index columns, and
`replace=True` / `fetch: {replace: true}` replace a window in one transaction.
"""

from datetime import datetime, timedelta

import pytest
import meerschaum as mrsm

from tests import debug
from tests.connectors import conns, get_flavors

SQL_FLAVORS = [flavor for flavor in get_flavors() if flavor in conns and conns[flavor].type == 'sql']
INSTANCE_FLAVORS = [flavor for flavor in get_flavors() if flavor in conns and flavor != 'api']

DAY_EXPR = {
    'sqlite': 'date(ts)',
    'geopackage': 'date(ts)',
    'mysql': 'DATE(ts)',
    'mariadb': 'DATE(ts)',
    'oracle': 'TRUNC(ts)',
}


def _source(conn, location: str):
    src = mrsm.Pipe(
        'test', 'rc15_f1_temps', location,
        instance=conn,
        columns={'datetime': 'ts', 'id': 'station'},
    )
    src.delete(debug=debug)
    src = mrsm.Pipe(
        'test', 'rc15_f1_temps', location,
        instance=conn,
        columns={'datetime': 'ts', 'id': 'station'},
    )
    docs = []
    start = datetime(2024, 1, 1)
    for day in range(4):
        for hour in range(0, 24, 6):
            for station in ('a', 'b'):
                docs.append({
                    'ts': start + timedelta(days=day, hours=hour),
                    'station': station,
                    'temperature': float(day * 10 + hour),
                })
    success, msg = src.sync(docs, debug=debug)
    assert success, msg
    return src


def _inplace_pipe(conn, metric: str, location: str, definition: str, **parameters):
    keys = (str(conn), metric, location)
    pipe = mrsm.Pipe(*keys, instance=conn)
    pipe.delete(debug=debug)
    return mrsm.Pipe(
        *keys,
        instance=conn,
        columns={'datetime': 'day', 'id': 'station'},
        parameters={'fetch': {'definition': definition, **parameters.pop('fetch', {})}},
        **parameters,
    )


@pytest.mark.parametrize("flavor", SQL_FLAVORS)
def test_inplace_rollups_resync_without_duplicates(flavor: str):
    """
    B1: a rollup whose datetime is computed (`date(ts)`) keeps its 8 keys over re-syncs, and so
    does a chained child of it. On SQLite the computed columns are typed.
    """
    conn = conns[flavor]
    src = _source(conn, 'rollup')
    day = DAY_EXPR.get(flavor, 'CAST(ts AS DATE)')
    rollup = _inplace_pipe(
        conn, 'rc15_f1_rollup', 'r',
        f"SELECT {day} AS day, station, AVG(temperature) AS avg_t "
        f"FROM {{{{ Pipe('test', 'rc15_f1_temps', 'rollup', instance='{conn}') }}}} "
        f"GROUP BY {day}, station"
    )
    for attempt in range(3):
        success, msg = rollup.sync(debug=debug)
        assert success, msg
        assert rollup.get_rowcount() == 8, f"sync {attempt}: {msg}"

    child = _inplace_pipe(
        conn, 'rc15_f1_rollup', 'child',
        f"SELECT day, station, avg_t * 2 AS double_t "
        f"FROM {{{{ Pipe('{conn}', 'rc15_f1_rollup', 'r', instance='{conn}') }}}}"
    )
    for attempt in range(3):
        success, msg = child.sync(debug=debug)
        assert success, msg
        assert child.get_rowcount() == 8, f"child sync {attempt}: {msg}"

    if flavor in ('sqlite', 'geopackage'):
        types = rollup.get_columns_types(refresh=True)
        assert all(typ and typ.upper() != 'NULL' for typ in types.values()), types
        df = rollup.get_data()
        assert str(df['day'].dtype).startswith('datetime64'), df.dtypes
        assert any(t in str(df['avg_t'].dtype).lower() for t in ('float', 'double')), df.dtypes

    for pipe in (child, rollup, src):
        pipe.delete(debug=debug)


@pytest.mark.parametrize("flavor", INSTANCE_FLAVORS)
def test_upserts_with_null_keys_update_in_place(flavor: str):
    """
    S3: with the default `null_indices: true`, a row whose ID is NULL upserts in place.
    """
    conn = conns[flavor]
    kwargs = dict(instance=conn, columns={'datetime': 'ts', 'id': 'id'}, upsert=True)
    pipe = mrsm.Pipe('test', 'rc15_f1_upsert', 'null_keys', **kwargs)
    pipe.delete(debug=debug)
    pipe = mrsm.Pipe('test', 'rc15_f1_upsert', 'null_keys', **kwargs)
    ts = datetime(2024, 1, 1)
    for values in ((1, 2), (10, 20), (10, 20)):
        success, msg = pipe.sync([
            {'ts': ts, 'id': 1, 'v': values[0]},
            {'ts': ts, 'id': None, 'v': values[1]},
        ], debug=debug)
        assert success, msg
    df = pipe.get_data()
    assert len(df) == 2, df
    assert sorted(df['v'].tolist()) == [10, 20]
    pipe.delete(debug=debug)


@pytest.mark.parametrize("flavor", INSTANCE_FLAVORS)
def test_null_like_strings_are_text_on_declared_and_index_columns(flavor: str):
    """
    Q18: 'None', 'NA', 'nan', '' ... are values in a declared `string` column and an index column;
    `params={'col': 'NA'}` matches the text, Python `None` matches nulls.
    """
    conn = conns[flavor]
    markers = ['None', 'NA', 'nan', 'NaT', '<NA>', 'x']
    kwargs = dict(
        instance=conn,
        columns={'id': 'code'},
        dtypes={'code': 'string', 'label': 'string'},
    )
    pipe = mrsm.Pipe('test', 'rc15_f1_markers', **kwargs)
    pipe.delete(debug=debug)
    pipe = mrsm.Pipe('test', 'rc15_f1_markers', **kwargs)
    ### On Valkey a NULL index value and its text ('None', 'nan', '<NA>') share a document key
    ### (the stored key format), so the NULL row has its own code there.
    null_code = None if conn.type != 'valkey' else 'null-row'
    docs = [
        {'code': marker, 'label': marker, 'n': i}
        for i, marker in enumerate(markers)
    ] + [{'code': null_code, 'label': None, 'n': len(markers)}]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    df = pipe.get_data()
    assert len(df) == len(markers) + 1
    codes = sorted(c for c in df['code'].tolist() if isinstance(c, str) and c != 'null-row')
    assert codes == sorted(markers), df
    labels = sorted(c for c in df['label'].tolist() if isinstance(c, str))
    assert labels == sorted(markers), df

    for marker in markers:
        matched = pipe.get_data(params={'code': marker})
        assert len(matched) == 1, (marker, matched)
        assert matched['code'].iloc[0] == marker
    if conn.type != 'valkey':
        assert len(pipe.get_data(params={'code': None})) == 1
        assert len(pipe.get_data(params={'code': '_None'})) == len(markers)
    assert pipe.get_rowcount(params={'label': 'NA'}) == 1

    ### A second sync of the same rows changes nothing.
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    assert pipe.get_rowcount() == len(markers) + 1
    pipe.delete(debug=debug)


@pytest.mark.parametrize("flavor", INSTANCE_FLAVORS)
def test_replace_windows(flavor: str):
    """
    Q19: `pipe.sync(df, begin=..., end=..., replace=True)` replaces the rows of `[begin, end)`.
    """
    conn = conns[flavor]
    kwargs = dict(instance=conn, columns={'datetime': 'ts', 'id': 'id'})
    pipe = mrsm.Pipe('test', 'rc15_f1_replace', **kwargs)
    pipe.delete(debug=debug)
    pipe = mrsm.Pipe('test', 'rc15_f1_replace', **kwargs)
    day = lambda d: datetime(2024, 1, d)
    success, msg = pipe.sync(
        [{'ts': day(d), 'id': 1, 'v': d} for d in range(1, 6)],
        debug=debug,
    )
    assert success, msg
    assert pipe.get_rowcount() == 5

    ### Days 2 to 3 are replaced by one row; the row of day 9 is outside the window.
    result = pipe.sync(
        [{'ts': day(2), 'id': 2, 'v': 20}, {'ts': day(9), 'id': 1, 'v': 9}],
        begin=day(2),
        end=day(4),
        replace=True,
        debug=debug,
    )
    assert result.success, result.message
    assert result.message.startswith('Replaced 2 rows with 1 row.'), result.message
    assert 'Skipped 1 row outside the window.' in result.message
    assert result.deleted == 2 and result.inserted == 1, result.to_dict()
    df = pipe.get_data()
    assert sorted(df['v'].tolist()) == [1, 4, 5, 20], df

    ### Without bounds, the window is the data's own datetime range.
    result = pipe.sync([{'ts': day(4), 'id': 1, 'v': 40}], replace=True, debug=debug)
    assert result.success, result.message
    assert sorted(pipe.get_data()['v'].tolist()) == [1, 5, 20, 40]

    ### An empty frame clears the window; without bounds it is refused.
    result = pipe.sync([], begin=day(1), end=day(3), replace=True, debug=debug)
    assert result.success, result.message
    assert sorted(pipe.get_data()['v'].tolist()) == [5, 40]
    result = pipe.sync([], replace=True, debug=debug)
    assert not result.success
    pipe.delete(debug=debug)


@pytest.mark.parametrize("flavor", SQL_FLAVORS)
def test_replace_failure_keeps_the_old_rows(flavor: str):
    """
    Q19: the window's rows are deleted and the new rows written in one transaction: a write
    that fails leaves the old rows.
    """
    conn = conns[flavor]
    kwargs = dict(instance=conn, columns={'datetime': 'ts', 'primary': 'pk'})
    pipe = mrsm.Pipe('test', 'rc15_f1_replace_crash', **kwargs)
    pipe.delete(debug=debug)
    pipe = mrsm.Pipe('test', 'rc15_f1_replace_crash', **kwargs)
    day = lambda d: datetime(2024, 1, d)
    success, msg = pipe.sync(
        [{'ts': day(d), 'pk': d, 'v': d} for d in range(1, 4)],
        debug=debug,
    )
    assert success, msg
    ### Two rows with the same key (a partitioned table's primary key includes the datetime):
    ### the insert fails after the delete ran.
    result = pipe.sync(
        [{'ts': day(1), 'pk': 7, 'v': 70}, {'ts': day(1), 'pk': 7, 'v': 71}],
        begin=day(1),
        end=day(4),
        replace=True,
        debug=debug,
    )
    assert not result.success, result.message
    df = pipe.get_data()
    assert sorted(df['v'].tolist()) == [1, 2, 3], df
    pipe.delete(debug=debug)


@pytest.mark.parametrize("flavor", SQL_FLAVORS)
def test_fetch_replace_makes_fetched_windows_authoritative(flavor: str):
    """
    Q19: with `fetch: {replace: true}`, rows that leave the source's window leave the pipe.
    """
    conn = conns[flavor]
    src = _source(conn, 'replace')
    day = DAY_EXPR.get(flavor, 'CAST(ts AS DATE)')
    rollup = _inplace_pipe(
        conn, 'rc15_f1_rollup', 'replace',
        f"SELECT {day} AS day, station, AVG(temperature) AS avg_t "
        f"FROM {{{{ Pipe('test', 'rc15_f1_temps', 'replace', instance='{conn}') }}}} "
        f"GROUP BY {day}, station",
        fetch={'replace': True},
    )
    success, msg = rollup.sync(debug=debug)
    assert success, msg
    assert rollup.get_rowcount() == 8
    success, msg = src.clear(begin=datetime(2024, 1, 4), params={'station': 'b'}, debug=debug)
    assert success, msg
    success, msg = rollup.sync(debug=debug)
    assert success, msg
    assert msg.startswith('Replaced '), msg
    assert rollup.get_rowcount() == 7
    rollup.delete(debug=debug)
    src.delete(debug=debug)


@pytest.mark.parametrize("flavor", INSTANCE_FLAVORS)
def test_replace_without_datetime_replaces_the_table(flavor: str):
    """
    Q19: a pipe without a datetime column has one window, the whole table.
    """
    conn = conns[flavor]
    kwargs = dict(instance=conn, columns={'id': 'id'})
    pipe = mrsm.Pipe('test', 'rc15_f1_replace_nodt', **kwargs)
    pipe.delete(debug=debug)
    pipe = mrsm.Pipe('test', 'rc15_f1_replace_nodt', **kwargs)
    success, msg = pipe.sync([{'id': i, 'v': i} for i in range(3)], debug=debug)
    assert success, msg
    result = pipe.sync([{'id': 9, 'v': 9}], replace=True, debug=debug)
    assert result.success, result.message
    assert pipe.get_data()['id'].tolist() == [9]
    result = pipe.sync([{'id': 1, 'v': 1}], begin=1, end=2, replace=True, debug=debug)
    assert not result.success
    pipe.delete(debug=debug)


@pytest.mark.parametrize("flavor", INSTANCE_FLAVORS)
def test_connector_sync_pipe_replace_takes_the_rows_window(flavor: str):
    """
    Q19: `instance.sync_pipe(pipe, df, replace=True)` replaces the window of the rows it is
    given (a bound that is not given comes from the rows), never the whole table, and honors
    `begin`, `end` and `params`.
    """
    conn = conns[flavor]
    kwargs = dict(instance=conn, columns={'datetime': 'ts', 'id': 'id'})
    pipe = mrsm.Pipe('test', 'rc15_f1_conn_replace', **kwargs)
    pipe.delete(debug=debug)
    pipe = mrsm.Pipe('test', 'rc15_f1_conn_replace', **kwargs)
    day = lambda d: datetime(2024, 1, d)
    success, msg = pipe.sync([{'ts': day(d), 'id': 1, 'v': d} for d in range(1, 6)], debug=debug)
    assert success, msg

    success, msg = conn.sync_pipe(pipe, [{'ts': day(3), 'id': 1, 'v': 30}], replace=True)
    assert success, msg
    assert msg.startswith('Replaced 1 row with 1 row.'), msg
    assert sorted(pipe.get_data()['v'].tolist()) == [1, 2, 4, 5, 30]

    success, msg = conn.sync_pipe(
        pipe,
        [{'ts': day(4), 'id': 2, 'v': 42}],
        begin=day(4),
        end=day(6),
        replace=True,
        params={'id': 2},
    )
    assert success, msg
    ### Only id 2's rows of [4, 6) were replaced: days 4 and 5 of id 1 stay.
    assert sorted(pipe.get_data()['v'].tolist()) == [1, 2, 4, 5, 30, 42]
    pipe.delete(debug=debug)


@pytest.mark.parametrize("flavor", INSTANCE_FLAVORS)
def test_first_replace_with_nothing_to_write_succeeds(flavor: str):
    """
    Q19: the first replacing sync of a pipe with no rows in its window succeeds.
    """
    conn = conns[flavor]
    kwargs = dict(instance=conn, columns={'datetime': 'ts', 'id': 'id'})
    pipe = mrsm.Pipe('test', 'rc15_f1_replace_first', **kwargs)
    pipe.delete(debug=debug)
    pipe = mrsm.Pipe('test', 'rc15_f1_replace_first', **kwargs)
    result = pipe.sync(
        [],
        begin=datetime(2024, 1, 1),
        end=datetime(2024, 1, 2),
        replace=True,
        debug=debug,
    )
    assert result.success, result.message
    pipe.delete(debug=debug)


@pytest.mark.parametrize(
    "flavor",
    [flavor for flavor in SQL_FLAVORS if flavor in ('mysql', 'mariadb', 'sqlite')],
)
def test_repeated_keys_in_one_upsert_keep_the_last_row(flavor: str):
    """
    S3: a batch that repeats a key (a NULL key included) keeps the last row of each key, as
    `INSERT ... ON DUPLICATE KEY UPDATE` resolves it.
    """
    conn = conns[flavor]
    kwargs = dict(instance=conn, columns={'datetime': 'ts', 'id': 'id'}, upsert=True)
    pipe = mrsm.Pipe('test', 'rc15_f1_upsert_repeats', **kwargs)
    pipe.delete(debug=debug)
    pipe = mrsm.Pipe('test', 'rc15_f1_upsert_repeats', **kwargs)
    ts = datetime(2024, 1, 1)
    success, msg = pipe.sync([{'ts': ts, 'id': 1, 'v': 1}], debug=debug)
    assert success, msg
    success, msg = pipe.sync(
        [
            {'ts': ts, 'id': 1, 'v': 2},
            {'ts': ts, 'id': 1, 'v': 3},
            {'ts': ts, 'id': 7, 'v': 70},
            {'ts': ts, 'id': 7, 'v': 71},
            {'ts': ts, 'id': None, 'v': 8},
            {'ts': ts, 'id': None, 'v': 9},
        ],
        debug=debug,
    )
    assert success, msg
    assert sorted(pipe.get_data()['v'].tolist()) == [3, 9, 71]
    pipe.delete(debug=debug)


@pytest.mark.parametrize("flavor", INSTANCE_FLAVORS)
def test_empty_strings_resync_idempotently(flavor: str):
    """
    Q18: `''` in a text index column and a declared text column re-syncs without changes. On
    Oracle, which stores `''` as `NULL`, `''` is a null there.
    """
    conn = conns[flavor]
    kwargs = dict(
        instance=conn,
        columns={'datetime': 'ts', 'id': 'code'},
        dtypes={'label': 'string'},
    )
    pipe = mrsm.Pipe('test', 'rc15_f1_empty_strings', **kwargs)
    pipe.delete(debug=debug)
    pipe = mrsm.Pipe('test', 'rc15_f1_empty_strings', **kwargs)
    ts = datetime(2024, 1, 1)
    docs = [
        {'ts': ts, 'code': '', 'label': '', 'v': 0},
        {'ts': ts, 'code': 'a', 'label': 'x', 'v': 1},
    ]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg
    for _ in range(2):
        result = pipe.sync(docs, debug=debug)
        assert result.success, result.message
        assert result.inserted == 0 and result.updated == 0, result.message
    assert pipe.get_rowcount() == 2
    assert pipe.get_rowcount(params={'code': ''}) == 1
    pipe.delete(debug=debug)
