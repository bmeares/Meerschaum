"""
Wave 18 (PF): the table layout a native sync ends with is the pipe's cached
`get_columns_types()` (no second reflection on the next read), and the columns of the next
write show up in it. A fresh `Pipe` reads its metadata and sync time correctly.
"""

import uuid
from datetime import datetime, timezone

import pytest

import meerschaum as mrsm


@pytest.fixture
def conn(tmp_path):
    return mrsm.get_connector(
        'sql', 'pfperf_' + uuid.uuid4().hex,
        flavor='sqlite', database=str(tmp_path / 'pfperf.db'),
    )


def _rows(*days, **extra):
    return [
        {'dt': datetime(2024, 1, day, tzinfo=timezone.utc), 'id': day, 'val': day * 10, **extra}
        for day in days
    ]


def _pipe(conn, metric):
    return mrsm.Pipe(
        'pfperf', metric, instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
    )


class _Reflections:
    """Counts the table reflections the instance performs."""

    def __init__(self, conn):
        self.count = 0
        self._conn = conn
        self._original = conn.get_pipe_columns_types

    def __enter__(self):
        def counted(*args, **kwargs):
            self.count += 1
            return self._original(*args, **kwargs)
        self._conn.get_pipe_columns_types = counted
        return self

    def __exit__(self, *exc):
        self._conn.__dict__.pop('get_pipe_columns_types', None)


def test_sync_into_an_existing_table_keeps_its_layout_for_the_next_read(conn):
    pipe = _pipe(conn, 'keeps_layout')
    assert pipe.sync(_rows(1, 2))[0]

    ### The sync that creates the table hands nothing out: the next read reflects it.
    with _Reflections(conn) as reflections:
        assert 'val' in pipe.get_columns_types()
        assert reflections.count == 1

    assert pipe.sync(_rows(3))[0]
    with _Reflections(conn) as reflections:
        types = pipe.get_columns_types()
        docs = pipe.get_data(order='asc', as_docs=True)
        assert pipe.get_sync_time() == datetime(2024, 1, 3, tzinfo=timezone.utc)
        assert reflections.count == 0, "the layout the sync ended with was not adopted"
    assert set(types) == {'dt', 'id', 'val'}
    assert [row['val'] for row in docs] == [10, 20, 30]
    pipe.delete()


def test_a_column_added_by_a_sync_is_in_the_adopted_layout(conn):
    pipe = _pipe(conn, 'adds_column')
    assert pipe.sync(_rows(1, 2))[0]
    assert pipe.sync(_rows(3, extra='x'))[0]
    with _Reflections(conn) as reflections:
        types = pipe.get_columns_types()
        docs = pipe.get_data(order='asc', as_docs=True)
        assert reflections.count == 0
    assert 'extra' in types, types
    assert docs[-1]['extra'] == 'x' and docs[0]['extra'] is None
    ### What was adopted is what the database reports.
    assert types == conn.get_pipe_columns_types(pipe)
    pipe.delete()


def test_an_adopted_layout_is_dropped_by_a_hard_invalidation(conn):
    pipe = _pipe(conn, 'invalidated')
    assert pipe.sync(_rows(1))[0]
    assert pipe.sync(_rows(2))[0]
    assert pipe.__dict__.get('_columns_types')
    pipe._invalidate_cache(hard=True)
    with _Reflections(conn) as reflections:
        assert 'val' in pipe.get_columns_types()
        assert reflections.count == 1
    pipe.delete()


def test_a_fresh_pipe_reads_its_registered_metadata_and_sync_time(conn):
    pipe = _pipe(conn, 'fresh')
    assert pipe.sync(_rows(1, 2, 3))[0]
    for _ in range(3):
        fresh = mrsm.Pipe('pfperf', 'fresh', instance=conn)
        assert fresh.columns == {'datetime': 'dt', 'id': 'id'}
        assert fresh.get_sync_time() == datetime(2024, 1, 3, tzinfo=timezone.utc)
        assert fresh.get_rowcount() == 3
    unregistered = mrsm.Pipe('pfperf', 'never_registered', instance=conn)
    assert unregistered.get_sync_time() is None
    assert unregistered.get_rowcount() == 0
    pipe.delete()
