#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Tests for reading data from a pipe.
"""

import pytest
from meerschaum import Pipe

from tests.connectors import conns, get_flavors
from tests import debug


@pytest.mark.parametrize("flavor", get_flavors())
def test_get_backtrack_data_limit(flavor: str):
    """
    Test reading backtrack data
    """
    conn = conns[flavor]
    pipe = Pipe('test', 'get_backtrack_data_limit', 'foo', instance=conn)
    _ = pipe.delete()
    pipe = Pipe(
        'test', 'get_backtrack_data_limit', 'foo',
        columns={'datetime': 'id'},
        dtypes={'id': 'int'},
        instance=conn,
    )
    success, msg = pipe.sync(
        [
            {'id': 1, 'color': 'red'},
            {'id': 2, 'color': 'green'},
            {'id': 3, 'color': 'blue'},
        ],
        check_existing=False,
        debug=debug,
    )
    assert success, msg

    limit = 1
    df = pipe.get_backtrack_data(
        limit=limit,
        debug=debug,
    )
    assert df['id'][0] == 3
    assert len(df) == limit


@pytest.mark.parametrize("flavor", get_flavors())
def test_get_data_order(flavor: str):
    """
    Test reading backtrack data
    """
    conn = conns[flavor]
    pipe = Pipe('test', 'get_data_order', 'foo', instance=conn)
    _ = pipe.delete()
    pipe = Pipe(
        'test', 'get_data_order', 'foo',
        columns={'datetime': 'id'},
        dtypes={'id': 'int'},
        instance=conn,
    )
    success, msg = pipe.sync(
        [
            {'id': 1, 'color': 'red'},
            {'id': 2, 'color': 'green'},
            {'id': 3, 'color': 'blue'},
        ],
        check_existing=False,
        debug=debug,
    )
    assert success, msg

    limit = 2
    df = pipe.get_data(
        limit=limit,
        order='asc',
        debug=debug,
    )
    assert len(df) == limit
    assert df['id'][0] == 1

    df = pipe.get_data(
        limit=limit,
        order='desc',
        debug=debug,
    )
    assert len(df) == limit
    assert df['id'][0] == 3


@pytest.mark.parametrize("flavor", get_flavors())
def test_get_docs(flavor: str):
    """Test that get_docs returns a list of dicts and as_docs=True works."""
    conn = conns[flavor]
    pipe = Pipe('test', 'get_docs', instance=conn)
    _ = pipe.delete()
    pipe = Pipe(
        'test', 'get_docs',
        columns={'datetime': 'dt', 'id': 'id'},
        instance=conn,
    )
    data = [
        {'dt': '2024-01-01', 'id': 1, 'val': 10.0},
        {'dt': '2024-01-02', 'id': 2, 'val': 20.0},
        {'dt': '2024-01-03', 'id': 3, 'val': 30.0},
    ]
    success, msg = pipe.sync(data, check_existing=False, debug=debug)
    assert success, msg

    docs = pipe.get_docs(debug=debug)
    assert isinstance(docs, list), f"Expected list, got {type(docs)}"
    assert len(docs) == 3
    assert isinstance(docs[0], dict)
    assert 'val' in docs[0]

    docs2 = pipe.get_data(as_docs=True, debug=debug)
    assert isinstance(docs2, list), f"Expected list from as_docs=True, got {type(docs2)}"
    assert len(docs2) == 3

    docs_empty = pipe.get_docs(begin='2099-01-01', debug=debug)
    assert docs_empty == []


@pytest.mark.parametrize("flavor", get_flavors())
def test_get_docs_as_iterator(flavor: str):
    """Test that as_docs=True with as_iterator=True yields lists of dicts."""
    conn = conns[flavor]
    pipe = Pipe('test', 'get_docs_iterator', instance=conn)
    _ = pipe.delete()
    pipe = Pipe(
        'test', 'get_docs_iterator',
        columns={'datetime': 'dt', 'id': 'id'},
        instance=conn,
    )
    data = [
        {'dt': '2024-01-01', 'id': 1, 'val': 10.0},
        {'dt': '2024-01-02', 'id': 2, 'val': 20.0},
    ]
    success, msg = pipe.sync(data, check_existing=False, debug=debug)
    assert success, msg

    import types
    chunks_gen = pipe.get_data(as_docs=True, as_iterator=True, debug=debug)
    assert isinstance(chunks_gen, types.GeneratorType)

    all_docs = []
    for chunk in chunks_gen:
        assert isinstance(chunk, list), f"Expected list chunk, got {type(chunk)}"
        for doc in chunk:
            assert isinstance(doc, dict)
        all_docs.extend(chunk)

    assert len(all_docs) == 2


def test_int_pipe_chunk_interval():
    """
    Test that integer pipes with precision correctly scale chunk and backtrack intervals.
    """
    import meerschaum as mrsm
    pipe = mrsm.Pipe(
        'demo', 'chunksize', 'int',
        instance='sql:memory',
        parameters={
            'columns': {'datetime': 'ts', 'id': 'id'},
            'dtypes': {'ts': 'int'},
            'precision': 'ms',
            'fetch': {'backtrack_minutes': 1440},
            'verify': {'chunk_minutes': 1440 * 30},
        },
    )
    assert pipe.get_chunk_interval() == 1440 * 30 * 60 * 1000
    assert pipe.get_backtrack_interval() == 1440 * 60 * 1000

    pipe.precision = 's'
    assert pipe.get_chunk_interval() == 1440 * 30 * 60
    assert pipe.get_backtrack_interval() == 1440 * 60

    simple_pipe = mrsm.Pipe(
        'demo', 'chunksize', 'int_no_precision',
        instance='sql:memory',
        parameters={
            'columns': {'datetime': 'id'},
            'dtypes': {'id': 'int'},
            'precision': None,
            'fetch': {'backtrack_minutes': 100},
            'verify': {'chunk_minutes': 10000},
        },
    )
    assert simple_pipe.get_chunk_interval() == 10000
    assert simple_pipe.get_backtrack_interval() == 100


def test_parse_date_bounds_epoch_int_axis():
    """
    Test that datetime bounds are translated to epoch integers for int-axis pipes
    with an explicit `precision`.
    """
    import meerschaum as mrsm
    from datetime import datetime, timezone
    from meerschaum.utils.dtypes import datetime_to_int
    pipe = mrsm.Pipe(
        'demo', 'bounds', 'epoch',
        instance='sql:memory',
        parameters={
            'columns': {'datetime': 'ts'},
            'dtypes': {'ts': 'int'},
            'precision': 'ms',
        },
    )
    expected = datetime_to_int(datetime(2026, 5, 30, tzinfo=timezone.utc), 'ms')

    ### A datetime string is translated to an epoch integer.
    assert pipe.parse_date_bounds('2026-05-30') == expected

    ### A `datetime` object is likewise translated.
    assert pipe.parse_date_bounds(datetime(2026, 5, 30, tzinfo=timezone.utc)) == expected

    ### Raw integers and integer strings pass through unchanged.
    assert pipe.parse_date_bounds(5) == 5
    assert pipe.parse_date_bounds(str(expected)) == expected

    ### An empty string and `None` are preserved.
    assert pipe.parse_date_bounds('') == ''
    assert pipe.parse_date_bounds(None) is None

    ### Both bounds are translated together.
    begin, end = pipe.parse_date_bounds('2026-05-30', '2026-05-31')
    assert begin == expected
    assert end == datetime_to_int(datetime(2026, 5, 31, tzinfo=timezone.utc), 'ms')


def test_parse_date_bounds_non_epoch_int_axis_raises():
    """
    Test that a datetime bound on a non-epoch int axis (no explicit `precision`)
    raises, while integer bounds still pass through.
    """
    import meerschaum as mrsm
    pipe = mrsm.Pipe(
        'demo', 'bounds', 'arbitrary',
        instance='sql:memory',
        parameters={
            'columns': {'datetime': 'n'},
            'dtypes': {'n': 'int'},
        },
    )
    ### Integer bounds are valid for an arbitrary integer axis.
    assert pipe.parse_date_bounds(5) == 5
    assert pipe.parse_date_bounds('5') == 5

    ### A datetime bound is nonsensical here and must raise.
    with pytest.raises(Exception):
        pipe.parse_date_bounds('2026-05-30')


def test_parse_date_bounds_datetime_axis_unchanged():
    """
    Test that datetime-axis pipes still return `datetime` bounds (no regression).
    """
    import meerschaum as mrsm
    from datetime import datetime
    pipe = mrsm.Pipe(
        'demo', 'bounds', 'dt',
        instance='sql:memory',
        parameters={
            'columns': {'datetime': 'ts'},
            'dtypes': {'ts': 'datetime'},
        },
    )
    begin = pipe.parse_date_bounds('2026-05-30')
    assert isinstance(begin, datetime)


@pytest.mark.parametrize("flavor", get_flavors())
def test_epoch_int_axis_datetime_begin(flavor: str):
    """
    Test that actions can filter an epoch int-axis pipe with a datetime `--begin`.
    """
    from datetime import datetime, timezone
    from meerschaum.utils.dtypes import datetime_to_int
    conn = conns[flavor]
    pipe = Pipe(
        'demo', 'epoch_begin', 'foo',
        instance=conn,
        parameters={
            'columns': {'datetime': 'ts', 'id': 'id'},
            'dtypes': {'ts': 'int', 'id': 'int'},
            'precision': 'ms',
        },
    )
    pipe.delete()
    pipe = Pipe(
        'demo', 'epoch_begin', 'foo',
        instance=conn,
        parameters={
            'columns': {'datetime': 'ts', 'id': 'id'},
            'dtypes': {'ts': 'int', 'id': 'int'},
            'precision': 'ms',
        },
    )

    def epoch(day: int) -> int:
        return datetime_to_int(datetime(2026, 5, day, tzinfo=timezone.utc), 'ms')

    docs = [{'ts': epoch(day), 'id': day} for day in (28, 29, 30, 31)]
    success, msg = pipe.sync(docs, debug=debug)
    assert success, msg

    ### `get_data` with a datetime begin filters by the translated epoch.
    filtered = pipe.get_data(begin='2026-05-30', debug=debug)
    assert sorted(filtered['id'].tolist()) == [30, 31]

    ### `clear` with a datetime end removes the earlier rows.
    success, msg = pipe.clear(end='2026-05-30', debug=debug)
    assert success, msg
    remaining = pipe.get_data(debug=debug)
    assert sorted(remaining['id'].tolist()) == [30, 31]

    pipe.delete()
