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
