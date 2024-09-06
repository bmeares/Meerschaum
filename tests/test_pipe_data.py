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
