#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test pipe deduplication syncs.
"""

import json
from datetime import datetime, timedelta

import pytest

import meerschaum as mrsm

from tests import debug
from tests.connectors import conns, get_flavors


@pytest.mark.parametrize("flavor", get_flavors())
def test_deduplicate_default(flavor: str):
    """
    Test that verification will fill any backtracked docs.
    """
    from meerschaum.utils.sql import sql_item_name
    from meerschaum.utils.dataframe import parse_df_datetimes
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    pipe = mrsm.Pipe('test', 'deduplicate', 'default_method', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'deduplicate', 'default_method',
        instance=conn,
        columns={
            'datetime': 'datetime',
            'id': 'id',
        }
    )
    _ = pipe.drop()

    docs = [
        {'datetime': '2023-01-01 00:00:00', 'id': 1},
        {'datetime': '2023-01-01 00:00:00', 'id': 1},
        {'datetime': '2023-01-02 00:00:00', 'id': 2},
        {'datetime': '2023-01-02 00:00:00', 'id': 2},
        {'datetime': '2023-01-02 00:00:00', 'id': 2},
        {'datetime': '2023-01-03 00:00:00', 'id': 3},
        {'datetime': '2023-01-03 00:00:00', 'id': 4},
    ]
    df = parse_df_datetimes(docs, strip_timezone=False)
    num_distinct = len(set([json.dumps(doc) for doc in docs]))

    conn.to_sql(df, pipe.target, debug=debug)
    assert pipe.get_rowcount() == len(docs)

    success, msg = pipe.deduplicate(debug=debug)
    assert success, msg
    assert pipe.get_rowcount() == num_distinct


@pytest.mark.parametrize("flavor", get_flavors())
def test_deduplicate_without_instance_method(flavor: str):
    """
    Test that verification will fill any backtracked docs.
    """
    from meerschaum.utils.dataframe import parse_df_datetimes
    conn = conns[flavor]
    if conn.type != 'sql':
        return
    pipe = mrsm.Pipe('test', 'deduplicate', 'chunked', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'test', 'deduplicate', 'chunked',
        instance=conn,
        columns={
            'datetime': 'datetime',
            'id': 'id',
        }
    )
    _ = pipe.drop()

    docs = [
        {'datetime': '2023-01-01 00:00:00', 'id': 1},
        {'datetime': '2023-01-01 00:00:00', 'id': 1},
        {'datetime': '2023-01-02 00:00:00', 'id': 2},
        {'datetime': '2023-01-02 00:00:00', 'id': 2},
        {'datetime': '2023-01-02 00:00:00', 'id': 2},
        {'datetime': '2023-01-03 00:00:00', 'id': 3},
        {'datetime': '2023-01-03 00:00:00', 'id': 4},
    ]
    df = parse_df_datetimes(docs)
    num_distinct = len(set([json.dumps(doc) for doc in docs]))

    conn.to_sql(df, pipe.target)
    assert pipe.get_rowcount() == len(docs)

    success, msg = pipe.deduplicate(debug=debug, _use_instance_method=False)
    assert success, msg
    assert pipe.get_rowcount() == num_distinct
