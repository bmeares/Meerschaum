#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test pipe verification syncs.
"""

import pytest
from datetime import datetime, timedelta
from tests import debug
from tests.connectors import conns, get_flavors
import meerschaum as mrsm
from meerschaum import Pipe
from meerschaum.actions import actions


@pytest.mark.parametrize("flavor", get_flavors())
def test_verify_backfill_simple(flavor: str):
    """
    Test that simple verification syncs will fill gaps.
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('plugin:stress', 'test', 'verify_backfill_simple', instance=conn)
    pipe.delete()
    pipe = mrsm.Pipe(
        'plugin:stress', 'test', 'verify_backfill_simple',
        instance=conn,
        columns=['datetime', 'id'],
        parameters={
            'fetch': {
                'rows': 100,
                'id': 3,
            },
        },
    )

    begin = datetime(2023, 1, 1)
    end = datetime(2023, 1, 2)
    _ = pipe.sync(begin=begin, end=end, debug=debug)

    clear_begin = datetime(2023, 1, 1, 12, 0)
    clear_end = datetime(2023, 1, 1, 14, 0)
    existing_df_in_clear_interval = pipe.get_data(begin=clear_begin, end=clear_end, debug=debug)

    _ = pipe.clear(begin=clear_begin, end=clear_end, debug=debug)
    assert pipe.get_rowcount(begin=clear_begin, end=clear_end) == 0

    success, msg = pipe.verify(debug=debug)
    new_df_in_clear_interval = pipe.get_data(begin=clear_begin, end=clear_end)

    assert len(existing_df_in_clear_interval) == len(new_df_in_clear_interval)
    assert (
        existing_df_in_clear_interval['datetime'].min()
        ==
        new_df_in_clear_interval['datetime'].min()
    )
    assert (
        existing_df_in_clear_interval['datetime'].max()
        ==
        new_df_in_clear_interval['datetime'].max()
    )


@pytest.mark.parametrize("flavor", get_flavors())
def test_verify_backfill_inplace(flavor: str):
    """
    Test that verification will fill any backtracked docs when syncing with an inplace SQLConnector.
    """
    from meerschaum.utils.sql import sql_item_name
    conn = conns[flavor]
    if not hasattr(conn, 'sync_pipe_inplace'):
        return
    source_pipe = mrsm.Pipe('plugin:stress', 'test', 'verify_backfill_inplace', instance=conn)
    source_pipe.delete()
    source_pipe = mrsm.Pipe(
        'plugin:stress', 'test', 'verify_backfill_inplace',
        instance=conn,
        columns=['datetime', 'id'],
        parameters={
            'fetch': {
                'rows': 100,
                'id': 3,
            },
        },
    )
    source_table_name = sql_item_name(source_pipe.target, source_pipe.instance_connector.flavor)
    target_pipe = mrsm.Pipe(source_pipe.instance_connector, 'test_verify', 'backfill', instance=conn)
    target_pipe.delete()
    target_pipe = Pipe(
        source_pipe.instance_connector, 'test_verify', 'backfill',
        instance=conn,
        columns={
            'datetime': 'datetime',
            'id': 'id',
        },
        parameters={
            'fetch': {
                'definition': f"SELECT * FROM {source_table_name}",
                'pipe': source_pipe.keys(),
            }
        },
    )

    begin = datetime(2023, 1, 1)
    end = datetime(2023, 1, 2)
    success, msg = source_pipe.sync(begin=begin, end=end, debug=debug)
    assert success, msg
    success, msg = target_pipe.sync(debug=debug)
    assert success, msg

    source_rowcount = source_pipe.get_rowcount(debug=debug)
    target_rowcount = target_pipe.get_rowcount(debug=debug)
    assert source_rowcount == target_rowcount

    backfill_begin = datetime(2022, 12, 24)
    backfill_end = datetime(2022, 12, 26)

    success, msg = source_pipe.sync(begin=backfill_begin, end=backfill_end, debug=debug)
    assert success, msg

    success, msg = target_pipe.verify(debug=debug)
    assert success, msg

    new_source_rowcount = source_pipe.get_rowcount(debug=debug)
    new_target_rowcount = target_pipe.get_rowcount(debug=debug)
    assert new_source_rowcount == new_target_rowcount
