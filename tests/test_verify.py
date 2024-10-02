#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test pipe verification syncs.
"""

import pytest
from datetime import datetime, timedelta
from tests import debug
from tests.pipes import all_pipes, stress_pipes, remote_pipes
from tests.connectors import conns, get_flavors
from tests.test_users import test_register_user
import meerschaum as mrsm
from meerschaum import Pipe
from meerschaum.actions import actions


@pytest.mark.parametrize("flavor", get_flavors())
def test_verify_backfill_simple(flavor: str):
    """
    Test that simple verification syncs will fill gaps.
    """
    conn = conns[flavor]
    pipe = stress_pipes[flavor]
    _ = pipe.drop()

    begin = datetime(2023, 1, 1)
    end = datetime(2023, 1, 2)
    _ = pipe.sync(begin=begin, end=end)

    clear_begin = datetime(2023, 1, 1, 12, 0)
    clear_end = datetime(2023, 1, 1, 14, 0)
    existing_df_in_clear_interval = pipe.get_data(begin=clear_begin, end=clear_end)

    _ = pipe.clear(begin=clear_begin, end=clear_end)
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
    source_pipe = stress_pipes[flavor]
    source_table_name = sql_item_name(source_pipe.target, source_pipe.instance_connector.flavor)
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
    _ = target_pipe.drop()
    _ = source_pipe.drop()

    begin = datetime(2023, 1, 1)
    end = datetime(2023, 1, 2)
    _ = source_pipe.sync(begin=begin, end=end)
    _ = target_pipe.sync()

    source_rowcount = source_pipe.get_rowcount()
    target_rowcount = target_pipe.get_rowcount()
    assert source_rowcount == target_rowcount

    backfill_begin = datetime(2022, 12, 24)
    backfill_end = datetime(2022, 12, 26)
    _ = source_pipe.sync(begin=backfill_begin, end=backfill_end)
    success, msg = target_pipe.verify(debug=debug)
    new_source_rowcount = source_pipe.get_rowcount()
    new_target_rowcount = target_pipe.get_rowcount()
    assert new_source_rowcount == new_target_rowcount
