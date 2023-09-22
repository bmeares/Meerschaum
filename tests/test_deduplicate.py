#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test pipe deduplication syncs.
"""

import pytest
import json
from datetime import datetime, timedelta
from tests import debug
from tests.pipes import all_pipes, stress_pipes, remote_pipes
from tests.connectors import conns, get_flavors
from tests.test_users import test_register_user
import meerschaum as mrsm
from meerschaum import Pipe
from meerschaum.actions import actions

@pytest.mark.parametrize("flavor", get_flavors())
def test_deduplicate_simple(flavor: str):
    """
    Test that verification will fill any backtracked docs.
    """
    from meerschaum.utils.sql import sql_item_name
    conn = conns[flavor]
    pipe = mrsm.Pipe(
        'test', 'deduplicate', 'simple',
        instance = conn,
        columns = {
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
    num_distinct = len(set([json.dumps(doc) for doc in docs]))

    _ = pipe.sync(docs)
    assert pipe.get_rowcount() == len(docs)

    success, msg = pipe.deduplicate(debug=debug)
    assert success, msg
    assert pipe.get_rowcount() == num_distinct