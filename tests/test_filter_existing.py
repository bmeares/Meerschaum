#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import pytest
from datetime import datetime, timezone

import meerschaum as mrsm

from tests import debug
from tests.connectors import conns, get_flavors


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_none_df(flavor: str):
    """filter_existing(None) returns three empty DataFrames with correct columns."""
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'none_df', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'none_df', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.sync([{'dt': datetime(2021, 1, 1), 'id': 1, 'val': 10}], debug=debug)

    unseen, update, delta = pipe.filter_existing(None, debug=debug)

    assert unseen is not None
    assert update is not None
    assert delta is not None
    assert len(unseen) == 0
    assert len(update) == 0
    assert len(delta) == 0


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_empty_df(flavor: str):
    """filter_existing of an empty DataFrame returns three references to the same empty df."""
    pd = mrsm.attempt_import('pandas')
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'empty_df', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'empty_df', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.sync([{'dt': datetime(2021, 1, 1), 'id': 1, 'val': 10}], debug=debug)

    empty = pd.DataFrame([])
    unseen, update, delta = pipe.filter_existing(empty, debug=debug)

    assert len(unseen) == 0
    assert len(update) == 0
    assert len(delta) == 0


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_all_new(flavor: str):
    """All rows are new → unseen contains all, update is empty, delta equals unseen."""
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'all_new', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'all_new', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})

    existing = [
        {'dt': datetime(2021, 1, 1), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2), 'id': 2, 'val': 20},
    ]
    pipe.sync(existing, debug=debug)

    new_rows = [
        {'dt': datetime(2021, 1, 3), 'id': 3, 'val': 30},
        {'dt': datetime(2021, 1, 4), 'id': 4, 'val': 40},
    ]
    pd = mrsm.attempt_import('pandas')
    new_df = pd.DataFrame(new_rows)

    unseen, update, delta = pipe.filter_existing(new_df, debug=debug)

    assert len(unseen) == 2
    assert len(update) == 0
    assert len(delta) == 2


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_all_duplicate(flavor: str):
    """Syncing the same data again → all three DataFrames are empty."""
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'all_dup', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'all_dup', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})

    rows = [
        {'dt': datetime(2021, 1, 1), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2), 'id': 2, 'val': 20},
    ]
    pipe.sync(rows, debug=debug)

    pd = mrsm.attempt_import('pandas')
    dup_df = pd.DataFrame(rows)
    unseen, update, delta = pipe.filter_existing(dup_df, debug=debug)

    assert len(unseen) == 0
    assert len(update) == 0
    assert len(delta) == 0


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_partial_update(flavor: str):
    """
    Some rows are new, some are updates.
    - id=1: value changed → update_df
    - id=2: unchanged → not in any df
    - id=3: new row → unseen_df
    - delta = unseen + update
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'partial', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'partial', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})

    existing = [
        {'dt': datetime(2021, 1, 1), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2), 'id': 2, 'val': 20},
    ]
    pipe.sync(existing, debug=debug)

    incoming = [
        {'dt': datetime(2021, 1, 1), 'id': 1, 'val': 99},  # changed
        {'dt': datetime(2021, 1, 2), 'id': 2, 'val': 20},  # unchanged
        {'dt': datetime(2021, 1, 3), 'id': 3, 'val': 30},  # new
    ]
    pd = mrsm.attempt_import('pandas')
    incoming_df = pd.DataFrame(incoming)
    unseen, update, delta = pipe.filter_existing(incoming_df, debug=debug)

    assert len(unseen) == 1
    assert int(unseen['id'].iloc[0]) == 3

    assert len(update) == 1
    assert int(update['id'].iloc[0]) == 1

    assert len(delta) == 2
    delta_ids = sorted(int(v) for v in delta['id'])
    assert delta_ids == [1, 3]


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_no_datetime_column(flavor: str):
    """Pipes without a datetime column still filter correctly by id."""
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'no_dt', instance=conn,
                     columns={'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'no_dt', instance=conn,
                     columns={'id': 'id'})

    pipe.sync([{'id': 1, 'val': 'a'}, {'id': 2, 'val': 'b'}], debug=debug)

    pd = mrsm.attempt_import('pandas')
    incoming_df = pd.DataFrame([
        {'id': 2, 'val': 'b'},   # unchanged
        {'id': 3, 'val': 'c'},   # new
    ])
    unseen, update, delta = pipe.filter_existing(incoming_df, debug=debug)

    assert len(unseen) == 1
    assert int(unseen['id'].iloc[0]) == 3


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_no_pipe_data(flavor: str):
    """filter_existing on an unsynced pipe treats all rows as unseen."""
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'no_data', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'no_data', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.register(debug=debug)

    pd = mrsm.attempt_import('pandas')
    df = pd.DataFrame([
        {'dt': datetime(2021, 1, 1), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2), 'id': 2, 'val': 20},
    ])
    unseen, update, delta = pipe.filter_existing(df, debug=debug)

    assert len(unseen) == 2
    assert len(update) == 0
    assert len(delta) == 2
