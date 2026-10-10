#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`Pipe.get_data()` skips its Pandas re-sort when the datetime column is already strictly ordered.
The result must still match a full sort (ties, nulls, descending order, index reset).
"""

from datetime import datetime, timedelta

import pytest

import meerschaum as mrsm

pd = mrsm.attempt_import('pandas', lazy=False)


@pytest.fixture
def pipe(tmp_path):
    ### Connectors are cached by label, so give each test its own database.
    conn = mrsm.get_connector(
        f'sql:order_{tmp_path.name}', flavor='sqlite', database=str(tmp_path / 'order.db'),
    )
    _pipe = mrsm.Pipe(
        'test', 'get_data', 'order',
        instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
    )
    return _pipe


def _expected(df, ascending=True):
    return df.sort_values(['dt', 'id'], ascending=ascending).reset_index(drop=True)


@pytest.mark.parametrize('order', ['asc', 'desc'])
def test_unique_datetimes_ordered(pipe, order):
    begin = datetime(2026, 1, 1)
    docs = [{'dt': begin + timedelta(minutes=i), 'id': i % 3, 'val': i} for i in range(50)]
    assert pipe.sync(docs)[0]
    df = pipe.get_data(order=order)
    assert list(df.index) == list(range(len(df)))
    pd.testing.assert_frame_equal(df, _expected(df, ascending=(order == 'asc')))


@pytest.mark.parametrize('order', ['asc', 'desc'])
def test_tied_datetimes_still_sorted_by_id(pipe, order):
    begin = datetime(2026, 1, 1)
    docs = [
        {'dt': begin + timedelta(minutes=i // 5), 'id': (7 * i) % 5, 'val': i}
        for i in range(50)
    ]
    assert pipe.sync(docs)[0]
    df = pipe.get_data(order=order)
    assert not df['dt'].is_unique
    pd.testing.assert_frame_equal(df, _expected(df, ascending=(order == 'asc')))


def test_sort_df_shortcut_resets_unsorted_and_chunked_frames(pipe, monkeypatch):
    """Frames the instance returns out of order (or with per-chunk indices) are still fixed up."""
    begin = datetime(2026, 1, 1)
    docs = [{'dt': begin + timedelta(minutes=i), 'id': i, 'val': i} for i in range(10)]
    assert pipe.sync(docs)[0]
    stored = pipe.get_data()

    shuffled = pd.concat([stored.iloc[5:], stored.iloc[:5]])
    monkeypatch.setattr(
        pipe.instance_connector.__class__, 'get_pipe_data',
        lambda self, *args, **kwargs: shuffled.copy(),
    )
    df = pipe.get_data()
    assert list(df.index) == list(range(len(df)))
    pd.testing.assert_frame_equal(df, _expected(df))
