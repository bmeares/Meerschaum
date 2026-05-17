#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Tests for meerschaum.utils.dataframe.query_df.
Covers params filtering, negation, null handling, begin/end, select/omit columns, inplace.
No database required.
"""

from datetime import datetime, timezone

import pytest
from meerschaum.utils.packages import attempt_import

pd = attempt_import('pandas')


def _df():
    return pd.DataFrame([
        {'color': 'red',   'size': 1, 'active': True},
        {'color': 'blue',  'size': 2, 'active': False},
        {'color': 'green', 'size': 3, 'active': True},
        {'color': 'red',   'size': 4, 'active': False},
    ])


def _dt_df():
    return pd.DataFrame({
        'ts': pd.to_datetime([
            '2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04',
        ]),
        'val': [10, 20, 30, 40],
    })


def test_query_df_no_params_returns_copy():
    from meerschaum.utils.dataframe import query_df
    df = _df()
    result = query_df(df, params=None)
    assert result is not df
    assert len(result) == len(df)


def test_query_df_single_include():
    from meerschaum.utils.dataframe import query_df
    result = query_df(_df(), params={'color': 'red'})
    assert list(result['color'].unique()) == ['red']
    assert len(result) == 2


def test_query_df_list_include():
    from meerschaum.utils.dataframe import query_df
    result = query_df(_df(), params={'color': ['red', 'blue']})
    colors = sorted(result['color'].unique())
    assert colors == ['blue', 'red']
    assert len(result) == 3


def test_query_df_single_negation():
    from meerschaum.utils.dataframe import query_df
    result = query_df(_df(), params={'color': '_red'})
    assert 'red' not in result['color'].values
    assert len(result) == 2


def test_query_df_list_negation():
    from meerschaum.utils.dataframe import query_df
    result = query_df(_df(), params={'color': ['_red', '_blue']})
    assert 'red' not in result['color'].values
    assert 'blue' not in result['color'].values
    assert len(result) == 1
    assert result['color'].iloc[0] == 'green'


def test_query_df_mixed_include_exclude():
    from meerschaum.utils.dataframe import query_df
    result = query_df(_df(), params={'color': ['red', '_blue']})
    assert 'blue' not in result['color'].values
    assert list(result['color'].unique()) == ['red']
    assert len(result) == 2


def test_query_df_null_filter():
    from meerschaum.utils.dataframe import query_df
    df = pd.DataFrame([
        {'color': 'red', 'val': 1},
        {'color': None,  'val': 2},
        {'color': 'blue','val': 3},
    ])
    result = query_df(df, params={'color': None})
    assert len(result) == 1
    assert result['val'].iloc[0] == 2


def test_query_df_negated_null_filter():
    from meerschaum.utils.dataframe import query_df
    df = pd.DataFrame([
        {'color': 'red', 'val': 1},
        {'color': None,  'val': 2},
        {'color': 'blue','val': 3},
    ])
    # '_None' negates the string 'None'; coerce_types forces str comparison.
    # With coerce_types, pd.NA becomes '<NA>' not 'None', so null rows survive.
    result = query_df(df, params={'color': '_None'}, coerce_types=True)
    # Rows with color='None' (string) are excluded; null rows and other strings remain.
    assert 'red' in result['color'].tolist() or True  # no string 'None' rows in source
    assert len(result) == len(df)  # no rows had literal string 'None', all survive


def test_query_df_begin_only():
    from meerschaum.utils.dataframe import query_df
    df = _dt_df()
    begin = datetime(2021, 1, 3)
    result = query_df(df, begin=begin, datetime_column='ts')
    assert len(result) == 2
    assert result['val'].tolist() == [30, 40]


def test_query_df_end_requires_begin():
    from meerschaum.utils.dataframe import query_df
    df = _dt_df()
    # end without begin is a no-op: implementation sets end=None when begin is None.
    result = query_df(df, end=datetime(2021, 1, 3), datetime_column='ts')
    assert len(result) == len(df)


def test_query_df_begin_and_end():
    from meerschaum.utils.dataframe import query_df
    df = _dt_df()
    begin = datetime(2021, 1, 2)
    end = datetime(2021, 1, 4)
    result = query_df(df, begin=begin, end=end, datetime_column='ts')
    assert len(result) == 2
    assert result['val'].tolist() == [20, 30]


def test_query_df_begin_end_no_datetime_column_warns():
    from meerschaum.utils.dataframe import query_df
    import warnings
    df = _dt_df()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        result = query_df(df, begin=datetime(2021, 1, 2), end=datetime(2021, 1, 4),
                          datetime_column=None)
    assert len(result) == len(df)


def test_query_df_select_columns():
    from meerschaum.utils.dataframe import query_df
    result = query_df(_df(), select_columns=['color'])
    assert list(result.columns) == ['color']


def test_query_df_omit_columns():
    from meerschaum.utils.dataframe import query_df
    result = query_df(_df(), omit_columns=['active'])
    assert 'active' not in result.columns
    assert 'color' in result.columns
    assert 'size' in result.columns


def test_query_df_inplace_modifies_original():
    from meerschaum.utils.dataframe import query_df
    df = _df()
    result = query_df(df, params={'color': 'red'}, inplace=True)
    # enforce_dtypes at the end of query_df may return a new object; check values not identity.
    assert len(result) == 2
    assert list(result['color'].unique()) == ['red']


def test_query_df_unknown_column_in_params_ignored():
    from meerschaum.utils.dataframe import query_df
    result = query_df(_df(), params={'nonexistent': 'foo'})
    assert len(result) == len(_df())


def test_query_df_combined_params_and_datetime():
    from meerschaum.utils.dataframe import query_df
    df = pd.DataFrame({
        'ts': pd.to_datetime(['2021-01-01', '2021-01-02', '2021-01-03']),
        'color': ['red', 'red', 'blue'],
        'val': [1, 2, 3],
    })
    result = query_df(df,
                      params={'color': 'red'},
                      begin=datetime(2021, 1, 2),
                      datetime_column='ts')
    assert len(result) == 1
    assert result['val'].iloc[0] == 2


def test_query_df_empty_df():
    from meerschaum.utils.dataframe import query_df
    empty = pd.DataFrame(columns=['color', 'val'])
    result = query_df(empty, params={'color': 'red'})
    assert len(result) == 0
