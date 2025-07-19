#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test functions from `meerschaum.utils.misc`.
"""

from datetime import datetime, timezone
import pytest
from meerschaum.utils.packages import attempt_import
DEBUG: bool = True
pd = attempt_import('pandas')


@pytest.mark.parametrize(
    "dtype",
    [
        'object',
        'bool[pyarrow]',
        'float64',
        'datetime64[ns]',
        'int64',
        'int32',
        'int64[pyarrow]',
        'datetime64[ns, UTC]',
    ]
)
def test_add_missing_cols_to_df(dtype: str):
    """
    Test that new columns are successfully added to a dataframe.
    """
    from meerschaum.utils.dataframe import add_missing_cols_to_df
    df = pd.DataFrame([{'foo': 'bar'}])
    new_df = add_missing_cols_to_df(df, {'baz': dtype})
    assert len(new_df.columns) == 2
    assert str(new_df.dtypes['baz']).lower() == dtype.lower()


@pytest.mark.parametrize(
    'old_docs,new_docs,expected_docs',
    [
        (
            [
                {'a': 1, 'b': 1},
            ], [
                {'a': 1},
            ], [
                {'a': 1},
            ]
        ),
        (
            [
                {'a': 1,},
            ], [
                {'a': 1},
            ], []
        ),
        (
            [
                {'a': datetime(2022, 1, 1), 'b': 100.0},
            ], [
                {'a': datetime(2022, 1, 1), 'b': 100.0},
            ], []
        ),
        (
            [
                {'a': 1, 'b': 1},
            ], [], []
        ),
        (
            [
                {'a': 'foo', 'b': 'bar'},
            ], [
                {'a': 'foo', 'b': 'bar'},
            ], []
        ),
        (
            [
                {'a': False, 'b': 'bar'},
            ], [
                {'a': False, 'b': 'bar'},
            ], []
        ),
        (
            [], [], []
        ),
        (
            [{'a': None}], [{'a': None}], []
        ),
        (
            pd.DataFrame([{'a': 1}], dtype='int64[pyarrow]'),
            pd.DataFrame([{'a': 1}], dtype='int32'),
            []
        )
    ]
)
def test_filter_unseen_df(old_docs, new_docs, expected_docs):
    """
    Test that duplicate rows are removed.
    """
    from meerschaum.utils.dataframe import filter_unseen_df
    old_df = pd.DataFrame(old_docs) if not isinstance(old_docs, pd.DataFrame) else old_docs
    new_df = pd.DataFrame(new_docs) if not isinstance(new_docs, pd.DataFrame) else new_docs
    delta_df = filter_unseen_df(old_df, new_df)
    assert delta_df.to_dict(orient='records') == expected_docs


@pytest.mark.parametrize(
    'df,expected_types,expected_tuples',
    [
        (
            pd.DataFrame({
                'dt_tz_aware': [datetime(2025, 1, 1, tzinfo=timezone.utc)],
                'distant_dt': [datetime(1, 1, 1)],
                'dt_second': pd.to_datetime([datetime(2025, 1, 1)]).astype('datetime64[s]'),
                'other': [1],
            }),
            {
                'dt_tz_aware': 'datetime64[ns, UTC]',
                'distant_dt': 'datetime64[us]',
                'dt_second': 'datetime64[s]',
            },
            {
                'dt_tz_aware': ('UTC', 'ns'),
                'distant_dt': (None, 'us'),
                'dt_second': (None, 's'),
            },
        ),
    ]
)
def test_get_datetime_cols_types(df, expected_types, expected_tuples):
    """
    Test that datetime columns are correctly identified along with their types.
    """
    from meerschaum.utils.dataframe import get_datetime_cols, get_datetime_cols_types
    dt_cols_types = get_datetime_cols_types(df)
    assert dt_cols_types == expected_types

    dt_cols_tuples = get_datetime_cols(df, with_tz_precision=True)
    assert dt_cols_tuples == expected_tuples
