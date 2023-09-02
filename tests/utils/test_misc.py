#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test functions from `meerschaum.utils.misc`.
"""

import datetime
import pytest
from meerschaum.utils.packages import import_pandas
DEBUG: bool = True
pd = import_pandas(debug=DEBUG)

@pytest.mark.parametrize(
    "dtype",
    [
        'object', 'bool', 'float64', 'datetime64[ns]',
        'Int64', 'int64', 'datetime64[ns, UTC]'
    ]
)
def test_add_missing_cols_to_df(dtype: str):
    """
    Test that new columns are successfully added to a dataframe.
    """
    from meerschaum.utils.misc import add_missing_cols_to_df
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
                {'a': datetime.datetime(2022, 1, 1), 'b': 100.0},
            ], [
                {'a': datetime.datetime(2022, 1, 1), 'b': 100.0},
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
    ]
)
def test_filter_unseen_df(old_docs, new_docs, expected_docs):
    """
    Test that duplicate rows are removed.
    """
    from meerschaum.utils.misc import filter_unseen_df
    old_df = pd.DataFrame(old_docs)
    new_df = pd.DataFrame(new_docs)
    delta_df = filter_unseen_df(old_df, new_df)
    assert delta_df.to_dict(orient='records') == expected_docs
