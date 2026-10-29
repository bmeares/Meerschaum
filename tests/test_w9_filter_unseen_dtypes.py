"""
`filter_unseen_df` result dtypes through the façade (PARITY.md rows 17 and 18): the Rust kernel
returns the dtype v4 gets from `merge(new, old)` after `infer_objects()`. The dtypes and rows the
parity-phase Python mode produced for each case are recorded in
`tests/oracle/test_w9_filter_unseen_dtypes.json` (not v4.1.4 output) and must agree with the Rust
result.
"""

import datetime
from decimal import Decimal

import pandas as pd
import pytest

from tests import _oracle as oracle


def _frames(column, old_values, new_values, dtype=object):
    return (
        pd.DataFrame({'id': [1, 2], column: pd.Series(old_values, dtype=dtype)}),
        pd.DataFrame({'id': [2, 3], column: pd.Series(new_values, dtype=dtype)}),
    )


CASES = {
    'bytes_hint': (
        'blob', [b'a', b'b'], [b'b', b'c'], {'id': 'int', 'blob': 'bytes'}, 'object',
    ),
    'bytes_no_hint': ('blob', [b'a', b'b'], [b'b', b'c'], None, 'object'),
    'date_hint': (
        'd',
        [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
        [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)],
        {'id': 'int', 'd': 'date'},
        'object',
    ),
    'object_floats': ('a', [1.5, 2.5], [2.5, 3.5], None, 'float64'),
    'object_floats_numeric_hint': ('a', [1.5, 2.5], [2.5, 3.5], {'id': 'int', 'a': 'numeric'}, 'float64'),
    'decimals': ('a', [Decimal('1.5'), Decimal('2.5')], [Decimal('2.5'), Decimal('3.5')], None, 'object'),
}


@pytest.mark.parametrize('name', list(CASES))
def test_result_dtype_matches_v4(name):
    column, old_values, new_values, dtypes, expected_dtype = CASES[name]
    old, new = _frames(column, old_values, new_values)
    from meerschaum.utils.dataframe import filter_unseen_df

    v4 = oracle.expected()
    result = filter_unseen_df(old, new, dtypes=dtypes)
    assert v4['dtype'] == expected_dtype
    assert str(result[column].dtype) == v4['dtype']
    assert result.to_dict('records') == v4['records']
