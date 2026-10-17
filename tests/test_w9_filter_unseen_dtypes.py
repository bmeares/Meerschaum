"""
`filter_unseen_df` result dtypes through the façade (PARITY.md rows 17 and 18): the Rust kernel
returns the dtype v4 gets from `merge(new, old)` after `infer_objects()`. Each case runs in
`python` mode, then `rust` mode, and the dtypes and rows must agree.
"""

import datetime
from decimal import Decimal

import pandas as pd
import pytest

from meerschaum._core import _gate

try:
    from meerschaum import _core
    HAS_CORE = _core.load() is not None
except Exception:
    HAS_CORE = False

pytestmark = pytest.mark.skipif(not HAS_CORE, reason="the Rust extension is not built")


@pytest.fixture(autouse=True)
def restore_mode():
    original = (_gate._MODE, _gate._FEATURES)
    yield
    _gate._MODE, _gate._FEATURES = original


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
    results = {}
    for mode in ('python', 'rust'):
        _gate.set_mode(mode)
        results[mode] = filter_unseen_df(old, new, dtypes=dtypes)
    assert str(results['python'][column].dtype) == expected_dtype
    assert str(results['rust'][column].dtype) == str(results['python'][column].dtype)
    assert results['rust'].to_dict('records') == results['python'].to_dict('records')
