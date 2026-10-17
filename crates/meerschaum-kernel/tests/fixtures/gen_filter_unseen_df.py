"""
Generate parity fixtures for `meerschaum.utils.dataframe.filter_unseen_df`.
"""

import uuid
from datetime import date
from decimal import Decimal

import pandas as pd
import shapely

from _common import encode_frame, dump, dt, ndt

from meerschaum.utils.dataframe import filter_unseen_df

U1 = uuid.UUID('12345678-1234-5678-1234-567812345678')
U2 = uuid.UUID('87654321-4321-8765-4321-876543218765')


def DF(**cols):
    return pd.DataFrame(cols)


CASES = [
    ('ints_simple', DF(a=[1, 2]), DF(a=[2, 3]), None, {}),
    ('changed_values', DF(id=[1, 2], val=[10, 20]), DF(id=[1, 2, 3], val=[10, 25, 30]), None, {}),
    ('no_changes', DF(id=[1, 2], val=[10, 20]), DF(id=[2, 1], val=[20, 10]), None, {}),
    ('new_dups', DF(id=[1], val=[10]), DF(id=[2, 2, 1], val=[5, 5, 10]), None, {}),
    ('old_dups', DF(id=[1, 1], val=[10, 10]), DF(id=[1, 2], val=[10, 20]), None, {}),
    ('int_vs_float', DF(id=[1, 2], val=[1, 2]), DF(id=[1, 2], val=[1.0, 2.5]), None, {}),
    ('int_vs_float_nocoerce', DF(id=[1, 2], val=[1, 2]), DF(id=[1, 2], val=[1.0, 2.5]), None,
     {'coerce_mixed_numerics': False}),
    ('float_vs_int', DF(id=[1, 2], val=[1.5, 2.0]), DF(id=[1, 2], val=[1, 2]), None, {}),
    ('nan_equal', DF(id=[1, 2], val=[1.5, float('nan')]), DF(id=[1, 2], val=[1.5, float('nan')]), None, {}),
    ('none_vs_value', DF(id=[1, 2], val=['a', None]), DF(id=[1, 2], val=[None, None]), None, {}),
    ('string_none_is_null', DF(id=[1, 2], val=['a', None]), DF(id=[1, 2], val=['a', 'None']), None, {}),
    ('string_nan_is_null', DF(id=[1], val=[None]), DF(id=[1], val=['NaN']), None, {}),
    ('empty_string_not_null', DF(id=[1], val=[None]), DF(id=[1], val=['']), None, {}),
    ('extra_new_col', DF(id=[1, 2], val=[10, 20]), DF(id=[1, 2], val=[10, 20], extra=['x', 'y']), None, {}),
    ('partial_new', DF(id=[1, 2], val=[10, 20], other=['a', 'b']), DF(id=[1, 2], val=[10, 21]), None, {}),
    ('reordered_cols', DF(a=[1], b=[2]), DF(b=[2, 3], a=[1, 4]), None, {}),
    ('datetimes_utc', DF(dt=pd.to_datetime(['2024-01-01', '2024-01-02'], utc=True), val=[1, 2]),
     DF(dt=pd.to_datetime(['2024-01-02', '2024-01-03'], utc=True), val=[2, 3]), None, {}),
    ('datetimes_new_naive', DF(dt=pd.to_datetime(['2024-01-01', '2024-01-02'], utc=True), val=[1, 2]),
     DF(dt=pd.to_datetime(['2024-01-02', '2024-01-03']), val=[2, 3]),
     {'dt': 'datetime64[us, UTC]', 'val': 'int64[pyarrow]'}, {}),
    ('datetimes_dtypes_naive', DF(dt=pd.to_datetime(['2024-01-01', '2024-01-02']), val=[1, 2]),
     DF(dt=pd.to_datetime(['2024-01-02', '2024-01-03']), val=[2, 30]),
     {'dt': 'datetime64[us]', 'val': 'int64[pyarrow]'}, {}),
    ('json_key_order', DF(id=[1, 2], meta=[{'a': 1, 'b': 2}, {'x': [1, 2]}]),
     DF(id=[1, 2, 3], meta=[{'b': 2, 'a': 1}, {'x': [2, 1]}, None]), None, {}),
    ('json_vs_string', DF(id=[1], meta=[{'a': 1}]), DF(id=[1], meta=['{"a": 1}']), None, {}),
    ('numeric_scale', DF(id=[1, 2], num=[Decimal('1.10'), Decimal('2')]),
     DF(id=[1, 2], num=[Decimal('1.1'), Decimal('2')]), None, {}),
    ('numeric_dtypes_quantize', DF(id=[1, 2], num=[Decimal('1.10'), Decimal('2.00')]),
     DF(id=[1, 2, 3], num=[Decimal('1.10'), Decimal('2.5'), Decimal('3.333')]),
     {'id': 'int64', 'num': 'numeric[10,2]'}, {}),
    ('numeric_nan', DF(id=[1, 2], num=[Decimal('1'), None]), DF(id=[1, 2], num=[Decimal('1'), None]), None, {}),
    ('uuids', DF(id=[1, 2], u=[U1, U2]), DF(id=[1, 2, 3], u=[U1, U1, None]), None, {}),
    ('bytes', DF(id=[1, 2], b=[b'a', b'b']), DF(id=[1, 2], b=[b'a', b'c']), None, {}),
    ('geometry', DF(id=[1, 2], g=[shapely.Point(0, 0), shapely.Point(1, 1)]),
     DF(id=[1, 2], g=[shapely.Point(0, 0), shapely.Point(1, 2)]), None, {}),
    ('dates', DF(id=[1, 2], d=[date(2024, 1, 1), date(2024, 1, 2)]),
     DF(id=[1, 2], d=[date(2024, 1, 1), date(2024, 1, 3)]), None, {}),
    ('bools', DF(id=[1, 2], f=pd.array([True, False], dtype='boolean')),
     DF(id=[1, 2], f=pd.array([True, None], dtype='boolean')), None, {}),
    ('nullable_ints', DF(id=pd.array([1, 2], dtype='Int64'), v=pd.array([None, 2], dtype='Int64')),
     DF(id=pd.array([1, 2], dtype='Int64'), v=pd.array([None, 3], dtype='Int64')), None, {}),
    ('empty_old', DF(id=pd.Series([], dtype='int64'), val=pd.Series([], dtype='int64')),
     DF(id=[1], val=[2]), None, {}),
    ('empty_new', DF(id=[1], val=[2]),
     DF(id=pd.Series([], dtype='int64'), val=pd.Series([], dtype='int64')), None, {}),
    ('both_empty_no_cols', pd.DataFrame(), pd.DataFrame(), None, {}),
    ('old_none', None, DF(id=[1], val=[2]), None, {}),
    ('strings', DF(id=[1, 2], s=['a', 'b']), DF(id=[1, 2], s=['a', 'B']), None, {}),
    ('dtypes_cast_str', DF(id=[1, 2], s=['1', '2']), DF(id=[1, 2], s=[1, 3]),
     {'id': 'int64', 's': 'string'}, {}),
    ('epochs', DF(ts=[1700000000, 1700000060], v=[1.0, 2.0]), DF(ts=[1700000060, 1700000120], v=[2.0, 3.0]),
     None, {}),
]


def main():
    cases = []
    for name, old, new, dtypes, kwargs in CASES:
        entry = {
            'name': name,
            'old': encode_frame(old),
            'new': encode_frame(new),
            'dtypes': dtypes,
            'kwargs': kwargs,
        }
        try:
            entry['ok'] = encode_frame(filter_unseen_df(old, new, dtypes=dtypes, **kwargs))
        except Exception as e:
            entry['err'] = f'{type(e).__name__}: {e}'
        cases.append(entry)
    dump('filter_unseen_df', cases)


if __name__ == '__main__':
    main()
