"""
Generate parity fixtures for `meerschaum.utils.dataframe.query_df` (the `params` grammar,
`begin` / `end` bounds, `select_columns` / `omit_columns`, `coerce_types`, `reset_index`).
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd

from _common import encode_frame, encode, dump, dt, ndt

from meerschaum.utils.dataframe import query_df


def frames():
    out = {}
    out['mixed'] = pd.DataFrame({
        'dt': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', None], utc=True),
        'id': [1, 2, 3, 4, 5],
        'color': ['red', 'blue', None, 'green', 'red'],
        'val': [1.5, float('nan'), 3.0, 4.0, 5.5],
        'n': pd.array([10, None, 30, 40, 50], dtype='Int64'),
        'flag': pd.array([True, False, None, True, False], dtype='boolean'),
    })
    out['naive'] = pd.DataFrame({
        'dt': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
        'id': [1, 2, 3],
    })
    out['epochs'] = pd.DataFrame({'ts': [100, 200, 300, 400], 'id': [1, 2, 3, 4]})
    out['objects'] = pd.DataFrame({
        'id': [1, 2, 3],
        'num': [Decimal('1.10'), Decimal('2'), None],
        'u': [uuid.UUID('12345678-1234-5678-1234-567812345678'), None, None],
        'o': pd.Series(['x', 1, None], dtype='object'),
    })
    out['all_null_row'] = pd.DataFrame({'id': [1, None], 'color': ['red', None]})
    out['empty'] = pd.DataFrame({'id': pd.Series([], dtype='int64'), 'color': pd.Series([], dtype='str')})
    return out


T1 = dt(2024, 1, 2)
T3 = dt(2024, 1, 3)

### (frame, kwargs)
CASES = [
    ('mixed', {}),
    ('mixed', {'params': {'color': 'red'}}),
    ('mixed', {'params': {'color': ['red', 'blue']}}),
    ('mixed', {'params': {'color': '_red'}}),
    ('mixed', {'params': {'color': ['_red', '_blue']}}),
    ('mixed', {'params': {'color': ['red', '_blue']}}),
    ('mixed', {'params': {'color': None}}),
    ('mixed', {'params': {'color': 'None'}}),
    ('mixed', {'params': {'color': '_None'}}),
    ('mixed', {'params': {'color': ['red', None]}}),
    ('mixed', {'params': {'color': ['_red', None]}}),
    ('mixed', {'params': {'id': 2}}),
    ('mixed', {'params': {'id': [1, 3]}}),
    ('mixed', {'params': {'id': '2'}}),
    ('mixed', {'params': {'id': '_2'}}),
    ('mixed', {'params': {'id': [1, '_3']}}),
    ('mixed', {'params': {'id': 2}, 'coerce_types': True}),
    ('mixed', {'params': {'val': 1.5}}),
    ('mixed', {'params': {'val': None}}),
    ('mixed', {'params': {'val': '_None'}}),
    ('mixed', {'params': {'val': '1.5'}, 'coerce_types': True}),
    ('mixed', {'params': {'n': None}}),
    ('mixed', {'params': {'n': 10}}),
    ('mixed', {'params': {'n': '_10'}}),
    ('mixed', {'params': {'flag': True}}),
    ('mixed', {'params': {'flag': None}}),
    ('mixed', {'params': {'flag': '_True'}}),
    ('mixed', {'params': {'missing_col': 'x'}}),
    ('mixed', {'params': {'color': ('red', 'green')}}),
    ('mixed', {'params': {'color': 'red', 'id': [1, 5]}}),
    ('mixed', {'params': {'color': 'red'}, 'reset_index': True}),
    ('mixed', {'params': {'color': 'red'}, 'select_columns': ['id', 'color']}),
    ('mixed', {'params': {'color': 'red'}, 'omit_columns': ['val', 'n']}),
    ('mixed', {'select_columns': ['id']}),
    ('mixed', {'select_columns': ['*']}),
    ('mixed', {'omit_columns': ['flag']}),
    ('mixed', {'params': {'color': 'red'}, 'select_columns': ['*']}),
    ('mixed', {'begin': T1, 'datetime_column': 'dt'}),
    ('mixed', {'end': T3, 'datetime_column': 'dt'}),
    ('mixed', {'begin': T1, 'end': T3, 'datetime_column': 'dt'}),
    ('mixed', {'begin': '2024-01-02', 'end': '2024-01-04', 'datetime_column': 'dt'}),
    ('mixed', {'begin': ndt(2024, 1, 2), 'end': ndt(2024, 1, 4), 'datetime_column': 'dt'}),
    ('mixed', {'begin': T1, 'datetime_column': 'missing'}),
    ('mixed', {'begin': T1}),
    ('mixed', {'begin': T1, 'end': T3, 'datetime_column': 'dt', 'params': {'color': '_green'}}),
    ('mixed', {'begin': 1704153600, 'end': 1704326400, 'datetime_column': 'dt'}),
    ('naive', {'begin': T1, 'datetime_column': 'dt'}),
    ('naive', {'begin': ndt(2024, 1, 2), 'end': ndt(2024, 1, 3), 'datetime_column': 'dt'}),
    ('naive', {'end': ndt(2024, 1, 3), 'datetime_column': 'dt'}),
    ('epochs', {'begin': 200, 'end': 400, 'datetime_column': 'ts'}),
    ('epochs', {'begin': 0, 'end': 300, 'datetime_column': 'ts'}),
    ('epochs', {'end': 300, 'datetime_column': 'ts'}),
    ('epochs', {'params': {'ts': ['_100', '_200']}}),
    ('objects', {'params': {'num': Decimal('2')}}),
    ('objects', {'params': {'num': None}}),
    ('objects', {'params': {'u': None}}),
    ('objects', {'params': {'o': 1}}),
    ('objects', {'params': {'o': '1'}, 'coerce_types': True}),
    ('objects', {'params': {'o': '_x'}}),
    ('all_null_row', {'params': {'color': None}}),
    ('all_null_row', {'params': {'id': None}}),
    ('empty', {'params': {'color': 'red'}}),
    ('empty', {'params': {'color': '_red'}}),
]


def enc_kwargs(kwargs):
    out = {}
    for k, v in kwargs.items():
        if k == 'params':
            out[k] = {
                col: (
                    {'list': [encode(x) for x in val]} if isinstance(val, (list, tuple, set))
                    else encode(val)
                )
                for col, val in v.items()
            }
        elif k in ('begin', 'end'):
            out[k] = encode(v)
        else:
            out[k] = v
    return out


def main():
    fs = frames()
    cases = []
    for name, kwargs in CASES:
        df = fs[name]
        entry = {'frame': name, 'input': encode_frame(df), 'kwargs': enc_kwargs(kwargs)}
        try:
            entry['ok'] = encode_frame(query_df(df.copy(), **kwargs))
        except Exception as e:
            entry['err'] = f'{type(e).__name__}: {e}'
        cases.append(entry)
    dump('query_df', cases)


if __name__ == '__main__':
    main()
