"""
Generate parity fixtures for the DataFrame part of `Pipe.filter_existing`
(`meerschaum/core/Pipe/_sync.py`): the instance is stubbed out, `get_data` returns a synthetic
backtrack frame, and the `begin` / `end` / `params` it receives are recorded.
"""

import json
from decimal import Decimal

import pandas as pd

import meerschaum as mrsm
from _common import encode_frame, encode, dump

DT = lambda *a: pd.to_datetime(list(a), utc=True)  # noqa: E731

REMOTE = {'dt': 'datetime64[us, UTC]', 'id': 'int64[pyarrow]', 'val': 'float64[pyarrow]'}

CASES = [
    {
        'name': 'basic',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'remote': REMOTE,
        'df': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-02', '2024-01-03'), 'id': [1, 1, 2], 'val': [1.0, 2.5, 3.0]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-02'), 'id': [1, 1], 'val': [1.0, 2.0]}),
    },
    {
        'name': 'partial_row_update',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'remote': {**REMOTE, 'other': 'string'},
        'df': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'val': [9.0]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'val': [1.0], 'other': ['keep']}),
    },
    {
        'name': 'partial_row_update_unchanged_cols',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'remote': {**REMOTE, 'other': 'string'},
        'df': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'val': [9.0]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'val': [1.0], 'other': ['keep']}),
        'kwargs': {'include_unchanged_columns': True},
    },
    {
        'name': 'primary_key',
        'columns': {'primary': 'pk', 'datetime': 'dt'},
        'remote': {'pk': 'int64[pyarrow]', 'dt': 'datetime64[us, UTC]', 'val': 'int64[pyarrow]'},
        'df': pd.DataFrame({'pk': [1, 2, 3], 'dt': DT('2024-01-01', '2024-01-01', '2024-01-02'), 'val': [1, 5, 3]}),
        'backtrack': pd.DataFrame({'pk': [1, 2], 'dt': DT('2024-01-01', '2024-01-01'), 'val': [1, 2]}),
    },
    {
        'name': 'no_backtrack',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'remote': REMOTE,
        'df': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'val': [1.0]}),
        'backtrack': None,
    },
    {
        'name': 'empty_df',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'remote': REMOTE,
        'df': pd.DataFrame({'dt': pd.Series([], dtype='datetime64[us, UTC]'), 'id': pd.Series([], dtype='int64')}),
        'backtrack': None,
    },
    {
        'name': 'duplicate_backtrack_index',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'remote': REMOTE,
        'df': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'val': [5.0]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-01'), 'id': [1, 1], 'val': [1.0, 2.0]}),
    },
    {
        'name': 'int_axis',
        'columns': {'datetime': 'ts', 'id': 'id'},
        'configured': {'ts': 'int'},
        'remote': {'ts': 'int64[pyarrow]', 'id': 'int64[pyarrow]', 'val': 'int64[pyarrow]'},
        'df': pd.DataFrame({'ts': [100, 160, 220], 'id': [1, 1, 1], 'val': [1, 2, 3]}),
        'backtrack': pd.DataFrame({'ts': [100, 160], 'id': [1, 1], 'val': [1, 20]}),
    },
    {
        'name': 'no_datetime',
        'columns': {'id': 'id'},
        'remote': {'id': 'int64[pyarrow]', 'val': 'string'},
        'df': pd.DataFrame({'id': [1, 2], 'val': ['a', 'b']}),
        'backtrack': pd.DataFrame({'id': [1], 'val': ['z']}),
    },
    {
        'name': 'value_role_excluded',
        'columns': {'datetime': 'dt', 'id': 'id', 'value': 'val'},
        'remote': REMOTE,
        'df': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-02'), 'id': [1, 1], 'val': [7.0, 8.0]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'val': [1.0]}),
    },
    {
        'name': 'json_column',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'configured': {'meta': 'json'},
        'remote': {'dt': 'datetime64[us, UTC]', 'id': 'int64[pyarrow]', 'meta': 'json'},
        'df': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-02'), 'id': [1, 1], 'meta': [{'b': 2, 'a': 1}, {'x': 1}]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'meta': [{'a': 1, 'b': 2}]}),
    },
    {
        'name': 'date_bound_only',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'remote': REMOTE,
        'df': pd.DataFrame({'dt': DT('2024-01-01 12:34:56'), 'id': [1], 'val': [1.0]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01 12:34:56'), 'id': [1], 'val': [1.0]}),
        'kwargs': {'date_bound_only': True},
    },
    {
        'name': 'static_no_mixed_numerics',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'parameters': {'static': True},
        'remote': {'dt': 'datetime64[us, UTC]', 'id': 'int64[pyarrow]', 'val': 'int64[pyarrow]'},
        'df': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-02'), 'id': [1, 1], 'val': [1.0, 2.5]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-02'), 'id': [1, 1], 'val': [1, 2]}),
    },
    {
        'name': 'mixed_numerics',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'remote': {'dt': 'datetime64[us, UTC]', 'id': 'int64[pyarrow]', 'val': 'int64[pyarrow]'},
        'df': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-02'), 'id': [1, 1], 'val': [1.0, 2.5]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-02'), 'id': [1, 1], 'val': [1, 2]}),
    },
    {
        'name': 'autotime_null_dt',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'parameters': {'autotime': True},
        'remote': REMOTE,
        'df': pd.DataFrame({'dt': pd.Series([None, None], dtype='datetime64[us, UTC]'), 'id': [1, 2], 'val': [1.0, 2.0]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'val': [1.0]}),
    },
    {
        'name': 'autoincrement_null_pk',
        'columns': {'primary': 'pk', 'id': 'id'},
        'parameters': {'autoincrement': True},
        'remote': {'pk': 'int64[pyarrow]', 'id': 'int64[pyarrow]', 'val': 'float64[pyarrow]'},
        'df': pd.DataFrame({'pk': pd.array([None, None], dtype='Int64'), 'id': [1, 2], 'val': [1.0, 2.0]}),
        'backtrack': pd.DataFrame({'pk': [1], 'id': [1], 'val': [1.0]}),
    },
    {
        'name': 'index_limit',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'remote': REMOTE,
        'df': pd.DataFrame({'dt': DT(*(['2024-01-01'] * 300)), 'id': list(range(300)), 'val': [1.0] * 300}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [5], 'val': [1.0]}),
    },
    {
        'name': 'null_index_values',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'remote': {'dt': 'datetime64[us, UTC]', 'id': 'string', 'val': 'float64[pyarrow]'},
        'df': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-01'), 'id': ['a', None], 'val': [1.0, 2.0]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [None], 'val': [1.0]}),
    },
    {
        'name': 'docs_input',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'remote': REMOTE,
        'df': [{'dt': '2024-01-01 00:00:00', 'id': 1, 'val': 2.5}, {'dt': '2024-01-05 00:00:00', 'id': 2, 'val': 1.5}],
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'val': [1.0]}),
    },
    {
        'name': 'no_enforce_aware_update',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'parameters': {'enforce': False},
        'remote': REMOTE,
        'df': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-02'), 'id': [1, 1], 'val': [9.0, 2.0]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-02'), 'id': [1, 1], 'val': [1.0, 2.0]}),
    },
    {
        'name': 'no_enforce_naive_both',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'parameters': {'enforce': False},
        'remote': {**REMOTE, 'dt': 'datetime64[us]'},
        'df': pd.DataFrame({'dt': pd.to_datetime(['2024-01-01', '2024-01-02']), 'id': [1, 1], 'val': [9.0, 2.0]}),
        'backtrack': pd.DataFrame({'dt': pd.to_datetime(['2024-01-01', '2024-01-02']), 'id': [1, 1], 'val': [1.0, 2.0]}),
    },
    {
        'name': 'no_enforce_naive_df_aware_backtrack',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'parameters': {'enforce': False},
        'remote': REMOTE,
        'df': pd.DataFrame({'dt': pd.to_datetime(['2024-01-01']), 'id': [1], 'val': [9.0]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'val': [1.0]}),
    },
    {
        'name': 'no_enforce_aware_df_naive_backtrack',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'parameters': {'enforce': False},
        'remote': REMOTE,
        'df': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'val': [9.0]}),
        'backtrack': pd.DataFrame({'dt': pd.to_datetime(['2024-01-01']), 'id': [1], 'val': [1.0]}),
    },
    {
        'name': 'no_enforce_integer_df_datetime_backtrack',
        'columns': {'datetime': 'ts', 'id': 'id'},
        'parameters': {'enforce': False},
        'remote': {'ts': 'datetime64[us, UTC]', 'id': 'int64[pyarrow]', 'val': 'int64[pyarrow]'},
        'df': pd.DataFrame({'ts': [1], 'id': [1], 'val': [9]}),
        'backtrack': pd.DataFrame({'ts': DT('1970-01-01 00:00:00.000001'), 'id': [1], 'val': [1]}),
    },
    {
        'name': 'numeric_column',
        'columns': {'datetime': 'dt', 'id': 'id'},
        'configured': {'num': 'numeric[10,2]'},
        'remote': {'dt': 'datetime64[us, UTC]', 'id': 'int64[pyarrow]', 'num': 'numeric[10,2]'},
        'df': pd.DataFrame({'dt': DT('2024-01-01', '2024-01-02'), 'id': [1, 1], 'num': [Decimal('1.10'), Decimal('2.5')]}),
        'backtrack': pd.DataFrame({'dt': DT('2024-01-01'), 'id': [1], 'num': [Decimal('1.10')]}),
    },
]


def run(ix, case):
    params = dict(case.get('parameters', {}))
    pipe = mrsm.Pipe(
        'gen_fixture', f'existing_{ix}',
        instance='sql:local',
        columns=case['columns'],
        dtypes=case.get('configured', {}),
        parameters=params,
    )
    pipe._invalidate_cache(hard=True)
    remote = case['remote']
    pipe.infer_dtypes = lambda persist=False, refresh=False, debug=False: dict(remote)
    calls = []

    def get_data(begin=None, end=None, params=None, **kw):
        calls.append({
            'begin': encode(begin),
            'end': encode(end),
            'params': {k: [encode(v) for v in vals] for k, vals in (params or {}).items()},
        })
        bt = case['backtrack']
        return bt.copy() if bt is not None else None

    pipe.get_data = get_data
    df = case['df']
    df_in = df.copy() if isinstance(df, pd.DataFrame) else df
    unseen, update, delta = pipe.filter_existing(df_in, **case.get('kwargs', {}))
    return {
        'pipe_dtypes': pipe.get_dtypes(),
        'explicit_dtypes': pipe.get_dtypes(infer=False),
        'get_data_calls': calls,
        'unseen': encode_frame(unseen),
        'update': encode_frame(update),
        'delta': encode_frame(delta),
    }


def main():
    cases = []
    for ix, case in enumerate(CASES):
        df = case['df']
        entry = {
            'name': case['name'],
            'columns': case['columns'],
            'configured_dtypes': case.get('configured', {}),
            'remote_dtypes': case['remote'],
            'parameters': case.get('parameters', {}),
            'kwargs': case.get('kwargs', {}),
            'filter_params_index_limit': mrsm.get_config('pipes', 'sync', 'filter_params_index_limit'),
            'df': encode_frame(df) if isinstance(df, pd.DataFrame) else [{k: encode(v) for k, v in d.items()} for d in df],
            'backtrack': encode_frame(case['backtrack']),
        }
        try:
            entry['ok'] = run(ix, case)
        except Exception as e:
            import traceback
            traceback.print_exc()
            entry['err'] = f'{type(e).__name__}: {e}'
        cases.append(entry)
    dump('filter_existing', cases)


if __name__ == '__main__':
    main()
