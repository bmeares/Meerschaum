"""
Generate parity fixtures for the dtype of `filter_unseen_df` results (PARITY.md rows 17 and 18).

v4 returns the rows of `merge(new, old, how='left')` after `normalize_nulls` called
`infer_objects()` on both frames, so the output dtype of a column is the dtype pandas gives that
merge, not the dtype of the new frame. Every case here is one value column `v` (plus an `id`
column) with an old and a new frame of the given dtypes and an optional `dtypes` hint.

The matrix is the one enumerated for PARITY.md: old frames hold what a pipe's instance returns
(Arrow-backed columns, or `object` columns holding special values), new frames are anything a
caller might pass, and the hint is absent or the natural one for the old column. Pairs that make
pandas raise (an `int64` column against a `str` one) are not recorded. The ns-resolution and
NumPy-backed `bool` / `str` old columns are left out: the kernel does not model pandas backends
(see PARITY.md).

Run from the repository root with the reference interpreter:

    scripts/parity/run.sh crates/meerschaum-kernel/tests/fixtures/gen_w9_merge_dtypes.py
"""

import datetime as dtm
import itertools
import uuid
import warnings
from decimal import Decimal

import pandas as pd

from _common import encode_frame, dump

from meerschaum.utils.dataframe import filter_unseen_df

warnings.simplefilter('ignore')

U = [uuid.UUID(int=i + 1) for i in range(3)]
FAMILIES = {
    'int': ('N', [1, 2, 3]),
    'float': ('N', [1.5, 2.5, 3.5]),
    'floatint': ('N', [1.0, 2.0, 3.0]),
    'bool': ('N', [True, False, True]),
    'dec': ('N', [Decimal('1.5'), Decimal('2.5'), Decimal('3.5')]),
    'str': ('S', ['a', 'b', 'c']),
    'bytes': ('Y', [b'a', b'b', b'c']),
    'uuid': ('U', U),
    'json': ('J', [{'a': 1}, {'a': 2}, {'a': 3}]),
    'dtutc': ('T', [pd.Timestamp(f'2024-01-0{i}', tz='UTC') for i in (1, 2, 3)]),
    'dtnaive': ('T', [pd.Timestamp(f'2024-01-0{i}') for i in (1, 2, 3)]),
    'date': ('D', [dtm.date(2024, 1, i) for i in (1, 2, 3)]),
}

# (label, value family, pandas dtype)
OLD = [
    ('int64', 'int', 'int64'), ('int64[pa]', 'int', 'int64[pyarrow]'),
    ('float64', 'float', 'float64'), ('double[pa]', 'float', 'double[pyarrow]'),
    ('bool[pa]', 'bool', 'bool[pyarrow]'),
    ('string[pa]', 'str', 'string[pyarrow]'),
    ('binary[pa]', 'bytes', 'binary[pyarrow]'),
    ('dt[us,UTC]', 'dtutc', 'datetime64[us, UTC]'), ('dt[us]', 'dtnaive', 'datetime64[us]'),
    ('ts[us,UTC][pa]', 'dtutc', 'timestamp[us, tz=UTC][pyarrow]'),
    ('date32[pa]', 'date', 'date32[day][pyarrow]'),
    ('obj:str', 'str', 'object'), ('obj:int', 'int', 'object'), ('obj:float', 'float', 'object'),
    ('obj:bool', 'bool', 'object'), ('obj:bytes', 'bytes', 'object'), ('obj:dec', 'dec', 'object'),
    ('obj:uuid', 'uuid', 'object'), ('obj:json', 'json', 'object'),
    ('obj:dtutc', 'dtutc', 'object'), ('obj:dtnaive', 'dtnaive', 'object'),
    ('obj:date', 'date', 'object'),
]
NEW = [
    ('int64', 'int', 'int64'), ('Int64', 'int', 'Int64'), ('int64[pa]', 'int', 'int64[pyarrow]'),
    ('float64', 'float', 'float64'), ('double[pa]', 'float', 'double[pyarrow]'),
    ('bool', 'bool', 'bool'), ('bool[pa]', 'bool', 'bool[pyarrow]'),
    ('str', 'str', 'str'), ('string[pa]', 'str', 'string[pyarrow]'),
    ('binary[pa]', 'bytes', 'binary[pyarrow]'),
    ('dt[us,UTC]', 'dtutc', 'datetime64[us, UTC]'), ('dt[us]', 'dtnaive', 'datetime64[us]'),
    ('ts[us,UTC][pa]', 'dtutc', 'timestamp[us, tz=UTC][pyarrow]'),
    ('date32[pa]', 'date', 'date32[day][pyarrow]'),
    ('obj:str', 'str', 'object'), ('obj:int', 'int', 'object'), ('obj:float', 'float', 'object'),
    ('obj:floatint', 'floatint', 'object'),
    ('obj:bool', 'bool', 'object'), ('obj:bytes', 'bytes', 'object'), ('obj:dec', 'dec', 'object'),
    ('obj:uuid', 'uuid', 'object'), ('obj:json', 'json', 'object'),
    ('obj:dtutc', 'dtutc', 'object'), ('obj:dtnaive', 'dtnaive', 'object'),
    ('obj:date', 'date', 'object'),
]
# The hint a pipe would declare for the old column's family.
NATURAL_HINT = {
    'int': 'int', 'float': 'float', 'bool': 'bool', 'dec': 'numeric', 'str': 'str',
    'bytes': 'bytes', 'uuid': 'uuid', 'json': 'json', 'dtutc': 'datetime', 'dtnaive': 'datetime',
    'date': 'date',
}


def make(spec, window):
    _, family, dtype = spec
    start, stop = window
    values = FAMILIES[family][1][start:stop]
    series = pd.Series(values, dtype='object' if dtype == 'object' else dtype)
    return pd.DataFrame({'id': [1, 2, 3][start:stop], 'v': series})


def main():
    cases = []
    for old_spec, new_spec in itertools.product(OLD, NEW):
        old_group, new_group = FAMILIES[old_spec[1]][0], FAMILIES[new_spec[1]][0]
        if old_group != new_group:
            continue
        hints = [None, {'id': 'int', 'v': NATURAL_HINT[old_spec[1]]}]
        for hint in hints:
            old, new = make(old_spec, (0, 2)), make(new_spec, (1, 3))
            try:
                out = filter_unseen_df(old, new, dtypes=hint)
            except Exception:
                ### pandas refuses to merge the pair (e.g. `int64` against `str`).
                continue
            cases.append({
                'name': f'{old_spec[0]}|{new_spec[0]}|{hint["v"] if hint else None}',
                'old': encode_frame(old),
                'new': encode_frame(new),
                'dtypes': hint,
                'kwargs': {},
                'ok': encode_frame(out),
            })
    dump('w9_merge_dtypes', cases)


if __name__ == '__main__':
    main()
