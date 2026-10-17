"""
Shared helpers for the `meerschaum-kernel` parity fixture generators.

Values are encoded as tagged JSON so the Rust tests can rebuild typed frames:

- `null`                          Python `None`
- `true` / `false`                `bool`
- `"text"`                        `str`
- `{"t": "int", "v": 1}`          `int` (numpy ints included)
- `{"t": "float", "v": 1.5}`      `float` (`"v"` is `"nan"`, `"inf"` or `"-inf"` for non-finite)
- `{"t": "na"}` / `{"t": "nat"}`  `pd.NA` / `pd.NaT`
- `{"t": "dec", "v": "1.10"}`     `decimal.Decimal` (`str(x)`)
- `{"t": "uuid", "v": "..."}`     `uuid.UUID`
- `{"t": "bytes", "v": "b64"}`    `bytes`
- `{"t": "dt", "v": iso, "tz": name|null}`  `datetime` / `pd.Timestamp`
- `{"t": "date", "v": "2024-01-01"}`
- `{"t": "json", "v": ...}`       `dict` / `list` (inner values plain JSON)
- `{"t": "geom", "wkt": ..., "wkb": hex}`  shapely geometries

Frames are `{"columns": [{"name", "dtype", "values"}], "index": [...]}`.

Run from the repository root with a Python that has pandas 3, pyarrow, polars, shapely and
geopandas (the versions pinned in `requirements/full.txt`). Plugins must not be loaded, so point
`MRSM_PLUGINS_DIR` at an empty temporary directory:

    PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=$(mktemp -d) \
        python crates/meerschaum-kernel/tests/fixtures/gen_<name>.py

By default the generator asserts that `meerschaum` is imported from this repository (a stale
install under `~/.local` would otherwise shadow it). Set `MRSM_PARITY_REFERENCE=1` to accept a
reference interpreter with a released Meerschaum installed instead (CI regenerates fixtures from
4.1.4 via `scripts/parity/run.sh`).
"""

import base64
import json
import math
import os
import uuid
import warnings
from datetime import datetime, date, timezone, timedelta
from decimal import Decimal

warnings.simplefilter('ignore')

### Fixtures must come from the Python implementations, never from the Rust kernel they test.
os.environ['MRSM_CORE'] = 'python'

import meerschaum as mrsm  # noqa: E402

_REPO_ROOT = os.path.realpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..', '..'))
if not os.environ.get('MRSM_PARITY_REFERENCE'):
    assert os.path.realpath(mrsm.__file__).startswith(_REPO_ROOT), f"Stale meerschaum imported: {mrsm.__file__}"

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import shapely  # noqa: E402

FIXTURES_DIR = os.path.dirname(os.path.abspath(__file__))


def tz_name(value):
    tz = getattr(value, 'tzinfo', None)
    if tz is None:
        return None
    name = str(tz)
    if name in ('UTC', 'utc', 'UTC+00:00', 'tzutc()'):
        return 'UTC'
    return name


def encode(value):
    """Encode a Python value into tagged JSON."""
    if value is None:
        return None
    if value is pd.NA:
        return {'t': 'na'}
    if value is pd.NaT:
        return {'t': 'nat'}
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        value = int(value)
        if abs(value) > 2**63 - 1:
            return {'t': 'int', 'v': str(value)}
        return {'t': 'int', 'v': value}
    if isinstance(value, (float, np.floating)):
        value = float(value)
        if math.isnan(value):
            return {'t': 'float', 'v': 'nan'}
        if math.isinf(value):
            return {'t': 'float', 'v': 'inf' if value > 0 else '-inf'}
        return {'t': 'float', 'v': value}
    if isinstance(value, str):
        return value
    if isinstance(value, Decimal):
        return {'t': 'dec', 'v': str(value)}
    if isinstance(value, uuid.UUID):
        return {'t': 'uuid', 'v': str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {'t': 'bytes', 'v': base64.b64encode(bytes(value)).decode('utf-8')}
    if isinstance(value, pd.Timestamp):
        return {'t': 'dt', 'v': value.isoformat(), 'tz': tz_name(value)}
    if isinstance(value, datetime):
        return {'t': 'dt', 'v': value.isoformat(), 'tz': tz_name(value)}
    if isinstance(value, date):
        return {'t': 'date', 'v': value.isoformat()}
    if isinstance(value, (dict, list, tuple)):
        return {'t': 'json', 'v': json.loads(json.dumps(value, default=str))}
    if 'shapely' in str(type(value)):
        return {
            't': 'geom',
            'wkt': shapely.to_wkt(value, rounding_precision=-1),
            'wkb': shapely.to_wkb(value, hex=True, include_srid=False),
        }
    if isinstance(value, timedelta):
        return {'t': 'td', 'v': value.total_seconds()}
    return {'t': 'repr', 'v': repr(value)}


def encode_frame(df):
    """Encode a pandas (or geopandas) DataFrame."""
    if df is None:
        return None
    if df.__class__.__module__.split('.')[0] == 'polars':
        return {'polars': True, **encode_frame(df.to_pandas())}
    cols = []
    for col in df.columns:
        series = df[col]
        crs = None
        if hasattr(series, 'crs') and series.crs is not None:
            crs = series.crs.to_string()
        cols.append({
            'name': str(col),
            'dtype': str(series.dtype),
            'crs': crs,
            'values': [encode(v) for v in series.tolist()],
        })
    return {
        'columns': cols,
        'index': [encode(v) for v in df.index.tolist()],
    }


def dump(name, cases):
    path = os.path.join(FIXTURES_DIR, f'{name}.json')
    with open(path, 'w') as f:
        json.dump(
            {
                'generator': f'gen_{name}.py',
                'meerschaum_version': mrsm.__version__,
                'pandas_version': pd.__version__,
                'cases': cases,
            },
            f,
            indent=1,
            sort_keys=False,
        )
        f.write('\n')
    print(f"Wrote {len(cases)} cases to {path}")


def catch(fn, *args, **kwargs):
    """Call `fn`, returning `{'ok': result}` or `{'err': repr(exc)}`."""
    try:
        return {'ok': fn(*args, **kwargs)}
    except Exception as e:
        return {'err': f'{type(e).__name__}: {e}'}


UTC = timezone.utc
NY = 'America/New_York'


def dt(*args, tz=UTC):
    return datetime(*args, tzinfo=tz)


def ndt(*args):
    return datetime(*args)


def point(x, y):
    return shapely.Point(x, y)


### Common synthetic frames used across generators.
def sample_frames():
    """Return a dict of named synthetic DataFrames covering every dtype."""
    frames = {}
    frames['ints'] = pd.DataFrame({'id': [1, 2, 3], 'val': [10, 20, 30]})
    frames['nullable_ints'] = pd.DataFrame({
        'id': pd.array([1, 2, None], dtype='Int64'),
        'val': pd.array([None, 5, 6], dtype='Int64'),
    })
    frames['floats'] = pd.DataFrame({'id': [1, 2, 3], 'val': [1.5, float('nan'), 3.25]})
    frames['bools'] = pd.DataFrame({
        'id': [1, 2, 3],
        'flag': pd.array([True, False, None], dtype='boolean'),
    })
    frames['strings'] = pd.DataFrame({'id': [1, 2, 3], 'name': ['a', None, 'c']})
    frames['datetimes_utc'] = pd.DataFrame({
        'dt': pd.to_datetime(['2024-01-01 00:00', '2024-01-02 00:00', None], utc=True),
        'id': [1, 2, 3],
    })
    frames['datetimes_naive'] = pd.DataFrame({
        'dt': pd.to_datetime(['2024-01-01 00:00', '2024-01-02 12:30', '2024-01-03 00:00']),
        'id': [1, 2, 3],
    })
    frames['datetimes_ny'] = pd.DataFrame({
        'dt': pd.to_datetime(['2024-01-01 00:00', '2024-06-02 12:30']).tz_localize(NY),
        'id': [1, 2],
    })
    frames['epochs'] = pd.DataFrame({'ts': [1_700_000_000, 1_700_000_060, None], 'id': [1, 2, 3]})
    frames['numerics'] = pd.DataFrame({
        'id': [1, 2, 3],
        'num': [Decimal('1.10'), Decimal('2'), None],
    })
    frames['uuids'] = pd.DataFrame({
        'id': [1, 2],
        'u': [uuid.UUID('12345678-1234-5678-1234-567812345678'), None],
    })
    frames['json'] = pd.DataFrame({
        'id': [1, 2, 3],
        'meta': [{'b': 1, 'a': [1, 2]}, [1, 'x'], None],
    })
    frames['bytes'] = pd.DataFrame({'id': [1, 2], 'blob': [b'\x00\x01abc', None]})
    frames['dates'] = pd.DataFrame({'id': [1, 2], 'd': [date(2024, 1, 1), None]})
    frames['geometry'] = pd.DataFrame({
        'id': [1, 2, 3],
        'geom': [point(1, 2), shapely.LineString([(0, 0), (1, 1)]), None],
    })
    frames['empty'] = pd.DataFrame({'id': pd.Series([], dtype='int64'), 'val': pd.Series([], dtype='float64')})
    frames['no_cols'] = pd.DataFrame()
    frames['mixed_num'] = pd.DataFrame({'id': [1, 2], 'val': [1, 2]})
    frames['all_null'] = pd.DataFrame({'id': [1, 2], 'val': [None, None]})
    return frames
