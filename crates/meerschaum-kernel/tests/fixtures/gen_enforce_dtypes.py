"""
Generate parity fixtures for `meerschaum.utils.dataframe.enforce_dtypes`
and the `Pipe.enforce_dtypes` call path (`meerschaum/core/Pipe/_dtypes.py`).
"""

import uuid
from datetime import date
from decimal import Decimal

import pandas as pd
import shapely

import meerschaum as mrsm
from _common import encode_frame, encode, dump, dt, ndt, sample_frames, NY

from meerschaum.utils.dataframe import enforce_dtypes


def input_frames():
    frames = sample_frames()
    frames['str_json'] = pd.DataFrame({'id': [1, 2, 3], 'v': ['{"b": 1, "a": 2}', '[1, 2]', None]})
    frames['str_json_bad'] = pd.DataFrame({'id': [1, 2], 'v': ['{"a": 1}', 'not json']})
    frames['str_uuid'] = pd.DataFrame({
        'id': [1, 2, 3],
        'v': ['12345678-1234-5678-1234-567812345678', '{87654321-4321-8765-4321-876543218765}', None],
    })
    frames['str_uuid_bad'] = pd.DataFrame({'id': [1, 2], 'v': ['12345678-1234-5678-1234-567812345678', 'nope']})
    frames['str_bytes'] = pd.DataFrame({'id': [1, 2, 3], 'v': ['YWJj', '\\x616263', None]})
    frames['str_numeric'] = pd.DataFrame({'id': [1, 2, 3], 'v': ['1.10', '2', None]})
    frames['str_numeric_bad'] = pd.DataFrame({'id': [1, 2], 'v': ['1.10', 'abc']})
    frames['str_datetimes'] = pd.DataFrame({
        'id': [1, 2, 3],
        'v': ['2024-01-01', '2024-01-01T12:00:00+02:00', None],
    })
    frames['str_datetimes_naive'] = pd.DataFrame({'id': [1, 2], 'v': ['2024-01-01 00:00:00', '2024-01-02 12:30:00']})
    frames['str_bools'] = pd.DataFrame({'id': [1, 2, 3, 4], 'v': ['true', 'False', '1', '0']})
    frames['str_dates'] = pd.DataFrame({'id': [1, 2], 'v': ['2024-01-01', '2024-02-03']})
    frames['str_ints'] = pd.DataFrame({'id': [1, 2], 'v': ['1', '2']})
    frames['str_wkt'] = pd.DataFrame({
        'id': [1, 2, 3],
        'v': ['POINT (1 2)', 'LINESTRING (0 0, 1 1)', None],
    })
    frames['str_wkt_z'] = pd.DataFrame({'id': [1, 2], 'v': ['POINT (1 2)', 'POINT Z (1 2 3)']})
    frames['str_wkb_hex'] = pd.DataFrame({
        'id': [1, 2],
        'v': [shapely.to_wkb(shapely.Point(1, 2), hex=True), shapely.to_wkb(shapely.Point(3, 4), hex=True)],
    })
    frames['floats_int_like'] = pd.DataFrame({'id': [1, 2], 'val': [1.0, 2.0]})
    frames['floats_frac'] = pd.DataFrame({'id': [1, 2], 'val': [1.5, 2.0]})
    frames['floats_all_nan'] = pd.DataFrame({'id': [1, 2], 'val': [float('nan'), float('nan')]})
    frames['int32'] = pd.DataFrame({'id': pd.Series([1, 2], dtype='int32'), 'val': pd.Series([1, 2], dtype='int32')})
    frames['obj_mixed_num'] = pd.DataFrame({'id': [1, 2], 'val': pd.Series([1, 2.5], dtype='object')})
    frames['dict_values'] = pd.DataFrame({'id': [1, 2], 'v': [{'a': 1}, {'b': [1, 2]}]})
    frames['dt_ms_utc'] = pd.DataFrame({
        'dt': pd.to_datetime(['2024-01-01 00:00:00.123'], utc=True).astype('datetime64[ms, UTC]'),
        'id': [1],
    })
    frames['dt_ns_naive'] = pd.DataFrame({
        'dt': pd.to_datetime(['2024-01-01 00:00:00.123456789']),
        'id': [1],
    })
    frames['obj_datetimes'] = pd.DataFrame({
        'dt': pd.Series([dt(2024, 1, 1), dt(2024, 1, 2)], dtype='object'),
        'id': [1, 2],
    })
    frames['obj_datetimes_naive'] = pd.DataFrame({
        'dt': pd.Series([ndt(2024, 1, 1), ndt(2024, 1, 2)], dtype='object'),
        'id': [1, 2],
    })
    frames['duplicates'] = pd.DataFrame({'id': [1, 1, 2], 'val': [1, 1, 3]})
    frames['geometry_srid'] = pd.DataFrame({
        'id': [1, 2],
        'geom': [shapely.Point(-80.0, 35.0), shapely.Point(-81.0, 34.0)],
        'geom2': [shapely.Point(1000, 2000), None],
    })
    frames['str_none_like'] = pd.DataFrame({'id': [1, 2, 3], 'v': ['None', 'nan', 'x']})
    return frames


### (frame, dtypes, kwargs)
CASES = [
    ('ints', {'id': 'int', 'val': 'int'}, {}),
    ('ints', {'id': 'int', 'val': 'float'}, {}),
    ('ints', {'id': 'int', 'val': 'str'}, {}),
    ('ints', {'id': 'int', 'val': 'bool'}, {}),
    ('ints', {'id': 'int', 'val': 'numeric'}, {}),
    ('ints', {'id': 'int', 'val': 'numeric[10,2]'}, {}),
    ('ints', {'val': 'int'}, {}),
    ('ints', {'val': 'float'}, {}),
    ('ints', {'id': 'Int64', 'val': 'int64[pyarrow]'}, {}),
    ('ints', {'id': 'int', 'val': 'datetime'}, {}),
    ('ints', {'id': 'int', 'val': 'json'}, {}),
    ('ints', {'id': 'int', 'val': 'uuid'}, {}),
    ('nullable_ints', {'id': 'int', 'val': 'int'}, {}),
    ('nullable_ints', {'id': 'int', 'val': 'float'}, {}),
    ('nullable_ints', {'val': 'float'}, {}),
    ('floats', {'id': 'int', 'val': 'float'}, {}),
    ('floats', {'id': 'int', 'val': 'int'}, {}),
    ('floats', {'val': 'int'}, {}),
    ('floats', {'val': 'int'}, {'coerce_numeric': True}),
    ('floats', {'val': 'int'}, {'coerce_numeric': True, 'explicit_dtypes': {'val': 'int'}}),
    ('floats', {'val': 'numeric'}, {}),
    ('floats', {'id': 'int', 'val': 'numeric[10,2]'}, {}),
    ('floats_int_like', {'val': 'int'}, {}),
    ('floats_int_like', {'id': 'int', 'val': 'int'}, {}),
    ('floats_int_like', {'val': 'int'}, {'coerce_numeric': True}),
    ('floats_frac', {'val': 'int'}, {}),
    ('floats_frac', {'id': 'int', 'val': 'int'}, {}),
    ('floats_frac', {'val': 'int'}, {'coerce_numeric': True}),
    ('floats_frac', {'val': 'int'}, {'coerce_numeric': True, 'explicit_dtypes': {'val': 'numeric'}}),
    ('floats_all_nan', {'val': 'int'}, {'coerce_numeric': True}),
    ('mixed_num', {'val': 'float'}, {'coerce_numeric': True}),
    ('mixed_num', {'val': 'float'}, {'coerce_numeric': False}),
    ('mixed_num', {'val': 'float'}, {'coerce_numeric': True, 'explicit_dtypes': {'val': 'float'}}),
    ('mixed_num', {'id': 'int', 'val': 'numeric'}, {'coerce_numeric': True}),
    ('int32', {'id': 'int', 'val': 'int'}, {}),
    ('int32', {'val': 'int64'}, {}),
    ('obj_mixed_num', {'val': 'float'}, {}),
    ('obj_mixed_num', {'val': 'int'}, {'coerce_numeric': True}),
    ('bools', {'id': 'int', 'flag': 'bool'}, {}),
    ('bools', {'flag': 'bool'}, {}),
    ('bools', {'id': 'int', 'flag': 'int'}, {}),
    ('strings', {'id': 'int', 'name': 'str'}, {}),
    ('strings', {'name': 'str'}, {}),
    ('strings', {'id': 'int', 'name': 'json'}, {}),
    ('strings', {'id': 'int', 'name': 'int'}, {}),
    ('strings', {'name': 'bytes'}, {}),
    ('str_json', {'v': 'json'}, {}),
    ('str_json', {'id': 'int', 'v': 'json'}, {}),
    ('str_json_bad', {'v': 'json'}, {}),
    ('str_json_bad', {'id': 'int', 'v': 'json'}, {}),
    ('str_uuid', {'v': 'uuid'}, {}),
    ('str_uuid', {'id': 'int', 'v': 'uuid'}, {}),
    ('str_uuid_bad', {'v': 'uuid'}, {}),
    ('str_bytes', {'v': 'bytes'}, {}),
    ('str_bytes', {'id': 'int', 'v': 'bytes'}, {}),
    ('str_numeric', {'v': 'numeric'}, {}),
    ('str_numeric', {'v': 'numeric[10,2]'}, {}),
    ('str_numeric', {'id': 'int', 'v': 'numeric[10,3]'}, {}),
    ('str_numeric_bad', {'v': 'numeric'}, {}),
    ('str_numeric_bad', {'id': 'int', 'v': 'numeric[10,2]'}, {}),
    ('str_datetimes', {'v': 'datetime'}, {}),
    ('str_datetimes', {'id': 'int', 'v': 'datetime'}, {}),
    ('str_datetimes', {'v': 'datetime'}, {'strip_timezone': True}),
    ('str_datetimes_naive', {'id': 'int', 'v': 'datetime64[us]'}, {'strip_timezone': True}),
    ('str_datetimes_naive', {'v': 'datetime64[ns]'}, {}),
    ('str_bools', {'id': 'int', 'v': 'bool'}, {}),
    ('str_bools', {'v': 'bool'}, {}),
    ('str_dates', {'id': 'int', 'v': 'date'}, {}),
    ('str_dates', {'v': 'date'}, {}),
    ('str_ints', {'id': 'int', 'v': 'int'}, {}),
    ('str_ints', {'v': 'int'}, {}),
    ('str_wkt', {'v': 'geometry'}, {}),
    ('str_wkt', {'v': 'geometry[Point, 4326]'}, {}),
    ('str_wkt', {'id': 'int', 'v': 'geometry[4326]'}, {}),
    ('str_wkt_z', {'v': 'geometry'}, {}),
    ('str_wkb_hex', {'v': 'geometry[2263]'}, {}),
    ('str_none_like', {'v': 'json'}, {}),
    ('str_none_like', {'v': 'numeric'}, {}),
    ('str_none_like', {'v': 'uuid'}, {}),
    ('datetimes_utc', {'dt': 'datetime', 'id': 'int'}, {}),
    ('datetimes_utc', {'dt': 'datetime'}, {}),
    ('datetimes_utc', {'dt': 'datetime'}, {'strip_timezone': True}),
    ('datetimes_utc', {'dt': 'datetime', 'id': 'int'}, {'strip_timezone': True}),
    ('datetimes_utc', {'dt': 'datetime64[ms, UTC]', 'id': 'int'}, {}),
    ('datetimes_utc', {'dt': 'datetime64[ns]', 'id': 'int'}, {}),
    ('datetimes_utc', {'dt': 'datetime'}, {'coerce_timezone': False}),
    ('datetimes_naive', {'dt': 'datetime', 'id': 'int'}, {}),
    ('datetimes_naive', {'dt': 'datetime'}, {}),
    ('datetimes_naive', {'dt': 'datetime'}, {'strip_timezone': True}),
    ('datetimes_naive', {'dt': 'datetime64[us]', 'id': 'int'}, {'strip_timezone': True}),
    ('datetimes_naive', {'dt': 'datetime'}, {'coerce_timezone': False}),
    ('datetimes_ny', {'dt': 'datetime', 'id': 'int'}, {}),
    ('datetimes_ny', {'dt': 'datetime'}, {}),
    ('datetimes_ny', {'dt': 'datetime'}, {'strip_timezone': True}),
    ('dt_ms_utc', {'dt': 'datetime', 'id': 'int'}, {}),
    ('dt_ms_utc', {'dt': 'datetime'}, {}),
    ('dt_ns_naive', {'dt': 'datetime', 'id': 'int'}, {}),
    ('dt_ns_naive', {'dt': 'datetime'}, {}),
    ('obj_datetimes', {'dt': 'datetime'}, {}),
    ('obj_datetimes', {'dt': 'datetime', 'id': 'int'}, {}),
    ('obj_datetimes_naive', {'dt': 'datetime'}, {}),
    ('obj_datetimes_naive', {'dt': 'datetime'}, {'strip_timezone': True}),
    ('epochs', {'ts': 'int', 'id': 'int'}, {}),
    ('epochs', {'ts': 'int'}, {}),
    ('epochs', {'ts': 'datetime', 'id': 'int'}, {}),
    ### The object path (`int64[pyarrow]` is no native dtype) reads integers and floats as ticks
    ### of the target unit (`Series.astype('datetime64[us, UTC]')`), `coerce_timezone` or not.
    ('ints', {'id': 'int64[pyarrow]', 'val': 'datetime'}, {}),
    ('ints', {'id': 'int64[pyarrow]', 'val': 'datetime'}, {'coerce_timezone': False}),
    ('ints', {'id': 'int64[pyarrow]', 'val': 'datetime'}, {'strip_timezone': True}),
    ('ints', {'id': 'int64[pyarrow]', 'val': 'datetime64[ns, UTC]'}, {'coerce_timezone': False}),
    ('ints', {'id': 'int64[pyarrow]', 'val': 'datetime64[ns]'}, {'coerce_timezone': False}),
    ('floats', {'id': 'int64[pyarrow]', 'val': 'datetime'}, {'coerce_timezone': False}),
    ('epochs', {'ts': 'datetime', 'id': 'int64[pyarrow]'}, {'coerce_timezone': False}),
    ('numerics', {'num': 'numeric'}, {}),
    ('numerics', {'num': 'numeric[10,3]'}, {}),
    ('numerics', {'id': 'int', 'num': 'numeric[10,3]'}, {}),
    ('numerics', {'num': 'float'}, {}),
    ('numerics', {'num': 'int'}, {}),
    ('numerics', {'num': 'str'}, {}),
    ('uuids', {'u': 'uuid'}, {}),
    ('uuids', {'u': 'str'}, {}),
    ('uuids', {'id': 'int', 'u': 'uuid'}, {}),
    ('json', {'meta': 'json'}, {}),
    ('json', {'id': 'int', 'meta': 'json'}, {}),
    ('json', {'meta': 'str'}, {}),
    ('dict_values', {'v': 'json'}, {}),
    ('bytes', {'blob': 'bytes'}, {}),
    ('bytes', {'id': 'int', 'blob': 'bytes'}, {}),
    ('bytes', {'blob': 'str'}, {}),
    ('dates', {'d': 'date'}, {}),
    ('dates', {'id': 'int', 'd': 'date'}, {}),
    ('dates', {'d': 'datetime'}, {}),
    ('geometry', {'geom': 'geometry'}, {}),
    ('geometry', {'geom': 'geometry[Point, 4326]'}, {}),
    ('geometry', {'id': 'int', 'geom': 'geography'}, {}),
    ('geometry_srid', {'geom': 'geometry[4326]', 'geom2': 'geometry[2263]'}, {}),
    ('empty', {'id': 'int', 'val': 'float'}, {}),
    ('empty', {'id': 'int', 'val': 'int'}, {}),
    ('empty', {'val': 'numeric'}, {}),
    ('empty', {'val': 'datetime'}, {}),
    ('no_cols', {'id': 'int'}, {}),
    ('all_null', {'val': 'int'}, {}),
    ('all_null', {'id': 'int', 'val': 'int'}, {}),
    ('all_null', {'val': 'json'}, {}),
    ('all_null', {'val': 'datetime'}, {}),
    ('all_null', {'val': 'numeric'}, {}),
    ('duplicates', {'id': 'int', 'val': 'int'}, {}),
    ('ints', {}, {}),
]

### Pipe.enforce_dtypes cases: (input, configured dtypes, remote dtypes, columns, parameters)
PIPE_CASES = [
    (
        [{'dt': '2024-01-01 00:00:00', 'id': 1, 'val': 1.0}, {'dt': '2024-01-02 00:00:00', 'id': 2, 'val': 2.5}],
        {}, {}, {'datetime': 'dt', 'id': 'id'}, {},
    ),
    (
        [{'dt': '2024-01-01 00:00:00', 'id': 1, 'val': 1.0}],
        {}, {'dt': 'datetime64[us, UTC]', 'id': 'int64[pyarrow]', 'val': 'float64[pyarrow]'},
        {'datetime': 'dt', 'id': 'id'}, {},
    ),
    (
        [{'dt': '2024-01-01 00:00:00', 'id': 1, 'val': 1}],
        {}, {'dt': 'datetime64[us, UTC]', 'id': 'int64[pyarrow]', 'val': 'float64[pyarrow]'},
        {'datetime': 'dt', 'id': 'id'}, {},
    ),
    (
        [{'dt': '2024-01-01 00:00:00', 'id': 1, 'val': 1.5}],
        {}, {'dt': 'datetime64[us, UTC]', 'id': 'int64[pyarrow]', 'val': 'int64[pyarrow]'},
        {'datetime': 'dt', 'id': 'id'}, {},
    ),
    (
        [{'dt': '2024-01-01 00:00:00', 'id': 1, 'val': 1.5}],
        {}, {'dt': 'datetime64[us, UTC]', 'id': 'int64[pyarrow]', 'val': 'int64[pyarrow]'},
        {'datetime': 'dt', 'id': 'id'}, {'mixed_numerics': False},
    ),
    (
        [{'dt': '2024-01-01 00:00:00', 'id': 1, 'val': 1.5}],
        {'val': 'int'}, {'dt': 'datetime64[us, UTC]', 'id': 'int64[pyarrow]', 'val': 'int64[pyarrow]'},
        {'datetime': 'dt', 'id': 'id'}, {},
    ),
    (
        [{'dt': '2024-01-01 00:00:00', 'id': 1, 'meta': {'a': 1}, 'u': '12345678-1234-5678-1234-567812345678'}],
        {'meta': 'json', 'u': 'uuid'}, {}, {'datetime': 'dt', 'id': 'id'}, {},
    ),
    (
        [{'dt': '2024-01-01 00:00:00', 'id': 1}],
        {'dt': 'datetime64[us]'}, {}, {'datetime': 'dt', 'id': 'id'}, {},
    ),
    (
        [{'ts': 1700000000000, 'id': 1, 'val': 1.0}],
        {'ts': 'int'}, {}, {'datetime': 'ts', 'id': 'id'}, {},
    ),
    (
        [{'dt': '2024-01-01T00:00:00+02:00', 'id': 'a', 'val': None}],
        {}, {}, {'datetime': 'dt', 'id': 'id'}, {},
    ),
    (
        [{'dt': '2024-01-01 00:00:00', 'id': 1, 'val': 1.5}],
        {}, {}, {'datetime': 'dt', 'id': 'id'}, {'enforce': False},
    ),
    (
        {'dt': ['2024-01-01 00:00:00', None], 'id': [1, 2]},
        {}, {}, {'datetime': 'dt', 'id': 'id'}, {},
    ),
    (
        [{'id': 1, 'num': '1.10'}, {'id': 2, 'num': 2}],
        {'num': 'numeric'}, {}, {'id': 'id'}, {},
    ),
    (
        [{'id': 1, 'geom': 'POINT (1 2)'}],
        {'geom': 'geometry[Point, 4326]'}, {}, {'id': 'id'}, {},
    ),
]


def run_pipe_case(ix, docs, configured, remote, columns, params):
    pipe = mrsm.Pipe(
        'gen_fixture', f'enforce_{ix}',
        instance='sql:local',
        columns=columns,
        dtypes=configured,
        parameters=params,
    )
    pipe._invalidate_cache(hard=True)

    def infer_dtypes(persist=False, refresh=False, debug=False):
        return dict(remote)

    pipe.infer_dtypes = infer_dtypes
    explicit = pipe.get_dtypes(infer=False)
    inferred = pipe.get_dtypes(infer=True)
    result = pipe.enforce_dtypes(docs if not isinstance(docs, pd.DataFrame) else docs.copy())
    return {
        'explicit_dtypes': explicit,
        'pipe_dtypes': inferred,
        'tzinfo': str(pipe.tzinfo) if pipe.tzinfo is not None else None,
        'df': encode_frame(result),
    }


def main():
    frames = input_frames()
    cases = []
    for frame_name, dtypes, kwargs in CASES:
        df = frames[frame_name]
        entry = {
            'kind': 'enforce_dtypes',
            'frame': frame_name,
            'dtypes': dtypes,
            'kwargs': kwargs,
            'input': encode_frame(df),
        }
        try:
            result = enforce_dtypes(df, dtypes, **kwargs)
            entry['ok'] = encode_frame(result)
            if hasattr(result, 'geometry') and hasattr(result, 'crs'):
                entry['geo_crs'] = result.crs.to_string() if result.crs is not None else None
        except Exception as e:
            entry['err'] = f'{type(e).__name__}: {e}'
        cases.append(entry)

    for ix, (docs, configured, remote, columns, params) in enumerate(PIPE_CASES):
        entry = {
            'kind': 'pipe_enforce_dtypes',
            'docs': [{k: encode(v) for k, v in d.items()} for d in docs] if isinstance(docs, list) else {
                k: [encode(v) for v in vals] for k, vals in docs.items()
            },
            'configured_dtypes': configured,
            'remote_dtypes': remote,
            'columns': columns,
            'parameters': params,
        }
        try:
            entry['ok'] = run_pipe_case(ix, docs, configured, remote, columns, params)
        except Exception as e:
            import traceback
            traceback.print_exc()
            entry['err'] = f'{type(e).__name__}: {e}'
        cases.append(entry)

    dump('enforce_dtypes', cases)


if __name__ == '__main__':
    main()
