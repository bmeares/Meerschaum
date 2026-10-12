"""
Generate parity fixtures for the dtype-detection helpers in `meerschaum.utils.dataframe`.
"""

import uuid
from datetime import datetime, date, timezone, timedelta
from decimal import Decimal

import pandas as pd
import shapely

from _common import encode_frame, dump, catch, dt, ndt, sample_frames, NY

from meerschaum.utils.dataframe import (
    get_unhashable_cols,
    get_json_cols,
    get_numeric_cols,
    get_bool_cols,
    get_uuid_cols,
    get_datetime_cols,
    get_datetime_cols_types,
    get_date_cols,
    get_bytes_cols,
    get_geometry_cols,
    get_geometry_cols_types,
    get_special_cols,
)


def extra_frames():
    import geopandas as gpd
    frames = {}
    frames['first_row_null'] = pd.DataFrame({
        'meta': [None, {'a': 1}],
        'num': [None, Decimal('1.5')],
        'u': [None, uuid.UUID('12345678-1234-5678-1234-567812345678')],
        'b': [None, b'x'],
    })
    frames['object_datetimes'] = pd.DataFrame({
        'aware': pd.Series([dt(2024, 1, 1), None], dtype='object'),
        'naive': pd.Series([ndt(2024, 1, 1), None], dtype='object'),
        'offset': pd.Series([datetime(2024, 6, 1, 8, tzinfo=timezone(timedelta(hours=-4))), None], dtype='object'),
        'd': [date(2024, 1, 1), None],
    })
    frames['geo_types'] = pd.DataFrame({
        'pts': [shapely.Point(0, 0), shapely.Point(1, 1)],
        'mixed': [shapely.Point(0, 0), shapely.LineString([(0, 0), (1, 1)])],
        'z': [shapely.Point(0, 0, 1), None],
    })
    gdf = gpd.GeoDataFrame(
        {'id': [1, 2], 'geom': [shapely.Point(0, 0), shapely.Point(1, 1)]},
        geometry='geom',
        crs=4326,
    )
    frames['geodataframe_4326'] = gdf
    gdf2 = gpd.GeoDataFrame(
        {'id': [1], 'geom': [shapely.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])]},
        geometry='geom',
        crs='EPSG:2263',
    )
    frames['geodataframe_2263'] = gdf2
    frames['arrow_types'] = pd.DataFrame({
        'b': pd.Series([b'a', None], dtype='binary[pyarrow]'),
        'd': pd.Series([date(2024, 1, 1), None], dtype='date32[day][pyarrow]'),
        'ts': pd.Series([dt(2024, 1, 1), None], dtype='timestamp[ms, tz=UTC][pyarrow]'),
        'flag': pd.Series([True, None], dtype='bool[pyarrow]'),
        'i': pd.Series([1, None], dtype='int64[pyarrow]'),
    })
    frames['dt_units'] = pd.DataFrame({
        'ns_utc': pd.to_datetime(['2024-01-01'], utc=True).astype('datetime64[ns, UTC]'),
        'ms_naive': pd.to_datetime(['2024-01-01']).astype('datetime64[ms]'),
        's_ny': pd.to_datetime(['2024-01-01']).tz_localize(NY).astype('datetime64[s, America/New_York]'),
    })
    frames['first_row_json'] = pd.DataFrame({'a': [[1], None], 'b': [None, {'x': 1}], 'c': [1, 2]})
    return frames


def main():
    frames = {**sample_frames(), **extra_frames()}
    cases = []
    for name, df in frames.items():
        results = {
            'get_unhashable_cols': catch(get_unhashable_cols, df),
            'get_json_cols': catch(get_json_cols, df),
            'get_numeric_cols': catch(get_numeric_cols, df),
            'get_bool_cols': catch(get_bool_cols, df),
            'get_uuid_cols': catch(get_uuid_cols, df),
            'get_datetime_cols': catch(get_datetime_cols, df),
            'get_datetime_cols_aware': catch(get_datetime_cols, df, timezone_naive=False),
            'get_datetime_cols_naive': catch(get_datetime_cols, df, timezone_aware=False),
            'get_datetime_cols_tz_precision': catch(
                lambda: {k: list(v) for k, v in get_datetime_cols(df, with_tz_precision=True).items()}
            ),
            'get_datetime_cols_types': catch(get_datetime_cols_types, df),
            'get_date_cols': catch(get_date_cols, df),
            'get_bytes_cols': catch(get_bytes_cols, df),
            'get_geometry_cols': catch(get_geometry_cols, df),
            'get_geometry_cols_types_srids': catch(
                lambda: {k: list(v) for k, v in get_geometry_cols(df, with_types_srids=True).items()}
            ),
            'get_geometry_cols_types': catch(get_geometry_cols_types, df),
            'get_special_cols': catch(get_special_cols, df),
        }
        cases.append({'name': name, 'df': encode_frame(df), 'results': results})
    dump('detect', cases)


if __name__ == '__main__':
    main()
