"""
Generate parity fixtures for serialization helpers in `meerschaum.utils.dtypes`
(bytes, datetimes, dates, JSON values, decimals, UUIDs and geometry).
"""

import struct
import uuid
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal

import shapely

from _common import encode, dump, catch, dt, ndt, sample_frames  # noqa: F401

from meerschaum.utils.dtypes import (
    serialize_bytes,
    deserialize_bytes_string,
    encode_bytes_for_bytea,
    serialize_datetime,
    serialize_date,
    json_serialize_value,
    geometry_is_wkt,
    geometry_is_gpkg,
    gpkg_wkb_to_wkb,
    attempt_cast_to_geometry,
    serialize_geometry,
    deserialize_geometry,
)
from meerschaum.utils.dataframe import to_polars, to_json


def res(r):
    if 'ok' not in r:
        return r
    value = r['ok']
    if isinstance(value, bytes):
        return {'ok': {'t': 'bytes_hex', 'v': value.hex()}}
    if isinstance(value, tuple):
        return {'ok': [encode(v) if not isinstance(v, bytes) else {'t': 'bytes_hex', 'v': v.hex()} for v in value]}
    return {'ok': encode(value)}


GEOMS_WKT = [
    'POINT (1 2)',
    'POINT (1.123456789 -2.5)',
    'POINT Z (1 2 3)',
    'POINT EMPTY',
    'LINESTRING (0 0, 1 1, 2 0)',
    'LINESTRING EMPTY',
    'POLYGON ((0 0, 1 0, 1 1, 0 0))',
    'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 2 1, 2 2, 1 1))',
    'MULTIPOINT ((0 0), (1 1))',
    'MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))',
    'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((5 5, 6 5, 6 6, 5 5)))',
    'GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))',
    'POINT (100000000000 0.0000001)',
    'POINT (-1.5 1e20)',
    'POINT (0.00012345 123456.1234565)',
    'LINESTRING Z (0 0 0, 1 1 1)',
    'POINT (-0 0)',
    'POINT (0.5 2.5000005)',
]

BYTES_INPUTS = [
    b'', b'abc', b'\x00\x01\xff', None, float('nan'), 'None',
]

BYTES_STRINGS = [
    'YWJj', '\\x616263', '616263', '\\xZZ', 'not base64!', 'YWJ', '', None, 'None', 'AAH/',
]

DATETIMES = [
    dt(2024, 1, 1),
    ndt(2024, 1, 1, 12, 30, 15, 123456),
    datetime(2024, 6, 1, 8, 0, tzinfo=timezone(timedelta(hours=-4))),
    ndt(1, 1, 1),
    ndt(9999, 12, 31, 23, 59, 59, 999999),
]

JSON_VALUES = [
    None, 1, 1.5, 'x', True, Decimal('1.10'), Decimal('NaN'), b'abc',
    uuid.UUID('12345678-1234-5678-1234-567812345678'),
    dt(2024, 1, 1), ndt(2024, 1, 1), date(2024, 1, 2),
    {'b': 1, 'a': [1, 2]}, [1, 'x', None], {'d': 1.5, 'nested': {'z': [1, None]}},
    (1, 2), float('nan'), 'NaN', '', shapely.Point(1, 2),
]


def gpkg(geom, srid, envelope=0):
    wkb = shapely.to_wkb(geom, hex=False, byte_order=1)
    flags = (1 & 0x01) | ((envelope & 0x07) << 1)
    header = struct.pack('<ccBBi', b'G', b'P', 0, flags, srid)
    env = b''
    if envelope == 1:
        minx, miny, maxx, maxy = geom.bounds
        env = struct.pack('<dddd', minx, maxx, miny, maxy)
    return header + env + wkb


def main():
    cases = []
    for value in BYTES_INPUTS:
        cases.append({'fn': 'serialize_bytes', 'args': [encode(value)], **res(catch(serialize_bytes, value))})
        cases.append({'fn': 'encode_bytes_for_bytea', 'args': [encode(value)], **res(catch(encode_bytes_for_bytea, value))})
        cases.append({
            'fn': 'encode_bytes_for_bytea_noprefix',
            'args': [encode(value)],
            **res(catch(encode_bytes_for_bytea, value, with_prefix=False)),
        })
    for value in BYTES_STRINGS:
        cases.append({'fn': 'deserialize_bytes_string', 'args': [encode(value)], **res(catch(deserialize_bytes_string, value))})
        cases.append({
            'fn': 'deserialize_bytes_string_hex',
            'args': [encode(value)],
            **res(catch(deserialize_bytes_string, value, force_hex=True)),
        })

    for value in DATETIMES:
        cases.append({'fn': 'serialize_datetime', 'args': [encode(value)], **res(catch(serialize_datetime, value))})
    for value in [date(2024, 1, 2), date(1, 1, 1), None, 'x']:
        cases.append({'fn': 'serialize_date', 'args': [encode(value)], **res(catch(serialize_date, value))})

    for value in JSON_VALUES:
        cases.append({'fn': 'json_serialize_value', 'args': [encode(value)], **res(catch(json_serialize_value, value))})

    ### Geometry.
    for wkt in GEOMS_WKT:
        geom = shapely.from_wkt(wkt)
        wkb_hex = shapely.to_wkb(geom, hex=True)
        enc = encode(geom)
        for fmt in ('wkb_hex', 'wkb', 'wkt', 'geojson', 'gpkg_wkb'):
            for srid in (None, 4326):
                cases.append({
                    'fn': 'serialize_geometry',
                    'args': [enc, fmt, srid],
                    **res(catch(serialize_geometry, geom, fmt, srid)),
                })
        geom_srid = shapely.set_srid(geom, 4326)
        cases.append({
            'fn': 'serialize_geometry_with_srid',
            'args': [enc, 4326],
            **res(catch(serialize_geometry, geom_srid, 'wkb_hex')),
        })
        cases.append({'fn': 'geometry_is_wkt', 'args': [wkt], 'ok': geometry_is_wkt(wkt)})
        cases.append({'fn': 'geometry_is_wkt', 'args': [wkb_hex], 'ok': geometry_is_wkt(wkb_hex)})
        cases.append({'fn': 'attempt_cast_to_geometry', 'args': [wkt], **res(catch(attempt_cast_to_geometry, wkt))})
        cases.append({'fn': 'attempt_cast_to_geometry', 'args': [wkb_hex], **res(catch(attempt_cast_to_geometry, wkb_hex))})
        wkb_bytes = shapely.to_wkb(geom, hex=False)
        cases.append({
            'fn': 'attempt_cast_to_geometry',
            'args': [encode(wkb_bytes)],
            **res(catch(attempt_cast_to_geometry, wkb_bytes)),
        })
        cases.append({
            'fn': 'deserialize_geometry',
            'args': [wkb_hex],
            **res(catch(deserialize_geometry, wkb_hex)),
        })
        if not geom.is_empty:
            for env in (0, 1):
                gp = gpkg(geom, 4326, env)
                cases.append({'fn': 'geometry_is_gpkg', 'args': [encode(gp)], 'ok': geometry_is_gpkg(gp)})
                cases.append({'fn': 'gpkg_wkb_to_wkb', 'args': [encode(gp)], **res(catch(gpkg_wkb_to_wkb, gp))})
                cases.append({'fn': 'attempt_cast_to_geometry', 'args': [encode(gp)], **res(catch(attempt_cast_to_geometry, gp))})
            geojson = shapely.geometry.mapping(geom)
            geojson = {'type': geojson['type'], **{k: v for k, v in geojson.items() if k != 'type'}}
            import json as _json
            geojson = _json.loads(_json.dumps(geojson))
            cases.append({
                'fn': 'attempt_cast_to_geometry',
                'args': [encode(geojson)],
                **res(catch(attempt_cast_to_geometry, geojson)),
            })

    for value in ['POINT(1 2)', '  point (1 2) ', 'POINT 1 2', 'abc', 'ABCD', 'ABC', '', 'xyz', None, 5, b'GP']:
        cases.append({'fn': 'geometry_is_wkt', 'args': [encode(value)], 'ok': geometry_is_wkt(value)})
        cases.append({'fn': 'attempt_cast_to_geometry', 'args': [encode(value)], **res(catch(attempt_cast_to_geometry, value))})
    for value in [b'GP', b'G', b'', b'GPxx', 'GP', None]:
        cases.append({'fn': 'geometry_is_gpkg', 'args': [encode(value)], 'ok': geometry_is_gpkg(value)})

    ### `to_polars` GeoArrow / JSON metadata.
    import pandas as pd
    df = pd.DataFrame({
        'id': [1, 2],
        'geom': [shapely.Point(1, 2), None],
        'meta': [{'b': 1, 'a': 2}, None],
    })
    for srid in (0, 4326, 'ESRI:102003'):
        pl_df = to_polars(df, geometry_cols_types_srids={'geom': ('Point', srid)}, json_cols=['meta'])
        arrow_table = pl_df.to_arrow()
        fields = {}
        for field in arrow_table.schema:
            fields[field.name] = {
                'type': str(field.type),
                'metadata': {
                    k.decode(): v.decode()
                    for k, v in (field.metadata or {}).items()
                },
                'extension': getattr(field.type, 'extension_name', None),
                'ext_metadata': (
                    field.type.__arrow_ext_serialize__().decode()
                    if hasattr(field.type, '__arrow_ext_serialize__') else None
                ),
                'polars_dtype': str(pl_df.schema[field.name]),
            }
        cases.append({
            'fn': 'to_polars_geoarrow',
            'args': [encode(srid)],
            'ok': {
                'fields': fields,
                'geom_wkb_hex': [v.hex() if v is not None else None for v in pl_df['geom'].ext.storage().to_list()]
                if hasattr(pl_df['geom'], 'ext') else None,
                'meta': pl_df['meta'].ext.storage().to_list() if hasattr(pl_df['meta'], 'ext') else pl_df['meta'].to_list(),
            },
        })

    ### `to_json` (geometry formats + special types).
    frames = sample_frames()
    for name in ('numerics', 'uuids', 'bytes', 'dates', 'geometry', 'json', 'datetimes_utc', 'datetimes_naive'):
        for fmt in ('geojson', 'wkb_hex', 'wkt'):
            if name != 'geometry' and fmt != 'geojson':
                continue
            cases.append({
                'fn': 'to_json',
                'args': [name, fmt],
                **catch(to_json, frames[name], geometry_format=fmt),
            })

    dump('serialize', cases)


if __name__ == '__main__':
    main()
