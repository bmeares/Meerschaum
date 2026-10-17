"""
Generate parity fixtures for the scalar dtype helpers in `meerschaum.utils.dtypes`.
"""

import itertools
import uuid
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal

from _common import encode, dump, catch, dt, ndt, UTC  # noqa: F401

from meerschaum.utils.dtypes import (
    to_pandas_dtype,
    are_dtypes_equal,
    is_dtype_numeric,
    is_dtype_special,
    get_geometry_type_srid,
    value_is_null,
    none_if_null,
    attempt_cast_to_numeric,
    attempt_cast_to_uuid,
    attempt_cast_to_bytes,
    quantize_decimal,
    serialize_decimal,
    coerce_timezone,
    to_datetime,
    datetime_to_int,
    round_time,
    get_next_precision_unit,
)
from meerschaum.utils.dtypes.sql import get_numeric_precision_scale

DTYPES = [
    'int', 'int8', 'int16', 'int32', 'int64', 'Int64', 'Int32', 'uint8', 'uint64', 'UInt16',
    'float', 'float32', 'float64', 'Float64', 'double', 'double[pyarrow]',
    'str', 'string', 'string[pyarrow]', 'large_string[pyarrow]', 'object',
    'bool', 'boolean', 'bool[pyarrow]',
    'datetime', 'datetime64[ns]', 'datetime64[us]', 'datetime64[ms, UTC]', 'datetime64[ns, UTC]',
    'datetime64[us, UTC]', 'datetime64[s, America/New_York]',
    'timestamp[us, tz=UTC][pyarrow]', 'timestamp[ms][pyarrow]',
    'date', 'date32[day][pyarrow]', 'date32[pyarrow]', 'date64[pyarrow]',
    'json', 'JSON', 'jsonl',
    'numeric', 'numeric[10,2]', 'numeric[38, 10]', 'decimal', 'Decimal', 'number',
    'decimal128(10, 2)[pyarrow]',
    'uuid', 'UUID', 'guid',
    'bytes', 'binary', 'blob', 'bytea', 'binary[pyarrow]', 'large_binary[pyarrow]',
    'geometry', 'geometry[Point, 4326]', 'geometry[4326]', 'geography', 'geography[POINT]', 'geom',
    'int64[pyarrow]', 'int32[pyarrow]', 'category', 'timedelta64[ns]', 'day',
    'not_a_dtype', '',
]

SQL_DTYPES = [
    'TEXT', 'INTEGER', 'BIGINT', 'INT', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMPTZ', 'TIMESTAMP',
    'DOUBLE PRECISION', 'DOUBLE', 'NUMERIC(10, 2)', 'NUMERIC', 'DECIMAL(28,10)', 'NUMBER(10,2)',
    'GEOMETRY(POINT, 4326)', 'GEOMETRY', 'GEOGRAPHY(POINT, 4326)', 'VARCHAR(255)', 'NVARCHAR(MAX)',
    'DATETIME2(3)', 'DATETIMEOFFSET', 'JSONB', 'BYTEA', 'BIT', 'DATE', 'TIME', 'CLOB', 'FLOAT',
    'UNIQUEIDENTIFIER', 'VARBINARY(MAX)', 'TINYINT', 'SMALLINT', 'REAL', 'BOOLEAN', 'MEDIUMTEXT',
    'LONGBLOB', 'Int64', 'TIMESTAMP(6) WITH TIME ZONE', 'CHAR(36)', 'NCHAR', 'XML',
]

GEOMETRY_DTYPES = [
    'geometry', 'geometry[]', 'geometry[Point, 0]', 'geometry[0, Point]', 'geometry[0]',
    'geometry[MULTILINESTRING, 4326]', 'geography', 'geography[POINT]',
    'geometry[POINT, ESRI:102003]', 'geometry[EPSG:4326]', 'geometry(Point, 4326)',
    'geometry(MultiPolygon,2263)', 'geometry[srid=4326]', 'geometry[type=Point, srid=3857]',
    'geometry[Polygon]', 'geometry[4326.0]', 'geometry[PointZ, 4326]',
]

NUMERIC_DTYPES = [
    'numeric', 'numeric[10,2]', 'numeric[10, 2]', 'numeric(28,10)', 'NUMERIC(10, 2)',
    'numeric[10]', 'numeric[a,b]', 'numeric[10,2,3]', 'decimal(5,2)', '', None,
]

VALUES = [
    None, '', 'None', 'none', 'NaN', 'nan', 'NA', 'NaT', 'natz', '<NA>', 'null', 'x',
    0, 1, -5, 1.5, float('nan'), float('inf'), True, False,
    Decimal('1.10'), Decimal('NaN'), '1.10', '1e3', '1E-7', '0.1', ' 2 ', 'abc',
    '12345678-1234-5678-1234-567812345678', '{12345678-1234-5678-1234-567812345678}',
    '12345678123456781234567812345678', 'urn:uuid:12345678-1234-5678-1234-567812345678',
    uuid.UUID('12345678-1234-5678-1234-567812345678'),
    b'abc', 'YWJj', '\\x616263', 'not base64!', 'YWJ',
    123456789.123456789, 1e20, 1e-7, -0.0, 2**53 + 1,
]

NUMERIC_CASTS = [
    (Decimal('1.005'), True, 10, 2),
    (Decimal('1.004'), True, 10, 2),
    (Decimal('-1.005'), True, 10, 2),
    (Decimal('123456.789'), True, 5, 2),
    (Decimal('12.3'), True, 5, 2),
    ('1.23456', True, 10, 4),
    (1.5, True, 10, 0),
    (2.5, True, 10, 1),
    ('7', True, 10, 3),
    ('abc', True, 10, 2),
    (None, True, 10, 2),
    (Decimal('1.10'), False, None, None),
    (Decimal('1.10'), True, None, None),
    (3, True, 10, 2),
    (Decimal('99.995'), True, 4, 2),
]

DATETIMES = [
    dt(2024, 1, 1),
    ndt(2024, 1, 1, 12, 30, 15, 123456),
    datetime(2024, 6, 1, 8, 0, tzinfo=timezone(timedelta(hours=-4))),
    datetime(2024, 6, 1, 8, 0, tzinfo=timezone(timedelta(hours=5, minutes=30))),
    '2024-01-01',
    '2024-01-01 00:00:00',
    '2024-01-01T12:00:00Z',
    '2024-01-01T12:00:00+02:00',
    '2024-01-01T12:00:00.123456-05:00',
    '2024-01-01T12:00:00.123',
    '2024-03-10T02:30:00',
    'not a date',
    5,
    None,
]

ROUND_TIMES = [
    (ndt(2022, 1, 1, 12, 15, 57, 200), None, 'down'),
    (ndt(2022, 1, 1, 12, 15, 57, 200), None, 'up'),
    (ndt(2022, 1, 1, 12, 15, 57, 200), timedelta(hours=1), 'down'),
    (ndt(2022, 1, 1, 12, 15, 57, 200), timedelta(hours=1), 'closest'),
    (ndt(2022, 1, 1, 12, 45, 57, 200), timedelta(hours=1), 'closest'),
    (ndt(2022, 1, 1, 12, 30), timedelta(hours=1), 'closest'),
    (dt(2022, 1, 1, 12, 15, 57, 200), timedelta(minutes=15), 'down'),
    (dt(2022, 1, 1, 12, 15, 0), timedelta(minutes=15), 'up'),
    (ndt(2022, 1, 1, 12, 15, 0), timedelta(0), 'down'),
    (ndt(2022, 1, 1, 12, 15, 0, 1), timedelta(seconds=1), 'up'),
    (ndt(1, 1, 1, 0, 0, 30), None, 'down'),
    (ndt(2022, 1, 1, 12, 15, 57, 200), timedelta(days=7), 'down'),
]

UNITS = ['ns', 'us', 'ms', 's', 'sec', 'm', 'min', 'h', 'hr', 'd', 'D',
         'nanosecond', 'microsecond', 'millisecond', 'second', 'minute', 'hour', 'day', 'week']


def enc_result(res):
    if 'ok' in res:
        return {'ok': encode(res['ok'])}
    return res


def main():
    cases = []

    for dtype in DTYPES + SQL_DTYPES:
        cases.append({'fn': 'to_pandas_dtype', 'args': [dtype], **enc_result(catch(to_pandas_dtype, dtype))})

    for ldtype, rdtype in itertools.product(DTYPES, DTYPES):
        cases.append({
            'fn': 'are_dtypes_equal',
            'args': [ldtype, rdtype],
            'ok': are_dtypes_equal(ldtype, rdtype),
        })

    dict_pairs = [
        ({'a': 'int64', 'b': 'string'}, {'a': 'Int64', 'b': 'str'}),
        ({'a': 'int64', 'b': 'string'}, {'a': 'Int64', 'c': 'str'}),
        ({'a': 'int64'}, {'a': 'int64', 'b': 'float64'}),
        ({'a': 'int64', 'b': 'float64'}, {'a': 'int64'}),
        ({'a': 'int64'}, {'a': 'float64'}),
        ({}, {}),
    ]
    for ldtype, rdtype in dict_pairs:
        cases.append({'fn': 'are_dtypes_equal_dict', 'args': [ldtype, rdtype], 'ok': are_dtypes_equal(ldtype, rdtype)})

    for dtype in DTYPES + SQL_DTYPES:
        cases.append({'fn': 'is_dtype_numeric', 'args': [dtype], 'ok': is_dtype_numeric(dtype)})
        cases.append({'fn': 'is_dtype_special', 'args': [dtype], 'ok': is_dtype_special(dtype)})

    for dtype in GEOMETRY_DTYPES:
        cases.append({'fn': 'get_geometry_type_srid', 'args': [dtype], **catch(lambda: list(get_geometry_type_srid(dtype)))})
        cases.append({
            'fn': 'get_geometry_type_srid_4326',
            'args': [dtype],
            **catch(lambda: list(get_geometry_type_srid(dtype, default_srid=4326))),
        })

    for dtype in NUMERIC_DTYPES:
        cases.append({
            'fn': 'get_numeric_precision_scale',
            'args': [dtype],
            'ok': list(get_numeric_precision_scale(None, dtype)),
        })

    for value in VALUES:
        enc = encode(value)
        cases.append({'fn': 'value_is_null', 'args': [enc], 'ok': value_is_null(value)})
        cases.append({'fn': 'none_if_null', 'args': [enc], 'ok': encode(none_if_null(value))})
        cases.append({'fn': 'attempt_cast_to_numeric', 'args': [enc], **enc_result(catch(attempt_cast_to_numeric, value))})
        cases.append({'fn': 'attempt_cast_to_uuid', 'args': [enc], **enc_result(catch(attempt_cast_to_uuid, value))})
        cases.append({'fn': 'attempt_cast_to_bytes', 'args': [enc], **enc_result(catch(attempt_cast_to_bytes, value))})
        cases.append({'fn': 'serialize_decimal', 'args': [enc], **enc_result(catch(serialize_decimal, value))})

    for value, quantize, precision, scale in NUMERIC_CASTS:
        cases.append({
            'fn': 'attempt_cast_to_numeric_q',
            'args': [encode(value), quantize, precision, scale],
            **enc_result(catch(attempt_cast_to_numeric, value, quantize, precision, scale)),
        })
        if isinstance(value, Decimal) and precision and scale is not None:
            cases.append({
                'fn': 'quantize_decimal',
                'args': [encode(value), precision, scale],
                **enc_result(catch(quantize_decimal, value, precision, scale)),
            })
            cases.append({
                'fn': 'serialize_decimal_q',
                'args': [encode(value), precision, scale],
                **enc_result(catch(serialize_decimal, value, True, precision, scale)),
            })

    for value in DATETIMES:
        enc = encode(value)
        cases.append({'fn': 'coerce_timezone', 'args': [enc], **enc_result(catch(coerce_timezone, value))})
        cases.append({
            'fn': 'coerce_timezone_strip',
            'args': [enc],
            **enc_result(catch(coerce_timezone, value, strip_utc=True)),
        })
        if value is None or isinstance(value, int):
            continue
        cases.append({
            'fn': 'to_datetime_py',
            'args': [enc],
            **enc_result(catch(to_datetime, value, as_pydatetime=True)),
        })
        cases.append({
            'fn': 'to_datetime_py_nocoerce',
            'args': [enc],
            **enc_result(catch(to_datetime, value, as_pydatetime=True, coerce_utc=False)),
        })

    for value in DATETIMES[:4]:
        for unit in UNITS:
            cases.append({
                'fn': 'datetime_to_int',
                'args': [encode(value), unit],
                **enc_result(catch(datetime_to_int, value, unit)),
            })
    cases.append({'fn': 'datetime_to_int', 'args': [encode(12345), 'ms'], 'ok': encode(datetime_to_int(12345, 'ms'))})

    for value, delta, to in ROUND_TIMES:
        cases.append({
            'fn': 'round_time',
            'args': [encode(value), (delta.total_seconds() * 1_000_000 if delta is not None else None), to],
            **enc_result(catch(round_time, value, delta, to)),
        })

    for unit in UNITS:
        cases.append({'fn': 'get_next_precision_unit', 'args': [unit, True], **catch(get_next_precision_unit, unit)})
        cases.append({
            'fn': 'get_next_precision_unit',
            'args': [unit, False],
            **catch(get_next_precision_unit, unit, decrease=False),
        })

    dump('dtypes', cases)


if __name__ == '__main__':
    main()
