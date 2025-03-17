#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for working with data types.
"""

import traceback
import json
import uuid
from datetime import timezone, datetime
from decimal import Decimal, Context, InvalidOperation, ROUND_HALF_UP

import meerschaum as mrsm
from meerschaum.utils.typing import Dict, Union, Any, Optional, Tuple
from meerschaum.utils.warnings import warn

MRSM_ALIAS_DTYPES: Dict[str, str] = {
    'decimal': 'numeric',
    'Decimal': 'numeric',
    'number': 'numeric',
    'jsonl': 'json',
    'JSON': 'json',
    'binary': 'bytes',
    'blob': 'bytes',
    'varbinary': 'bytes',
    'bytea': 'bytes',
    'guid': 'uuid',
    'UUID': 'uuid',
    'geom': 'geometry',
    'geog': 'geography',
}
MRSM_PD_DTYPES: Dict[Union[str, None], str] = {
    'json': 'object',
    'numeric': 'object',
    'geometry': 'object',
    'geography': 'object',
    'uuid': 'object',
    'datetime': 'datetime64[ns, UTC]',
    'bool': 'bool[pyarrow]',
    'int': 'Int64',
    'int8': 'Int8',
    'int16': 'Int16',
    'int32': 'Int32',
    'int64': 'Int64',
    'str': 'string[python]',
    'bytes': 'object',
    None: 'object',
}


def to_pandas_dtype(dtype: str) -> str:
    """
    Cast a supported Meerschaum dtype to a Pandas dtype.
    """
    known_dtype = MRSM_PD_DTYPES.get(dtype, None)
    if known_dtype is not None:
        return known_dtype

    alias_dtype = MRSM_ALIAS_DTYPES.get(dtype, None)
    if alias_dtype is not None:
        return MRSM_PD_DTYPES[alias_dtype]

    if dtype.startswith('numeric'):
        return MRSM_PD_DTYPES['numeric']

    if dtype.startswith('geometry'):
        return MRSM_PD_DTYPES['geometry']

    if dtype.startswith('geography'):
        return MRSM_PD_DTYPES['geography']

    ### NOTE: Kind of a hack, but if the first word of the given dtype is in all caps,
    ### treat it as a SQL db type.
    if dtype.split(' ')[0].isupper():
        from meerschaum.utils.dtypes.sql import get_pd_type_from_db_type
        return get_pd_type_from_db_type(dtype)

    from meerschaum.utils.packages import attempt_import
    _ = attempt_import('pyarrow', lazy=False)
    pandas = attempt_import('pandas', lazy=False)

    try:
        return str(pandas.api.types.pandas_dtype(dtype))
    except Exception:
        warn(
            f"Invalid dtype '{dtype}', will use 'object' instead:\n"
            + f"{traceback.format_exc()}",
            stack=False,
        )
    return 'object'


def are_dtypes_equal(
    ldtype: Union[str, Dict[str, str]],
    rdtype: Union[str, Dict[str, str]],
) -> bool:
    """
    Determine whether two dtype strings may be considered
    equivalent to avoid unnecessary conversions.

    Parameters
    ----------
    ldtype: Union[str, Dict[str, str]]
        The left dtype to compare.
        May also provide a dtypes dictionary.

    rdtype: Union[str, Dict[str, str]]
        The right dtype to compare.
        May also provide a dtypes dictionary.

    Returns
    -------
    A `bool` indicating whether the two dtypes are to be considered equivalent.
    """
    if isinstance(ldtype, dict) and isinstance(rdtype, dict):
        lkeys = sorted([str(k) for k in ldtype.keys()])
        rkeys = sorted([str(k) for k in rdtype.keys()])
        for lkey, rkey in zip(lkeys, rkeys):
            if lkey != rkey:
                return False
            ltype = ldtype[lkey]
            rtype = rdtype[rkey]
            if not are_dtypes_equal(ltype, rtype):
                return False
        return True

    try:
        if ldtype == rdtype:
            return True
    except Exception:
        warn(f"Exception when comparing dtypes, returning False:\n{traceback.format_exc()}")
        return False

    ### Sometimes pandas dtype objects are passed.
    ldtype = str(ldtype).split('[', maxsplit=1)[0]
    rdtype = str(rdtype).split('[', maxsplit=1)[0]

    if ldtype in MRSM_ALIAS_DTYPES:
        ldtype = MRSM_ALIAS_DTYPES[ldtype]

    if rdtype in MRSM_ALIAS_DTYPES:
        rdtype = MRSM_ALIAS_DTYPES[rdtype]

    json_dtypes = ('json', 'object')
    if ldtype in json_dtypes and rdtype in json_dtypes:
        return True

    numeric_dtypes = ('numeric', 'object')
    if ldtype in numeric_dtypes and rdtype in numeric_dtypes:
        return True

    uuid_dtypes = ('uuid', 'object')
    if ldtype in uuid_dtypes and rdtype in uuid_dtypes:
        return True

    bytes_dtypes = ('bytes', 'object')
    if ldtype in bytes_dtypes and rdtype in bytes_dtypes:
        return True

    geometry_dtypes = ('geometry', 'object', 'geography')
    if ldtype in geometry_dtypes and rdtype in geometry_dtypes:
        return True

    if ldtype.lower() == rdtype.lower():
        return True

    datetime_dtypes = ('datetime', 'timestamp')
    ldtype_found_dt_prefix = False
    rdtype_found_dt_prefix = False
    for dt_prefix in datetime_dtypes:
        ldtype_found_dt_prefix = (dt_prefix in ldtype.lower()) or ldtype_found_dt_prefix
        rdtype_found_dt_prefix = (dt_prefix in rdtype.lower()) or rdtype_found_dt_prefix
    if ldtype_found_dt_prefix and rdtype_found_dt_prefix:
        return True

    string_dtypes = ('str', 'string', 'object')
    if ldtype in string_dtypes and rdtype in string_dtypes:
        return True

    int_dtypes = ('int', 'int64', 'int32', 'int16', 'int8')
    if ldtype.lower() in int_dtypes and rdtype.lower() in int_dtypes:
        return True

    float_dtypes = ('float', 'float64', 'float32', 'float16', 'float128', 'double')
    if ldtype.lower() in float_dtypes and rdtype.lower() in float_dtypes:
        return True

    bool_dtypes = ('bool', 'boolean')
    if ldtype in bool_dtypes and rdtype in bool_dtypes:
        return True

    return False


def is_dtype_numeric(dtype: str) -> bool:
    """
    Determine whether a given `dtype` string
    should be considered compatible with the Meerschaum dtype `numeric`.

    Parameters
    ----------
    dtype: str
        The pandas-like dtype string.

    Returns
    -------
    A bool indicating the dtype is compatible with `numeric`.
    """
    dtype_lower = dtype.lower()

    acceptable_substrings = ('numeric', 'float', 'double', 'int')
    for substring in acceptable_substrings:
        if substring in dtype_lower:
            return True

    return False


def attempt_cast_to_numeric(
    value: Any,
    quantize: bool = False,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
)-> Any:
    """
    Given a value, attempt to coerce it into a numeric (Decimal).

    Parameters
    ----------
    value: Any
        The value to be cast to a Decimal.

    quantize: bool, default False
        If `True`, quantize the decimal to the specified precision and scale.

    precision: Optional[int], default None
        If `quantize` is `True`, use this precision.

    scale: Optional[int], default None
        If `quantize` is `True`, use this scale.

    Returns
    -------
    A `Decimal` if possible, or `value`.
    """
    if isinstance(value, Decimal):
        if quantize and precision and scale:
            return quantize_decimal(value, precision, scale)
        return value
    try:
        if value_is_null(value):
            return Decimal('NaN')

        dec = Decimal(str(value))
        if not quantize or not precision or not scale:
            return dec
        return quantize_decimal(dec, precision, scale)
    except Exception:
        return value


def attempt_cast_to_uuid(value: Any) -> Any:
    """
    Given a value, attempt to coerce it into a UUID (`uuid4`).
    """
    if isinstance(value, uuid.UUID):
        return value
    try:
        return (
            uuid.UUID(str(value))
            if not value_is_null(value)
            else None
        )
    except Exception:
        return value


def attempt_cast_to_bytes(value: Any) -> Any:
    """
    Given a value, attempt to coerce it into a bytestring.
    """
    if isinstance(value, bytes):
        return value
    try:
        return (
            deserialize_bytes_string(str(value))
            if not value_is_null(value)
            else None
        )
    except Exception:
        return value


def attempt_cast_to_geometry(value: Any) -> Any:
    """
    Given a value, attempt to coerce it into a `shapely` (`geometry`) object.
    """
    shapely, shapely_wkt, shapely_wkb = mrsm.attempt_import(
        'shapely',
        'shapely.wkt',
        'shapely.wkb',
        lazy=False,
    )
    if 'shapely' in str(type(value)):
        return value

    if isinstance(value, (dict, list)):
        try:
            return shapely.from_geojson(json.dumps(value))
        except Exception as e:
            return value

    value_is_wkt = geometry_is_wkt(value)
    if value_is_wkt is None:
        return value

    try:
        return (
            shapely_wkt.loads(value)
            if value_is_wkt
            else shapely_wkb.loads(value)
        )
    except Exception:
        return value


def geometry_is_wkt(value: Union[str, bytes]) -> Union[bool, None]:
    """
    Determine whether an input value should be treated as WKT or WKB geometry data.

    Parameters
    ----------
    value: Union[str, bytes]
        The input data to be parsed into geometry data.

    Returns
    -------
    A `bool` (`True` if `value` is WKT and `False` if it should be treated as WKB).
    Return `None` if `value` should be parsed as neither.
    """
    import re
    if not isinstance(value, (str, bytes)):
        return None

    if isinstance(value, bytes):
        return False
    
    wkt_pattern = r'^\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(.*\)\s*$'
    if re.match(wkt_pattern, value, re.IGNORECASE):
        return True
    
    if all(c in '0123456789ABCDEFabcdef' for c in value) and len(value) % 2 == 0:
        return False
    
    return None


def value_is_null(value: Any) -> bool:
    """
    Determine if a value is a null-like string.
    """
    return str(value).lower() in ('none', 'nan', 'na', 'nat', '', '<na>')


def none_if_null(value: Any) -> Any:
    """
    Return `None` if a value is a null-like string.
    """
    return (None if value_is_null(value) else value)


def quantize_decimal(x: Decimal, precision: int, scale: int) -> Decimal:
    """
    Quantize a given `Decimal` to a known scale and precision.

    Parameters
    ----------
    x: Decimal
        The `Decimal` to be quantized.

    precision: int
        The total number of significant digits.

    scale: int
        The number of significant digits after the decimal point.

    Returns
    -------
    A `Decimal` quantized to the specified scale and precision.
    """
    precision_decimal = Decimal(('1' * (precision - scale)) + '.' + ('1' * scale))
    try:
        return x.quantize(precision_decimal, context=Context(prec=precision), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        pass

    raise ValueError(f"Cannot quantize value '{x}' to {precision=}, {scale=}.")


def serialize_decimal(
    x: Any,
    quantize: bool = False,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
) -> Any:
    """
    Return a quantized string of an input decimal.

    Parameters
    ----------
    x: Any
        The potential decimal to be serialized.

    quantize: bool, default False
        If `True`, quantize the incoming Decimal to the specified scale and precision
        before serialization.

    precision: Optional[int], default None
        The precision of the decimal to be quantized.

    scale: Optional[int], default None
        The scale of the decimal to be quantized.

    Returns
    -------
    A string of the input decimal or the input if not a Decimal.
    """
    if not isinstance(x, Decimal):
        return x

    if value_is_null(x):
        return None

    if quantize and scale and precision:
        x = quantize_decimal(x, precision, scale)

    return f"{x:f}"


def coerce_timezone(
    dt: Any,
    strip_utc: bool = False,
) -> Any:
    """
    Given a `datetime`, pandas `Timestamp` or `Series` of `Timestamp`,
    return a UTC timestamp (strip timezone if `strip_utc` is `True`.
    """
    if dt is None:
        return None

    if isinstance(dt, int):
        return dt

    if isinstance(dt, str):
        dateutil_parser = mrsm.attempt_import('dateutil.parser')
        dt = dateutil_parser.parse(dt)

    dt_is_series = hasattr(dt, 'dtype') and hasattr(dt, '__module__')

    if dt_is_series:
        pandas = mrsm.attempt_import('pandas', lazy=False)

        if (
            pandas.api.types.is_datetime64_any_dtype(dt) and (
                (dt.dt.tz is not None and not strip_utc)
                or
                (dt.dt.tz is None and strip_utc)
            )
        ):
            return dt

        dt_series = to_datetime(dt, coerce_utc=False)
        if strip_utc:
            try:
                if dt_series.dt.tz is not None:
                    dt_series = dt_series.dt.tz_localize(None)
            except Exception:
                pass

        return dt_series

    if dt.tzinfo is None:
        if strip_utc:
            return dt
        return dt.replace(tzinfo=timezone.utc)

    utc_dt = dt.astimezone(timezone.utc)
    if strip_utc:
        return utc_dt.replace(tzinfo=None)
    return utc_dt


def to_datetime(dt_val: Any, as_pydatetime: bool = False, coerce_utc: bool = True) -> Any:
    """
    Wrap `pd.to_datetime()` and add support for out-of-bounds values.
    """
    pandas, dateutil_parser = mrsm.attempt_import('pandas', 'dateutil.parser', lazy=False)
    is_dask = 'dask' in getattr(dt_val, '__module__', '')
    dd = mrsm.attempt_import('dask.dataframe') if is_dask else None
    dt_is_series = hasattr(dt_val, 'dtype') and hasattr(dt_val, '__module__')
    pd = pandas if dd is None else dd

    try:
        new_dt_val = pd.to_datetime(dt_val, utc=True, format='ISO8601')
        if as_pydatetime:
            return new_dt_val.to_pydatetime()
        return new_dt_val
    except (pd.errors.OutOfBoundsDatetime, ValueError):
        pass

    def parse(x: Any) -> Any:
        try:
            return dateutil_parser.parse(x)
        except Exception:
            return x

    if dt_is_series:
        new_series = dt_val.apply(parse)
        if coerce_utc:
            return coerce_timezone(new_series)
        return new_series

    new_dt_val = parse(dt_val)
    if not coerce_utc:
        return new_dt_val
    return coerce_timezone(new_dt_val)


def serialize_bytes(data: bytes) -> str:
    """
    Return the given bytes as a base64-encoded string.
    """
    import base64
    if not isinstance(data, bytes) and value_is_null(data):
        return data
    return base64.b64encode(data).decode('utf-8')


def serialize_geometry(
    geom: Any,
    geometry_format: str = 'wkb_hex',
    as_wkt: bool = False,
) -> Union[str, Dict[str, Any]]:
    """
    Serialize geometry data as a hex-encoded well-known-binary string. 

    Parameters
    ----------
    geom: Any
        The potential geometry data to be serialized.

    geometry_format: str, default 'wkb_hex'
        The serialization format for geometry data.
        Accepted formats are `wkb_hex` (well-known binary hex string),
        `wkt` (well-known text), and `geojson`.

    Returns
    -------
    A string containing the geometry data.
    """
    shapely = mrsm.attempt_import('shapely', lazy=False)
    if geometry_format == 'geojson':
        geojson_str = shapely.to_geojson(geom)
        return json.loads(geojson_str)

    if hasattr(geom, 'wkb_hex'):
        return geom.wkb_hex if geometry_format == 'wkb_hex' else geom.wkt

    return str(geom)


def deserialize_geometry(geom_wkb: Union[str, bytes]):
    """
    Deserialize a WKB string into a shapely geometry object.
    """
    shapely = mrsm.attempt_import(lazy=False)
    return shapely.wkb.loads(geom_wkb)


def deserialize_bytes_string(data: str | None, force_hex: bool = False) -> bytes | None:
    """
    Given a serialized ASCII string of bytes data, return the original bytes.
    The input data may either be base64- or hex-encoded.

    Parameters
    ----------
    data: str | None
        The string to be deserialized into bytes.
        May be base64- or hex-encoded (prefixed with `'\\x'`).

    force_hex: bool = False
        If `True`, treat the input string as hex-encoded.
        If `data` does not begin with the prefix `'\\x'`, set `force_hex` to `True`.
        This will still strip the leading `'\\x'` prefix if present.

    Returns
    -------
    The original bytes used to produce the encoded string `data`.
    """
    if not isinstance(data, str) and value_is_null(data):
        return data

    import binascii
    import base64

    is_hex = force_hex or data.startswith('\\x')

    if is_hex:
        if data.startswith('\\x'):
            data = data[2:]
        return binascii.unhexlify(data)

    return base64.b64decode(data)


def deserialize_base64(data: str) -> bytes:
    """
    Return the original bytestring from the given base64-encoded string.
    """
    import base64
    return base64.b64decode(data)


def encode_bytes_for_bytea(data: bytes, with_prefix: bool = True) -> str | None:
    """
    Return the given bytes as a hex string for PostgreSQL's `BYTEA` type.
    """
    import binascii
    if not isinstance(data, bytes) and value_is_null(data):
        return data
    return ('\\x' if with_prefix else '') + binascii.hexlify(data).decode('utf-8')


def serialize_datetime(dt: datetime) -> Union[str, None]:
    """
    Serialize a datetime object into JSON (ISO format string).

    Examples
    --------
    >>> import json
    >>> from datetime import datetime
    >>> json.dumps({'a': datetime(2022, 1, 1)}, default=json_serialize_datetime)
    '{"a": "2022-01-01T00:00:00Z"}'

    """
    if not isinstance(dt, datetime):
        return None
    tz_suffix = 'Z' if dt.tzinfo is None else ''
    return dt.isoformat() + tz_suffix


def json_serialize_value(x: Any, default_to_str: bool = True) -> str:
    """
    Serialize the given value to a JSON value. Accounts for datetimes, bytes, decimals, etc.

    Parameters
    ----------
    x: Any
        The value to serialize.

    default_to_str: bool, default True
        If `True`, return a string of `x` if x is not a designated type.
        Otherwise return x.

    Returns
    -------
    A serialized version of x, or x.
    """
    if isinstance(x, (mrsm.Pipe, mrsm.connectors.Connector)):
        return x.meta

    if hasattr(x, 'tzinfo'):
        return serialize_datetime(x)

    if isinstance(x, bytes):
        return serialize_bytes(x)

    if isinstance(x, Decimal):
        return serialize_decimal(x)

    if 'shapely' in str(type(x)):
        return serialize_geometry(x)

    if value_is_null(x):
        return None

    return str(x) if default_to_str else x


def get_geometry_type_srid(
    dtype: str = 'geometry',
    default_type: str = 'geometry',
    default_srid: int = 4326,
) -> Union[Tuple[str, int], Tuple[str, None]]:
    """
    Given the specified geometry `dtype`, return a tuple in the form (type, SRID).

    Parameters
    ----------
    dtype: Optional[str], default None
        Optionally provide a specific `geometry` syntax (e.g. `geometry[MultiLineString, 4326]`).
        You may specify a supported `shapely` geometry type and an SRID in the dtype modifier:

        - `Point`
        - `LineString`
        - `LinearRing`
        - `Polygon`
        - `MultiPoint`
        - `MultiLineString`
        - `MultiPolygon`
        - `GeometryCollection`

    Returns
    -------
    A tuple in the form (type, SRID).
    Defaults to `(default_type, default_srid)`.

    Examples
    --------
    >>> from meerschaum.utils.dtypes import get_geometry_type_srid
    >>> get_geometry_type_srid()
    ('geometry', 4326)
    >>> get_geometry_type_srid('geometry[]')
    ('geometry', 4326)
    >>> get_geometry_type_srid('geometry[Point, 0]')
    ('Point', 0)
    >>> get_geometry_type_srid('geometry[0, Point]')
    ('Point', 0)
    >>> get_geometry_type_srid('geometry[0]')
    ('geometry', 0)
    >>> get_geometry_type_srid('geometry[MULTILINESTRING, 4326]')
    ('MultiLineString', 4326)
    >>> get_geometry_type_srid('geography')
    ('geometry', 4326)
    >>> get_geometry_type_srid('geography[POINT]')
    ('Point', 4376)
    """
    from meerschaum.utils.misc import is_int
    ### NOTE: PostGIS syntax must also be parsed.
    dtype = dtype.replace('(', '[').replace(')', ']')
    bare_dtype = dtype.split('[', maxsplit=1)[0]
    modifier = dtype.split(bare_dtype, maxsplit=1)[-1].lstrip('[').rstrip(']')
    if not modifier:
        return default_type, default_srid

    shapely_geometry_base = mrsm.attempt_import('shapely.geometry.base')
    geometry_types = {
        typ.lower(): typ
        for typ in shapely_geometry_base.GEOMETRY_TYPES
    }

    parts = [part.lower().replace('srid=', '').replace('type=', '').strip() for part in modifier.split(',')]
    parts_casted = [
        (
            int(part)
            if is_int(part)
            else part
        ) for part in parts]

    srid = default_srid
    geometry_type = default_type

    for part in parts_casted:
        if isinstance(part, int):
            srid = part
            break

    for part in parts:
        if part.lower() in geometry_types:
            geometry_type = geometry_types.get(part)
            break

    return geometry_type, srid
