#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test functions from `meerschaum.utils.misc`.
"""

from typing import Tuple, Dict
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

import meerschaum as mrsm
from meerschaum.utils.packages import import_pandas
from meerschaum.utils.dtypes import (
    are_dtypes_equal,
    json_serialize_value,
    get_geometry_type_srid,
    attempt_cast_to_geometry,
    get_next_precision_unit,
)
DEBUG: bool = True
pd = import_pandas(debug=DEBUG)
np = mrsm.attempt_import('numpy')
shapely = mrsm.attempt_import('shapely')


@pytest.mark.parametrize(
    'ldtype,rdtype,are_equal',
    [
        ('string', 'string', True),
        ('str', 'string', True),
        ('str', 'object', True),
        ('int', 'int32', True),
        ('datetime64[ns, UTC]', 'datetime', True),
        ('float', 'float64', True),
        ('bool', 'bool[pyarrow]', True),
        ('Int64', 'int', True),
        ('json', 'object', True),
        ('float', 'object', False),
        ('datetime', 'object', False),
        ('uuid', 'object', True),
        ('bytes', 'object', True),
        ('numeric', 'decimal', True),
        ('decimal[28,10]', 'numeric[28,10]', True),
    ]
)
def test_are_dtypes_equal(ldtype: str, rdtype: str, are_equal: bool):
    """
    Test that different combinations of dtypes are equal (or inequal).
    """
    assert are_dtypes_equal(ldtype, rdtype) == are_equal


@pytest.mark.parametrize(
    'value,expected_serialized_value',
    [
        (datetime(2025, 1, 1), '2025-01-01T00:00:00Z'),
        (datetime(2025, 1, 1, tzinfo=timezone.utc), '2025-01-01T00:00:00+00:00'),
        (Decimal('0.0000000001'), '0.0000000001'),
        (b'hello, world!', 'aGVsbG8sIHdvcmxkIQ=='),
        (np.nan, None),
        (pd.NA, None),
        (Decimal('NaN'), None),
        (UUID('eb5ba760-5b84-433e-965f-a1ede8b0e9a6'), 'eb5ba760-5b84-433e-965f-a1ede8b0e9a6'),
        (shapely.MultiLineString([[[0, 0], [1, 2]], [[4, 4], [5, 6]]]), '01050000000200000001020000000200000000000000000000000000000000000000000000000000F03F00000000000000400102000000020000000000000000001040000000000000104000000000000014400000000000001840'),
        (
            mrsm.Pipe('test', 'serialize', instance='sql:local'),
            {
                'connector_keys': 'test',
                'metric_key': 'serialize',
                'location_key': None,
                'instance_keys': 'sql:local',
            }
        ),
        (
            mrsm.connectors.SQLConnector('test_serialize', flavor='sqlite', database=':memory:'),
            {
                'label': 'test_serialize',
                'type': 'sql',
                'database': ':memory:',
                'flavor': 'sqlite',
            },
        ),
    ]
)
def test_json_serialize_value(value, expected_serialized_value):
    """
    Test that custom dtypes are handled by the JSON serializer.
    """
    serialized_value = json_serialize_value(value)
    assert serialized_value == expected_serialized_value

@pytest.mark.parametrize(
    'kwargs,expected_type_srid',
    [
        ({}, ('geometry', 0)),
        ({'dtype': 'geometry'}, ('geometry', 0)),
        ({'dtype': 'geometry[4326]'}, ('geometry', 4326)),
        ({'dtype': 'geometry[0]'}, ('geometry', 0)),
        ({'dtype': 'geometry[PolygonZ]'}, ('PolygonZ', 0)),
        ({'dtype': 'geometry', 'default_type': 'Point'}, ('Point', 0)),
        ({'dtype': 'geometry', 'default_srid': 0}, ('geometry', 0)),
        ({'dtype': 'geometry', 'default_type': 'MultiLineString', 'default_srid': 0}, ('MultiLineString', 0)),
        ({'dtype': 'geometry[POLYGON, 4326]'}, ('POLYGON', 4326)),
        ({'dtype': 'geometry[4326, POLYGON]'}, ('POLYGON', 4326)),
        ({'dtype': 'geometry[SRID=4326, POLYGON]'}, ('POLYGON', 4326)),
        ({'dtype': 'geometry[GeometryCollection,srid=0]'}, ('GeometryCollection', 0)),
        ({'dtype': 'geometry[type=LineString,srid=2000]'}, ('LineString', 2000)),
        ({'dtype': 'geometry[type=UNKNOWN,srid=2000]'}, ('UNKNOWN', 2000)),
        ({'dtype': 'geography[POINT]'}, ('POINT', 0)),
        ({'dtype': 'geography[4326]'}, ('geometry', 4326)),
        ({'dtype': 'geography[GEOMETRY, 4326]'}, ('GEOMETRY', 4326)),
    ]
)
def test_parse_geometry_type_srid(
    kwargs: Dict[str, str],
    expected_type_srid: Tuple[str, int],
):
    """
    Testing parsing custom `geometry` syntax.
    """
    geometry_type, srid = get_geometry_type_srid(**kwargs)
    expected_type, expected_srid = expected_type_srid

    assert geometry_type == expected_type
    assert srid == expected_srid


@pytest.mark.parametrize(
    'input_data,expected_output',
    [
        ('POINT (-82.35004 34.84873)', shapely.Point(-82.35004, 34.84873)),
        ('0101000000AE122C0E679654C02A91442FA36C4140', shapely.Point(-82.35004, 34.84873)),
        ({'type': 'Point', 'coordinates': [-82.35004, 34.84873]}, shapely.Point(-82.35004, 34.84873)),
        (None, None),
        ({'invalid': 1}, {'invalid': 1}),
        (shapely.Point(-82.35004, 34.84873), shapely.Point(-82.35004, 34.84873)),
    ]
)
def test_parse_geometry_formats(input_data, expected_output):
    """
    Test that serialized geometry data are parsed into geometry objects.
    """
    output = attempt_cast_to_geometry(input_data)
    assert output == expected_output


@pytest.mark.parametrize(
    "input_dt_val,expected_output,kwargs",
    [
        (
            "2026-01-01T12:00:00Z",
            datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            {'as_pydatetime': True, 'coerce_utc': True},
        ),
        (
            pd.Series(["2026-01-01T12:00:00Z"]),
            pd.Series([pd.Timestamp("2026-01-01T12:00:00Z")]),
            {},
        ),
        (
            pd.Timestamp("2026-02-10"),
            datetime(2026, 2, 10, tzinfo=timezone.utc),
            {'as_pydatetime': True, 'coerce_utc': True},
        ),
        (
            pd.Series([pd.Timestamp("2026-03-03")]),
            pd.Series([pd.Timestamp("2026-03-03 00:00:00+0000", tz='UTC')]),
            {'coerce_utc': True},
        ),
        (
            pd.Series([pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-02", tz='US/Eastern')]),
            pd.Series([
                pd.Timestamp("2026-01-01 00:00:00+0000", tz='UTC'),
                pd.Timestamp("2026-01-02 05:00:00+0000", tz='UTC')
            ]),
            {'coerce_utc': True},
        ),
        (
            pd.Series([datetime(2026, 1, 1), datetime(2026, 1, 2, tzinfo=timezone.utc)]),
            pd.Series([
                pd.Timestamp("2026-01-01 00:00:00+0000", tz='UTC'),
                pd.Timestamp("2026-01-02 00:00:00+0000", tz='UTC'),
            ]),
            {'coerce_utc': True},
        ),
    ],
)
def test_to_datetime(input_dt_val, expected_output, kwargs):
    """
    Test the `to_datetime()` parsing.
    """
    from meerschaum.utils.dtypes import to_datetime
    output_dt_val = to_datetime(input_dt_val, **kwargs)
    if isinstance(output_dt_val, pd.Series):
        assert output_dt_val.to_dict() == expected_output.to_dict()
    else:
        assert output_dt_val == expected_output


@pytest.mark.parametrize(
    "precision_unit,decrease,expected",
    [
        ('nanosecond', True, 'microsecond'),
        ('ns', True, 'microsecond'),
        ('microsecond', True, 'millisecond'),
        ('us', True, 'millisecond'),
        ('millisecond', True, 'second'),
        ('ms', True, 'second'),
        ('second', True, 'minute'),
        ('s', True, 'minute'),
        ('minute', True, 'hour'),
        ('min', True, 'hour'),
        ('hour', True, 'day'),
        ('hr', True, 'day'),
        ('day', False, 'hour'),
        ('d', False, 'hour'),
        ('hour', False, 'minute'),
        ('h', False, 'minute'),
        ('minute', False, 'second'),
        ('m', False, 'second'),
        ('second', False, 'millisecond'),
        ('sec', False, 'millisecond'),
        ('millisecond', False, 'microsecond'),
        ('microsecond', False, 'nanosecond'),
    ]
)
def test_get_next_precision(precision_unit, decrease, expected):
    """
    Test the `get_next_precision_unit()` function.
    """
    assert get_next_precision_unit(precision_unit, decrease=decrease) == expected


@pytest.mark.parametrize(
    "precision_unit,decrease",
    [
        ('day', True),
        ('d', True),
        ('nanosecond', False),
        ('ns', False),
    ]
)
def test_get_next_precision_unit_raises_value_error_at_bounds(precision_unit: str, decrease: bool):
    """
    Test that `get_next_precision_unit()` raises a `ValueError` at the bounds of the list.
    """
    with pytest.raises(ValueError):
        get_next_precision_unit(precision_unit, decrease=decrease)


def test_get_next_precision_unit_raises_value_error_for_invalid_precision():
    """
    Test that `get_next_precision_unit()` raises a `ValueError` for an invalid precision string.
    """
    with pytest.raises(ValueError):
        get_next_precision_unit('invalid_precision')
