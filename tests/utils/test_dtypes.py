#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test functions from `meerschaum.utils.misc`.
"""

from typing import Tuple, Union
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
        ('datetime64[ns, UTC]', 'Timestamp', True),
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
        ({}, ('geometry', 4326)),
        ({'dtype': 'geometry'}, ('geometry', 4326)),
        ({'dtype': 'geometry[4326]'}, ('geometry', 4326)),
        ({'dtype': 'geometry[0]'}, ('geometry', 0)),
        ({'dtype': 'geometry', 'default_type': 'Point'}, ('Point', 4326)),
        ({'dtype': 'geometry', 'default_srid': 0}, ('geometry', 0)),
        ({'dtype': 'geometry', 'default_type': 'MultiLineString', 'default_srid': 0}, ('MultiLineString', 0)),
        ({'dtype': 'geometry[POLYGON, 4326]'}, ('Polygon', 4326)),
        ({'dtype': 'geometry[4326, POLYGON]'}, ('Polygon', 4326)),
        ({'dtype': 'geometry[SRID=4326, POLYGON]'}, ('Polygon', 4326)),
        ({'dtype': 'geometry[GeometryCollection,srid=0]'}, ('GeometryCollection', 0)),
        ({'dtype': 'geometry[type=LineString,srid=2000]'}, ('LineString', 2000)),
        ({'dtype': 'geometry[type=UNKNOWN,srid=2000]'}, ('geometry', 2000)),
        ({'dtype': 'geography[POINT]'}, ('Point', 4326)),
        ({'dtype': 'geography[4326]'}, ('geometry', 4326)),
        ({'dtype': 'geography[GEOMETRY, 4326]'}, ('geometry', 4326)),
    ]
)
def test_parse_geometry_type_srid(
    kwargs: Tuple[Union[str, int], ...],
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
