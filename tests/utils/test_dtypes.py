#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test functions from `meerschaum.utils.misc`.
"""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

import meerschaum as mrsm
from meerschaum.utils.packages import import_pandas
from meerschaum.utils.dtypes import are_dtypes_equal, json_serialize_value
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
