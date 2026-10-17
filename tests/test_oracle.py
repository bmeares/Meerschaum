"""The type-tagged JSON format of `tests/oracle/*.json` (see `tests/_oracle.py`)."""

import datetime
import decimal
import json
import math
import uuid
import zoneinfo

from tests import _oracle as oracle


def _roundtrip(value):
    return oracle.decode(json.loads(json.dumps(oracle.encode(value))))


def test_scalars_and_containers_roundtrip():
    value = {
        'text': 'a', 'int': 2 ** 70, 'float': 1.5, 'none': None, 'flag': True,
        'decimal': decimal.Decimal('1.20'), 'uuid': uuid.UUID(int=7), 'bytes': b'\x00\xff',
        'date': datetime.date(2024, 1, 2), 'delta': datetime.timedelta(days=1, microseconds=5),
        'tuple': (1, 'a', None), 'list': [1, [2, 3]], 'set': {1, 2},
    }
    result = _roundtrip(value)
    assert result == value
    assert {k: type(v) for k, v in result.items()} == {k: type(v) for k, v in value.items()}


def test_non_finite_floats_roundtrip():
    result = _roundtrip([float('nan'), float('inf'), float('-inf')])
    assert math.isnan(result[0]) and result[1:] == [float('inf'), float('-inf')]


def test_time_zones_stay_distinguishable():
    zone = datetime.datetime(2026, 1, 1, tzinfo=zoneinfo.ZoneInfo('Etc/UTC'))
    fixed = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    offset = datetime.datetime(2026, 1, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(hours=-5)))
    naive = datetime.datetime(2026, 1, 1, 12, 30, 45, 123456)
    for value in (zone, fixed, offset, naive):
        result = _roundtrip(value)
        assert result == value and repr(result.tzinfo) == repr(value.tzinfo)
    assert repr(_roundtrip(zone).tzinfo) != repr(_roundtrip(fixed).tzinfo)


def test_timestamps_keep_their_unit_and_zone():
    import pytest
    pandas = pytest.importorskip('pandas')
    stamp = pandas.Timestamp('2024-01-02 03:04:05', tz='UTC').as_unit('us')
    result = _roundtrip(stamp)
    assert result == stamp and result.unit == 'us' and repr(result.tz) == repr(stamp.tz)


def test_dicts_with_a_type_key_or_non_text_keys_are_not_mistaken_for_tags():
    value = {
        'plain': {'$t': 'datetime', 'v': 'not a date'},
        'keys': {1: 'a', (2, 3): 'b'},
        'nested': [{'$t': 'tuple', 'v': [1, 2]}],
    }
    assert _roundtrip(value) == value


def test_unknown_tags_are_rejected():
    import pytest
    with pytest.raises(ValueError):
        oracle.decode({'$t': 'nonsense', 'v': 1})
