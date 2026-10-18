"""
Recorded results of the parity-phase Python mode, replayed by the tests that compare them with
the Rust core.

During the v5 parity phase the v4 Python bodies still lived in the tree beside the Rust core
(`MRSM_CORE=python`), and these tests ran each scenario in both and compared. The Python core is
deleted in 5.0, so the Python-mode half was recorded once, from the last tree that had it, and is
replayed:

```python
expected = oracle.expected()          # the recorded value
actual = run_in_rust()
assert actual == expected
```

These are recordings of that Python mode (the v4 code path of the parity-phase tree), **not**
output of the released v4.1.4, and they cannot be re-recorded: the code that produced them is
gone. The released v4.1.4 remains the reference for the golden fixtures of the crates
(`scripts/parity/run.sh`).

Recordings live in `tests/oracle/<module>.json`, one file per test module, as type-tagged JSON
(read without `pickle`): `{"format": 1, "recorded_with": {"pandas": "3.0.6"},
"pandas_dependent": [node id prefix, ...], "nodes": {node id: [value, ...]}}`.
Each node id is `tests/<file>::<test>[<params>]` and the list holds the values of the test's
successive `oracle.expected()` calls. Tagged values: `{"$t": "datetime" | "timestamp" | "date" |
"time" | "timedelta" | "decimal" | "uuid" | "bytes" | "tuple" | "set" | "float" | "dict", ...}`;
timezone-aware values carry their tzinfo (`zoneinfo` key or fixed offset), so `ZoneInfo('Etc/UTC')`
and `timezone.utc` stay distinguishable.

Tests whose recorded data depends on the pandas major version (dtype strings, datetime units) are
listed in `"pandas_dependent"` (by node id prefix): they are skipped, never failed, when the
installed pandas has another major than `recorded_with.pandas`. The tests never install pandas
(CI installs the `pandas` extra up front and sets `MRSM_NO_AUTO_INSTALL=1`).
"""

from __future__ import annotations

import base64
import datetime
import decimal
import json
import os
import pathlib
import uuid
from typing import Any, Dict, List

import pytest

DIRECTORY = pathlib.Path(__file__).parent / 'oracle'
_cache: Dict[str, Dict[str, Any]] = {}
_counters: Dict[str, int] = {}


def _node() -> str:
    """The node id of the running test, relative to `tests/` whatever the pytest rootdir is."""
    node = os.environ.get('PYTEST_CURRENT_TEST', '').rsplit(' (', 1)[0]
    module, separator, rest = node.partition('::')
    return 'tests/' + pathlib.PurePosixPath(module).name + separator + rest


def _path(node: str) -> pathlib.Path:
    module = node.split('::', 1)[0]
    return DIRECTORY / (pathlib.PurePosixPath(module).stem + '.json')


def _tzinfo(tag: Any) -> Any:
    if tag is None:
        return None
    if tag['kind'] == 'zoneinfo':
        import zoneinfo
        return zoneinfo.ZoneInfo(tag['key'])
    if tag['utc']:
        return datetime.timezone.utc
    return datetime.timezone(datetime.timedelta(seconds=tag['seconds']))


def _tz_tag(tzinfo: Any) -> Any:
    if tzinfo is None:
        return None
    import zoneinfo
    if isinstance(tzinfo, zoneinfo.ZoneInfo):
        return {'kind': 'zoneinfo', 'key': tzinfo.key}
    if isinstance(tzinfo, datetime.timezone):
        return {
            'kind': 'timezone',
            'seconds': tzinfo.utcoffset(None).total_seconds(),
            'utc': tzinfo == datetime.timezone.utc,
        }
    raise TypeError(f"Unsupported tzinfo {tzinfo!r}.")


def encode(value: Any) -> Any:
    """
    Turn a Python value into the type-tagged JSON form `decode()` reads (the definition of the
    format, used by the tests of this module). A plain JSON object never has a `$t` key: a dict
    that has one (or a non-text key) is written as `{"$t": "dict", "v": [[key, value], ...]}`.
    """
    import math
    if value is None or isinstance(value, (bool, str, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return {'$t': 'float', 'v': repr(value)}
        return value
    try:
        import pandas
        is_timestamp = isinstance(value, pandas.Timestamp)
    except ImportError:
        is_timestamp = False
    if is_timestamp:
        naive = value.tz_localize(None) if value.tz is not None else value
        return {
            'v': naive.isoformat(), '$t': 'timestamp', 'tz': _tz_tag(value.tz), 'unit': value.unit,
        }
    if isinstance(value, datetime.datetime):
        return {
            '$t': 'datetime', 'v': value.replace(tzinfo=None).isoformat(),
            'tz': _tz_tag(value.tzinfo), 'fold': value.fold,
        }
    if isinstance(value, datetime.date):
        return {'$t': 'date', 'v': value.isoformat()}
    if isinstance(value, datetime.time):
        return {'$t': 'time', 'v': value.isoformat()}
    if isinstance(value, datetime.timedelta):
        return {'$t': 'timedelta', 'v': [value.days, value.seconds, value.microseconds]}
    if isinstance(value, decimal.Decimal):
        return {'$t': 'decimal', 'v': str(value)}
    if isinstance(value, uuid.UUID):
        return {'$t': 'uuid', 'v': str(value)}
    if isinstance(value, (bytes, bytearray)):
        return {'$t': 'bytes', 'v': base64.b64encode(bytes(value)).decode('ascii')}
    if isinstance(value, tuple):
        return {'$t': 'tuple', 'v': [encode(item) for item in value]}
    if isinstance(value, list):
        return [encode(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return {'$t': 'set', 'v': [encode(item) for item in sorted(value, key=repr)]}
    if isinstance(value, dict):
        if all(isinstance(key, str) for key in value) and '$t' not in value:
            return {key: encode(item) for key, item in value.items()}
        return {'$t': 'dict', 'v': [[encode(key), encode(item)] for key, item in value.items()]}
    raise TypeError(f"Cannot record {type(value)}: {value!r}.")


def decode(value: Any) -> Any:
    """Turn a type-tagged JSON value back into the Python value it was recorded from."""
    if isinstance(value, list):
        return [decode(item) for item in value]
    if not isinstance(value, dict):
        return value
    kind = value.get('$t')
    if kind is None:
        return {key: decode(item) for key, item in value.items()}
    payload = value.get('v')
    if kind == 'datetime':
        return datetime.datetime.fromisoformat(payload).replace(
            tzinfo=_tzinfo(value['tz']), fold=value.get('fold', 0),
        )
    if kind == 'timestamp':
        import pandas
        return pandas.Timestamp(payload, tz=_tzinfo(value['tz'])).as_unit(value['unit'])
    if kind == 'date':
        return datetime.date.fromisoformat(payload)
    if kind == 'time':
        return datetime.time.fromisoformat(payload)
    if kind == 'timedelta':
        return datetime.timedelta(days=payload[0], seconds=payload[1], microseconds=payload[2])
    if kind == 'decimal':
        return decimal.Decimal(payload)
    if kind == 'uuid':
        return uuid.UUID(payload)
    if kind == 'bytes':
        return base64.b64decode(payload)
    if kind == 'tuple':
        return tuple(decode(item) for item in payload)
    if kind == 'set':
        return {decode(item) for item in payload}
    if kind == 'float':
        return float(payload)
    if kind == 'dict':
        return {decode(key): decode(item) for key, item in payload}
    raise ValueError(f"Unknown oracle tag '{kind}'.")


def _load(path: pathlib.Path) -> Dict[str, Any]:
    key = str(path)
    if key not in _cache:
        _cache[key] = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    return _cache[key]


def _skip_for_pandas(recorded: str) -> None:
    try:
        import pandas
    except ImportError:
        pytest.skip(f"the recording needs pandas {recorded} (not installed)")
    if pandas.__version__.split('.')[0] != recorded.split('.')[0]:
        pytest.skip(
            f"the recording was made with pandas {recorded}; "
            f"pandas {pandas.__version__} is installed"
        )


def expected(node: Any = None) -> Any:
    """
    Return the recorded value of this test's next call.

    `node` names another recorded node id (`tests/<file>::<test>[<params>]`) to read instead of the
    running test's own: for a scenario whose recorded result does not depend on the parametrized
    flavor, so a flavor that could not be recorded (no v4 driver on the recording host) compares
    with the same value. The call counter stays per running test.
    """
    running = _node()
    index = _counters.get(running, 0)
    _counters[running] = index + 1
    node = node or running
    path = _path(node)
    document = _load(path)
    if node.startswith(tuple(document.get('pandas_dependent', ()))):
        _skip_for_pandas(document['recorded_with']['pandas'])
    values: List[Any] = document.get('nodes', {}).get(node, [])
    if index >= len(values):
        pytest.fail(
            f"no recorded result for call {index} of {node} (tests/oracle/{path.name})",
            pytrace=False,
        )
    return decode(values[index])
