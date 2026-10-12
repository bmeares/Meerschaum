#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Dump the Python `STATIC_CONFIG` to `static_config.json` for the `static.toml` parity test.

Run from the repository root:

    PYTHONPATH=$(pwd) python crates/meerschaum-core/tests/fixtures/gen_static.py

Normalization (TOML and JSON cannot express every Python type):
- `set` values become sorted lists (set iteration order is hash-randomized).
- `tuple` values become lists.
- `None` stays JSON `null` (`static.toml` spells it `{ __null__ = true }`).
- `os.cpu_count()` is recorded under `__cpu_count__` so the test can resolve
  `{ __cpu_count_or__ = N }` markers the same way.
"""

import json
import os
import pathlib
import tempfile

### Isolate from any real root and never load plugins.
for _key in [k for k in os.environ if k.startswith('MRSM_')]:
    del os.environ[_key]
_TMP = tempfile.mkdtemp(prefix='mrsm-fixture-')
os.environ['MRSM_ROOT_DIR'] = _TMP
os.makedirs(os.path.join(_TMP, 'plugins'), exist_ok=True)
os.environ['MRSM_PLUGINS_DIR'] = os.path.join(_TMP, 'plugins')
os.environ['MRSM_CORE'] = 'python'

import meerschaum  # noqa: E402
from meerschaum._internal.static import STATIC_CONFIG  # noqa: E402

HERE = pathlib.Path(__file__).parent


def normalize(value):
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    if isinstance(value, set):
        return sorted(normalize(v) for v in value)
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    return value


if __name__ == '__main__':
    assert meerschaum.__file__.startswith(str(pathlib.Path.cwd())), meerschaum.__file__
    doc = {
        '__cpu_count__': os.cpu_count(),
        'static_config': normalize(STATIC_CONFIG),
    }
    with open(HERE / 'static_config.json', 'w', encoding='utf-8') as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)
        f.write('\n')
