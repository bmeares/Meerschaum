#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Generate the argument-parity fixtures for the `meerschaum-cli` crate.

Run from the repository root:

    PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \
        python crates/meerschaum-cli/tests/fixtures/gen_args.py

Outputs (next to this script):

- `args.json`: the synthetic sysargs corpus and the Python results of `parse_arguments`,
  `parse_line`, `parse_dict_to_sysargs`, the chain/pipeline helpers, the entry-level step
  splitting, and the custom type functions.
- `parser.json`: an introspection of the v4 `argparse` parser (every action, in order).
- `actions.json`: the core actions, their subactions, and which accept `executor_keys`.
- `help/*.txt`: the Python `--help` text, globally and per action / subaction (compared byte for
  byte by `tests/parity.rs`), and for an unknown action (`_unknown_action.txt`, the
  `show actions` table).

Determinism:
- `now` is fixed (see `FIXED_NOW`) by patching the `datetime` name in `_parser`.
- `dateutil.parser.parse` gets a fixed `default` (midnight of the fixed local day),
  and its two-digit-year pivot is pinned to the fixed year.
- `meerschaum.utils.warnings.error` is replaced by a plain `raise Exception(message)`:
  the real one only decorates the message with ANSI colors and an icon.
- `COLUMNS=80` pins the `argparse` help width.
- Plugins are never loaded (`load_plugins`/`import_plugins`/`get_plugins` are stubbed and
  `_loaded_plugins_args = True`): the corpus covers core flags only, and `main()` asserts that no
  plugin module was imported and no plugin argument group exists.
"""

from __future__ import annotations

import contextlib
import inspect
import io
import json
import math
import os
import pathlib
import re
import sys
import textwrap
from datetime import datetime as _real_datetime, timedelta, timezone

HERE = pathlib.Path(__file__).resolve().parent

### `argparse` wraps help text to the terminal width; pin it (80 columns, the non-TTY fallback).
os.environ['COLUMNS'] = '80'

FIXED_NOW = _real_datetime(2026, 10, 12, 15, 4, 5, 123456, tzinfo=timezone.utc)
FIXED_LOCAL_DEFAULT = _real_datetime(2026, 10, 12)
FIXED_LOCAL_YEAR = 2026


### --------------------------------------------------------------------------
### Patching for determinism.
### --------------------------------------------------------------------------

import meerschaum  # noqa: E402
import meerschaum.plugins as _plugins_mod  # noqa: E402
import meerschaum.utils.warnings as _warnings_mod  # noqa: E402

### Never load plugins: the corpus covers the core parser only, and plugin flags or names
### (e.g. from symlinks left in the test root's plugin resources) must not reach the fixtures.
_plugins_mod.load_plugins = lambda *args, **kwargs: None
_plugins_mod.import_plugins = lambda *args, **kwargs: None
_plugins_mod.get_plugins = lambda *args, **kwargs: ()
import meerschaum._internal.arguments._parser as _parser_mod  # noqa: E402
import meerschaum._internal.arguments._parse_arguments as _pa_mod  # noqa: E402
from meerschaum.utils.packages import attempt_import  # noqa: E402

assert meerschaum.__file__.startswith(str(HERE.parents[3])), meerschaum.__file__


def _plain_error(message, exception_class=Exception, *args, **kwargs):
    raise exception_class(str(message))


_warnings_mod.error = _plain_error


class _DatetimeMeta(type):
    def __instancecheck__(cls, inst):
        return isinstance(inst, _real_datetime)


class _FixedDatetime(_real_datetime, metaclass=_DatetimeMeta):
    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return FIXED_NOW.replace(tzinfo=None)
        return FIXED_NOW.astimezone(tz)


_parser_mod.datetime = _FixedDatetime

_dateutil_parser = attempt_import('dateutil.parser')
_original_dateutil_parse = _dateutil_parser.parse


def _fixed_dateutil_parse(timestr, parserinfo=None, **kwargs):
    kwargs.setdefault('default', FIXED_LOCAL_DEFAULT)
    return _original_dateutil_parse(timestr, parserinfo, **kwargs)


_dateutil_parser.parse = _fixed_dateutil_parse
### `attempt_import` may hand v4's parser a different (lazy) handle on `dateutil.parser`, so the
### module-level patch above does not reach it. Every handle shares `DEFAULTPARSER`, so pin the
### default there too (otherwise partial dates like `12:30` fill in the real current date).
_default_parser = _dateutil_parser.DEFAULTPARSER
_original_default_parse = _default_parser.parse


def _fixed_default_parse(timestr, default=None, *args, **kwargs):
    return _original_default_parse(timestr, FIXED_LOCAL_DEFAULT if default is None else default, *args, **kwargs)


_default_parser.parse = _fixed_default_parse
_dateutil_parser.DEFAULTPARSER.info._year = FIXED_LOCAL_YEAR
_dateutil_parser.DEFAULTPARSER.info._century = FIXED_LOCAL_YEAR // 100 * 100

_pa_mod._loaded_plugins_args = True

from meerschaum._internal.arguments import (  # noqa: E402
    parse_arguments,
    parse_line,
    parse_dict_to_sysargs,
    split_chained_sysargs,
    split_pipeline_sysargs,
    sysargs_has_api_executor_keys,
    remove_api_executor_keys,
    get_pipeline_sysargs,
    compress_pipeline_sysargs,
    remove_leading_action,
)
from meerschaum._internal.arguments._parser import (  # noqa: E402
    parser,
    parse_datetime,
    parse_executor_keys,
    parse_name,
)
from meerschaum.utils.misc import string_to_dict  # noqa: E402
import meerschaum._internal.entry as _entry_mod  # noqa: E402


### --------------------------------------------------------------------------
### Encoding Python values as tagged JSON.
### --------------------------------------------------------------------------

def _norm_message(msg: str) -> str:
    return re.sub(r'\x1b\[[0-9;]*m', '', msg)


def enc(v):
    """Encode a Python value into tagged JSON (see the Rust `PyValue::to_fixture_json`)."""
    if v is None or isinstance(v, (bool, str)):
        return v
    if v is Ellipsis:
        return {'$type': 'ellipsis'}
    if isinstance(v, int):
        if -2**63 <= v < 2**63:
            return v
        return {'$type': 'int', 'value': str(v)}
    if isinstance(v, float):
        if math.isfinite(v):
            return v
        return {'$type': 'float', 'value': repr(v)}
    if isinstance(v, complex):
        return {'$type': 'complex', 'real': enc(v.real), 'imag': enc(v.imag)}
    if isinstance(v, _real_datetime):
        return {'$type': 'datetime', 'value': v.isoformat()}
    if isinstance(v, timedelta):
        return {
            '$type': 'timedelta',
            'days': v.days,
            'seconds': v.seconds,
            'microseconds': v.microseconds,
        }
    if isinstance(v, pathlib.PurePath):
        return {'$type': 'path', 'value': str(v)}
    if isinstance(v, BaseException):
        return {
            '$type': 'exception',
            'class': type(v).__name__,
            'message': _norm_message(str(v)),
        }
    if isinstance(v, bytes):
        return {'$type': 'bytes', 'hex': v.hex()}
    if isinstance(v, list):
        return [enc(x) for x in v]
    if isinstance(v, tuple):
        return {'$type': 'tuple', 'items': [enc(x) for x in v]}
    if isinstance(v, (set, frozenset)):
        items = [enc(x) for x in v]
        items.sort(key=lambda x: json.dumps(x, sort_keys=True, separators=(',', ':'), ensure_ascii=False))
        return {'$type': 'set', 'items': items}
    if isinstance(v, dict):
        if all(isinstance(k, str) for k in v):
            return {k: enc(val) for k, val in v.items()}
        return {'$type': 'dict', 'items': [[enc(k), enc(val)] for k, val in v.items()]}
    raise TypeError(f"Cannot encode {type(v)}: {v!r}")


def capture(fn, *args, **kwargs):
    """Return {'ok': enc(result)} or {'raises': enc(exception)}."""
    try:
        return {'ok': enc(fn(*args, **kwargs))}
    except Exception as e:
        return {'raises': enc(e)}


### --------------------------------------------------------------------------
### The corpus.
### --------------------------------------------------------------------------

INT_SAMPLES = ['5', '0', '-5', ' 7 ', '+3', '1_000', '007', 'x', '3.0', '0x10', '', '99999999999999999999999']
FLOAT_SAMPLES = ['300', '1.5', '-2', '.5', '5.', '1e3', '1_0.5', 'inf', '-Infinity', 'nan', 'abc', '', ' 2 ']
STR_SAMPLES = ['value', '', 'with space', 'None', '[None]', '_negated', '-dash', '-5']
PATH_SAMPLES = ['a/b', './x//y/', '', '/abs/path/', '.', '..//a/./b', '//net/share']
NAME_SAMPLES = ['myjob', '', 'None', 'a b']
EXECUTOR_SAMPLES = ['local', 'systemd', 'none', 'None', 'NONE', 'api:main', 'api:', 'foo', 'API:main', '']

DATETIME_SAMPLES = [
    ### Integers.
    '0', '42', '-100', ' 12 ', '1.0', '1e3', '1_000', '99999999999999999999',
    ### None.
    'None',
    ### Relative.
    'now', 'NOW', 'Now',
    '1 day ago', '2 days ago', '10 days ago', '3 hours ago', '1 hour ago', '90 minutes ago',
    '45 seconds ago', '1 week ago', '2 weeks ago', '1 month ago', '13 months ago', '1 year ago',
    '2 years ago', '3 hours ago 1 hour', '30 days ago 1 day', '1 month ago 1 day',
    '2 days ago 1 week', '5 minutes ago 15 minutes', '1 day ago 1 month', 'foo 3 days ago',
    '3  days   ago', '3 DAYS AGO', '7 days ago 1 hours', '0 days ago', '10 mins ago',
    '1 day ago 2 years', '100000000 days ago',
    ### Absolute.
    '2024-01-01', '2024-01-01 12:34', '2024-01-01 12:34:56', '2024-01-01T12:34:56',
    '2024-01-01T12:34:56.789', '2024-01-01T12:34:56.789Z', '2024-01-01T12:34:56Z',
    '2024-01-01T12:34:56+05:30', '2024-01-01 00:00:00-0400', '2024-01-01 00:00:00 -04:00',
    '2024-01-01 12:00 UTC', '2024-01-01 12:00 GMT', '2024-01-01T00:00:00+00:00',
    '2024/01/02', '01/02/2024', '13/02/2024', '2024.01.02', '1/2/24', '01-02-03',
    'Jan 5 2024', '5 January 2024', 'January 5, 2024', 'January 5, 2024 3pm', 'Jan 5 2024 3:30 PM',
    '5 Jan 2024 12am', 'Sep 2024', 'Sept 3 2024', '2024 Jan', 'Jan-05-2024', '05-Jan-2024',
    '20240105', '20240105T1230', '20240105T123045', '20240105123045', '240105',
    '2024-01-05 12h30', '2024-01-05 12h', '12:30', '12:30:45.5', '3pm', '12 pm',
    'Monday', 'Mon 2024-01-01', 'Jan', 'December', '2024', '2024-02-29', '2023-02-29',
    '2024-13-01', '2024-01-32', 'tomorrow', 'xyz', '', 'T', '2024-01-01 25:00',
    '2024-01-01T12', '2024-W01', '1999', '2024-01-01 12:34:56.1234567', '2024-01-01,12:00',
    '10 am Jan 5 2024', 'nan', 'inf',
]

PARAMS_SAMPLES = [
    '{"a": 1}', '{"a": {"b": [1, 2.5, null, true]}}', '{}', '{"a": 1, "a": 2}',
    '{"x": NaN}', '{"big": 123456789012345678901234567890}', '{"u": "\\u00e9t\\u00e9"}',
    '{bad json}', '{"a": 1',
    'a:1', 'a:1,b:2', 'a:1, b: two', 'key1:value1,key2:value2', 'a:[1,2],b:{"c":3}',
    'a:{1: 2}', "a:'quoted, with comma'", 'a:"double"', 'a:True,b:None,c:False',
    'a:1.5,b:-2,c:1e3,d:0x1F,e:1_000', 'a:(1, 2),b:(),c:(1,)', 'a:{1, 2}', "a:b'bytes'",
    'a:hello world', 'a:b:c', 'novalue', ':empty', 'a:', ',,,a:1,,', 'a :1', 'a: [1, [2, 3]]',
    'a:1j', 'a:1+2j', 'a:-1.5e-3', "a:'it\\'s'", "a:r'raw\\n'", "a:'tab\\t'", 'a:{"k": [1, {"x": 2}]}',
    "'{\"quoted\": 1}'", 'x{"inner": 2}y', 'a:{[1]: 2}', 'a:[1, 2', 'a:"unterminated',
    'a:set()', 'a:-True', 'a:+5', 'a:--5', 'a:1 if 1 else 2', 'a:None,b:none',
    'a:{"x": 1}, b:[3]', 'a:0o17,b:0b101', 'a:1__0', "a:'a' 'b'", 'a:...',
    'a:1e400', 'a:\'\\x41\\u00e9\'', 'a:[]', 'a:{}', 'a:   7',
]


def _option_samples(action):
    """Yield sysargs fragments exercising a single argparse action."""
    opts = action.option_strings
    if not opts:
        return
    nargs = action.nargs
    t = action.type
    choices = action.choices
    takes_values = not (nargs == 0)

    if not takes_values:
        for opt in opts:
            yield [opt]
            yield ['show', 'pipes', opt]
        return

    if nargs == '...':
        for opt in opts:
            yield [opt, '--version']
            yield ['ls', opt, '-lh', '--color']
            yield [opt]
        yield [opts[0] + '=--version']
        return

    if choices is not None:
        samples = list(choices) + ['bogus', '']
    elif t is int:
        samples = INT_SAMPLES
    elif t is float:
        samples = FLOAT_SAMPLES
    elif t is parse_datetime:
        samples = DATETIME_SAMPLES
    elif t is string_to_dict:
        samples = PARAMS_SAMPLES
    elif t is parse_name:
        samples = NAME_SAMPLES
    elif t is parse_executor_keys:
        samples = EXECUTOR_SAMPLES
    elif t is pathlib.Path:
        samples = PATH_SAMPLES
    else:
        samples = STR_SAMPLES

    first = opts[0]
    for val in samples:
        yield ['sync', 'pipes', first, val]
    for opt in opts:
        yield [opt, samples[0]]
        if opt.startswith('--'):
            yield [opt + '=' + samples[0]]
        else:
            yield [opt + samples[0]]
    yield [first]
    yield ['show', 'pipes', first]
    yield [first, '--debug']
    if nargs == '+':
        yield [first, 'a', 'b', 'c']
        yield [first, '_a', 'b']
        yield [first, 'a', first, 'b']
        yield ['show', first, 'a', 'b', '--debug', 'pipes']
        yield [first, 'a', '--', 'b']
        yield [first + '=a', 'b']


EXPLICIT = [
    [],
    ['show', 'pipes'],
    ['show', 'version'],
    ['show', 'version', '--nopretty'],
    ['show', 'arguments', '-c', 'sql:main'],
    ['show', 'help'],
    ['python'],
    ['python', '-c', 'print(1)'],
    ['python', '-c', 'print(1)', '--noask'],
    ['python', '-c'],
    ['python', 'print(1)', '-c', 'x'],
    ['compose', 'up'],
    ['compose', 'up', '--file', 'mrsm-compose.yaml', '--dry', '-v'],
    ['compose', 'down', '-v', '--env-file', '.env.test'],
    ['compose', 'up', '--file=x.yaml', '--presync', '--no-jobs', '--isolated'],
    ['ls', '-v', '--file', 'x', '--dry'],
    ['sh', 'echo', '--file=abc', '--env-file', 'e', '--drop', '--volumes'],
    ['stack', 'up', '-d'],
    ['stack', 'logs', '-f', 'api'],
    ['start', 'api', '-p', '8224', '--host', '127.0.0.1', '-i', 'sql:main', '--secure'],
    ['start', 'api', '--production', '--workers', '4', '--no-dash', '--no-webterm', '--private'],
    ['start', 'api', '--keyfile', 'k.pem', '--certfile', 'c.pem', '--webterm-port', '8765'],
    ['sync', 'pipes', '-c', 'plugin:x', '-m', 'y', '-l', '_z', '-t', 'a', '-t', 'b'],
    ['sync', 'pipes', '-c', 'plugin:x', 'sql:main', '-m', 'y', 'z', '-l', 'None', '[None]'],
    ['sync', 'pipes', '-l', 'None'],
    ['sync', 'pipes', '-l', '_None', 'a'],
    ['sync', 'pipes', '-t', 'a', 'b', '_c'],
    ['register', 'pipe', '-c', 'sql:main', '-m', 'temp', '--params', '{"a": 1}'],
    ['register', 'pipe', '-c', 'sql:main', '-m', 'temp', '-P', 'a:1,b:2'],
    ['register', 'pipe', '-c', 'sql:main', '-m', 'temp', '--params', '{"columns": {"datetime": "ts", "id": "id"}, "fetch": {"backtrack_minutes": 1440}}'],
    ['sql', 'main', 'SELECT 1'],
    ['sql', 'main', 'SELECT * FROM t WHERE x = -1'],
    ['sql', 'main', 'read', 'SELECT 1', '--nopretty'],
    ['sync', 'pipes', '-d', '--name', 'myjob'],
    ['sync', 'pipes', '-d', '--name', 'myjob', '--restart', '--rm'],
    ['sync', 'pipes', '-d', '--name='],
    ['sync', 'pipes', '--loop', '--min-seconds', '300', '-w', '1'],
    ['sync', 'pipes', '--loop', '--cooldown', '20', '--timeout', '60'],
    ['sync', 'pipes', '-s', 'daily starting 02:00'],
    ['sync', 'pipes', '--schedule', 'every 10 seconds', '-d'],
    ['sync', 'pipes', '--cron', '*/5 * * * *'],
    ['sync', 'pipes', '-s', 'every 1 hours starting 00:00', '-i', 'sql:local', '-c', 'sql:main', '-m', 'foo'],
    ['sync', 'pipes', '--begin', '2024-01-01', '--end', 'now'],
    ['sync', 'pipes', '--begin', '1 day ago', '--end', '1 hour ago'],
    ['sync', 'pipes', '--begin', 'None', '--end', 'None'],
    ['sync', 'pipes', '--begin=2024-01-01'],
    ['sync', 'pipes', '--begin', '0', '--end', '100'],
    ['verify', 'pipes', '--chunk-minutes', '1440', '--bounded', '--skip-chunks-with-greater-rowcounts'],
    ['verify', 'pipes', '--chunk-hours', '24', '--check-rowcounts-only'],
    ['verify', 'pipes', '--chunk-days', '7', '--deduplicate'],
    ['verify', 'pipes', '--chunk-minutes', '10', '--chunk-days', '7'],
    ['verify', 'pipes', '--chunk-days', '999999999999'],
    ['verify', 'pipes', '--chunk-minutes', '-5'],
    ['sync', 'pipes', '--skip-check-existing', '--skip-enforce-dtypes', '--skip-hooks'],
    ['sync', 'pipes', '--allow-duplicates'],
    ['sync', 'pipes', '--listen', '--async', '--unblock'],
    ['sync', 'pipes', '--chunksize', '1000', '--batchsize', '4', '--cache', '--verify'],
    ['copy', 'pipes', '--sync-data', 'filter', '--begin', '2024-01-01'],
    ['delete', 'pipes', '-y', '-f'],
    ['delete', 'pipes', '-yf'],
    ['delete', 'pipes', '-fy', '--noask'],
    ['delete', 'pipes', '--no-ask'],
    ['delete', 'pipes', '-y=1'],
    ['edit', 'config', '--patch', '--rollback'],
    ['edit', 'config', '--restore'],
    ['install', 'plugins', 'noaa', '-r', 'api:mrsm', '--skip-deps', '--dry-run'],
    ['install', 'packages', 'pandas', '--venv', 'noaa'],
    ['install', 'packages', 'pandas', '--venv', 'None'],
    ['register', 'token', '--ttl-days', '30', '--scopes', 'pipes:read', 'pipes:write'],
    ['register', 'token', '--scope', 'actions:execute'],
    ['show', 'pipes', '--nopretty', '--gui', '--temporary'],
    ['show', 'pipes', '--temp', '--shell', '--use-bash', '--allow-shell-job', '--no-daemon'],
    ['show', 'pipes', '--debug', '--trace'],
    ['show', 'pipes', '--targets', 'a', '_b', '--datetime-dtypes', 'datetime', '_None'],
    ['show', 'pipes', '--target', 'a', '--dtype', 'int'],
    ['show', 'pipes', '--dtypes', 'bogus'],
    ['compress', 'pipes', '--no-policy'],
    ['vacuum', 'pipes', '--full'],
    ['start', 'jobs', '-e', 'api:main'],
    ['start', 'jobs', '--executor-keys', 'systemd'],
    ['start', 'job', 'foo', '-e', 'none'],
    ['-V'],
    ['--version'],
    ['-h'],
    ['--help'],
    ['sync', '--help'],
    ['--debug'],
    ['--debug', 'show', 'pipes'],
    ['-d', 'show', 'pipes'],
    ['show', '-d', 'pipes'],
    ['show', '--debug', 'pipes'],
    ['show', 'pipes', '-c', 'a', 'pipes'],
    ['-c', 'sql:main', 'show', 'pipes'],
    ['show', 'pipes', '--unknown-flag', 'x'],
    ['show', 'pipes', '--unknown-flag=x', '-Z'],
    ['show', 'pipes', '-xyz'],
    ['show', 'pipes', '--deb'],
    ['show', 'pipes', '--chunk', '5'],
    ['show', 'pipes', '--no-a'],
    ['show', 'pipes', '--noa'],
    ['show', 'pipes', '--para=a:1'],
    ['show', 'pipes', '--datetime-dt', 'int'],
    ['show', 'pipes', '--min', '5'],
    ['show', 'pipes', '--sk'],
    ['show', 'pipes', '--skip-c'],
    ['show', 'pipes', '--debug=1'],
    ['show', 'pipes', '-p8224'],
    ['show', 'pipes', '-p=8224'],
    ['show', 'pipes', '-yp', '80'],
    ['show', 'pipes', '-yp80'],
    ['show', 'pipes', '-csql:main'],
    ['show', 'pipes', '-c foo'],
    ['show', 'pipes', '-c', '-5'],
    ['show', 'pipes', '-p', '-5'],
    ['show', 'pipes', '-p', '-x'],
    ['show', 'pipes', '-c'],
    ['show', 'pipes', '--', '-c', 'x'],
    ['show', '--', 'pipes', '--', 'x'],
    ['show', 'pipes', '-'],
    ['show', 'pipes', '-', '--debug'],
    ['show', 'pipes', ''],
    ['', 'show'],
    ['ls', '[-lh]'],
    ['ls', '[-l', '-h]'],
    ['show', '[-lh]', 'pipes'],
    ['ls', '[', 'x', ']'],
    ['ls', '[a]', '[b]'],
    ['ls', 'a]', '[b'],
    ['ls', '[a b]'],
    ['ls', '[]'],
    ['ls', '[unclosed'],
    ['echo', '-A', 'these', 'are', 'sub-arguments'],
    ['echo', '-A', 'a b', 'c'],
    ['echo', '-A', ''],
    ['echo', '-A', '--debug', '-c', 'x'],
    ['stack', '-A=--version'],
    ['stack', '-A', '--version', '[-x]'],
    ['show', 'pipes', '+', 'show', 'version'],
    ['show', 'pipes', ':', '--loop'],
    ['show', 'pipes', '::', 'x'],
    ['mrsm'],
    ['mrsm', 'show', 'pipes'],
    ['do', 'show', 'pipes'],
    ['sync', 'pipes', '-c', 'sql:main', '-m', 'a', '-l', 'b', '-i', 'sql:x', '-t', 't1', '--begin', '2024-01-01', '--end', '2024-02-01', '--params', 'a:1', '-w', '2', '--min-seconds', '1.5', '--timeout-seconds', '3', '--chunksize', '10', '--debug'],
    ['bootstrap', 'pipes', '--sync-data', 'nothing'],
    ['show', 'logs', 'myjob', '--nopretty'],
    ['attach', 'job', 'myjob', '-e', 'api:main'],
    ['restart', 'jobs', '-y'],
    ['pause', 'jobs', 'a', 'b'],
    ['os', 'ls'],
    ['api', 'main', 'show', 'pipes'],
    ['login', 'api:main'],
    ['reload'],
    ['upgrade', 'mrsm'],
    ['tag', 'pipes', '+foo', '-t', 'x'],
    ['tag', 'pipes', 'foo', '_bar'],
    ['show', 'columns', '-c', 'sql:main', '-m', 'a', '-l', '[None]'],
    ['show', 'data', '--begin', 'Jan 5 2024', '--end', '2024-01-06T00:00:00Z'],
    ['clear', 'pipes', '--begin', '7 days ago 1 day', '-y'],
    ['drop', 'pipes', '-y', '--drop'],
    ['show', 'pipes', '-I', 'sql:main', '-C', 'a', '-M', 'b', '-L', 'c'],
    ['show', 'pipes', '--mrsm-instance', 'api:main'],
    ['show', 'pipes', '--instance', 'None'],
    ['show', 'pipes', '-i', 'None'],
    ['show', 'pipes', '--name', 'None'],
    ['partition', 'pipes', '--chunk-minutes', '60'],
    ['analyze', 'pipes', '--debug'],
    ['index', 'pipes'],
    ['deduplicate', 'pipes', '--dedup'],
    ['decompress', 'pipes', '--nopolicy'],
    ['start', 'webterm', '--webterm-port', '9000', '--host', '0.0.0.0'],
    ['start', 'mcp'],
    ['setup', 'plugins', 'x', '-y'],
    ['uninstall', 'plugins', 'x', '--noask'],
    ['show', 'pipes', '-c', 'x', '-m', 'y', '--', '-l', 'z'],
    ['sync', 'pipes', '--min-seconds'],
    ['sync', 'pipes', '-w', 'many'],
    ['show', 'pipes', '--gunicorn', '--insecure', '--noauth', '--private-mode', '--nodash', '--nowebterm'],
    ['show', 'pipes', '--compose-file', 'a.yaml', '--volumes'],
    ['compose', 'up', '--compose-file', 'a.yaml', '--volumes'],
    ['compose', 'up', '--fi', 'a.yaml'],
    ['show', 'pipes', '--fi', 'a.yaml'],
    ['show', 'pipes', '-vy'],
    ['show', 'pipes', '--file'],
    ['show', 'pipes', '--file', 'x', '--file', 'y'],
    ['show', 'pipes', '--env-file=a', '--env-file', 'b'],
    ['show', 'pipes', '-P', ''],
    ['show', 'pipes', '-P', 'a:1', '-P', 'b:2'],
    ['show', 'pipes', '-c', 'a', '-c'],
    ['show', 'pipes', '-h', '-c', 'x'],
    ['show', 'pipes', '-V', '--nopretty'],
    ['show', 'pipes', '--yes', '--force'],
    ['show', 'pipes', '-y', '-y'],
    ['x' * 3, '-c', 'é', '-m', '日本'],
    ['sync', 'pipes', '-c', 'plugin:x', '-m', 'y', '--loop', '--min-seconds', '604800', '-l', '_z', '-w', '1'],
    ['start', 'pipeline', 'show', 'pipes', '-P', '{"sub_args_line":"show pipes","patch_args":null}'],
    ### More argparse edge cases.
    ['show', 'pipes', '-yfd'],
    ['show', 'pipes', '-yfx'],
    ['show', 'pipes', '-ypx'],
    ['show', 'pipes', '-yf=1'],
    ['show', 'pipes', '-y-f'],
    ['show', 'pipes', '--na=x'],
    ['show', 'pipes', '--na', 'x'],
    ['show', 'pipes', '--no'],
    ['show', 'pipes', '--n'],
    ['show', 'pipes', '--foo bar'],
    ['show', 'pipes', '-x y'],
    ['show', 'pipes', '--=x'],
    ['show', 'pipes', '-=x'],
    ['show', 'pipes', '---debug'],
    ['show', 'pipes', '-1', '-2.5', '-.5'],
    ['-5', 'show'],
    ['show', 'pipes', '-c', 'a', '--', '--', 'b'],
    ['--', 'show', 'pipes', '-c', 'x'],
    ['show', 'pipes', '-A', '--', 'x'],
    ['show', 'pipes', '-Ax', 'y'],
    ['show', 'pipes', '-A'],
    ['show', 'pipes', '--sub-args=a b', 'c'],
    ['show', 'pipes', '-i', 'sql:main', '-i', 'api:main'],
    ['show', 'pipes', '-i'],
    ['show', 'pipes', '--instance='],
    ['show', 'pipes', '-P', 'a:"x,y",b:[1,{"c":[2]}]'],
    ['show', 'pipes', '-P', "a:'[unclosed,b:2"],
    ['show', 'pipes', '-P', 'a:1,a:2'],
    ['show', 'pipes', '-P', '  a:1  ,  b : 2  '],
    ['show', 'pipes', '--params', '{"nested": {"x": [1, 2, {"y": "z"}]}, "f": 1.5e10}'],
    ['show', 'pipes', '--begin', '2024-01-01', '--begin', '2024-02-01'],
    ['show', 'pipes', '--begin', '1 day ago', '--end', '2024-01-01T00:00:00-05:00'],
    ['sync', 'pipes', '-c', 'sql:main', '-m', 'x', '-d', '+', 'show', 'pipes'],
    ['show', '-c', 'x', 'pipes', '-m', 'y'],
    ['show', 'pipes', '[--debug]', '-c', 'x'],
    ['[a', 'b]', 'show'],
    ['show', 'pipes', '-l', 'a', '-l', '[None]'],
    ['show', 'pipes', '--venv', '[None]'],
    ['show', 'pipes', '--venv=[None]'],
    ['show', 'pipes', '--tags=a', 'b'],
    ['show', 'pipes', '-ta', 'b'],
    ['show', 'pipes', '-w1'],
    ['show', 'pipes', '--min-seconds', '1e400'],
    ['show', 'pipes', '--timeout-seconds', '-0'],
    ['show', 'pipes', '--sync-data', 'all', '--sync-data', 'bogus'],
    ['show', 'pipes', '--dtypes', 'int', 'bogus', '-p', 'x'],
    ['show', 'pipes', '-p', 'x', '--dtypes', 'bogus'],
    ['show', 'pipes', '--file=a', '--file', 'b', '-v'],
    ['compose', 'down', '--file=a', '-v', '--drop'],
    ['None'],
    ['show', 'None'],
    ['show', 'pipes', '--name', 'my job', '-d', '--restart'],
    ['show', 'pipes', '--executor-keys=api:main'],
    ['show', 'pipes', '-esystemd'],
]


### Chains: run through the entry-level splitter.
CHAINS = [
    ['show', 'pipes', '+', 'show', 'version'],
    ['show', 'pipes', '+', '+', 'show', 'version', '+'],
    ['+', 'show', 'pipes'],
    ['show', 'pipes', '+', 'sync', 'pipes', '-c', 'sql:main', '+', 'show', 'version', '--nopretty'],
    ['show', 'pipes', ':', '--loop', '--min-seconds', '10'],
    ['show', 'pipes', '+', 'show', 'version', ':', '-s', 'every 5 seconds'],
    ['show', 'pipes', ':', 'x', ':', '-d'],
    ['show', 'pipes', '::', 'x'],
    ['show', 'pipes', '-d', '+', 'show', 'version'],
    ['start', 'job', 'x', '+', 'show', 'version'],
    ['show', 'pipes', '+', 'show', 'version', ':', '-e', 'api:main'],
    ['show', 'pipes', '+', 'show', 'version', ':', '-e'],
    ['sync', 'pipes', '--begin', 'xyz', '+', 'show', 'version'],
    ['show', 'version', '+', 'sync', 'pipes', '--begin', 'xyz'],
    ['show', 'arguments', '--begin', 'xyz'],
    ['show', 'pipes', '-p', 'abc'],
    ['show', 'pipes', '+', 'show', 'pipes', '--chunk', '5'],
    [],
    ['+'],
    [':'],
    ['show', ':'],
    [':', 'show'],
    ['show', 'pipes', ':', '--loop', '+', 'x'],
    ['show', 'pipes', '+', 'x', ':', '-d', '--name', 'chain'],
    ['sync', 'pipes', '-c', 'plugin:x', '::', 'a', '+', 'show', 'version'],
]

LINES = [
    'show pipes --debug',
    'sync pipes -c "sql:main" -m \'my metric\' --params \'{"a": 1}\'',
    'sql main "SELECT * FROM t WHERE x = \'y\'"',
    'show pipes "unclosed',
    'show pipes \\',
    'echo a\\ b "c\\"d" \'e\\f\'',
    '',
    '   ',
    'show pipes # not a comment',
    'show "" pipes',
    'show pi"pe"s',
    'register pipe -c sql:main -m x -P a:1,b:"two words"',
    'show pipes\t-c\tx\n-m y',
    'echo "a\\$b" "\\\\" "\\x"',
    "echo 'it'\"'\"'s'",
    'sync pipes --begin "1 day ago" --end now',
    'start job foo -e api:main',
    'show pipes + show version',
]

STRING_TO_DICT = PARAMS_SAMPLES + ['', '{"a": 1}', 'a:{"b": {"c": [1, 2, {"d": null}]}}']

REMOVE_LEADING_ACTION = [
    ['show'], ['show', 'pipes'], ['show', 'pipe'], ['show', 'pipes', 'baz'], ['show', 'foo'],
    ['show', 'foo', 'bar'], ['sync', 'pipes'], ['sync', 'pipe', 'x', 'y'], ['foo'], ['foo', 'bar'],
    ['start', 'job', 'myjob'], ['start', 'jobs'], ['start', 'api'], ['start', 'pipeline', 'x'],
    ['delete', 'jobs', 'a_b', 'c'], ['show', 'pipes', 'a_b'], ['show_pipes'], ['install', 'plugin', 'noaa'],
    ['register', 'pipe'], ['register', 'tokens'], ['edit', 'definition', 'x'], ['api', 'main', 'show', 'pipes'],
    ['api', 'start'], ['sh', 'ls', '-l'], ['python'], ['stack', 'up', '-d'], ['compose', 'up'],
    ['show', 'version'], ['show', 'arguments'], ['show', 'help'], ['verify', 'rowcount'], ['bootstrap', 'job'],
    ['', 'show'], ['show', ''], ['show', 'pipes', ''], ['drop', 'index'], ['drop', 'indices'],
    ['upgrade', 'meerschaum', 'x'], ['restart', 'daemon'], ['show', 'partition'], ['show', 'sizes', 'x'],
]


def _norm_text(msg: str) -> str:
    return _norm_message(msg)


def gen_entry_steps(sysargs):
    """Run `entry_without_daemon` with a capturing `entry_with_args`."""
    steps = []

    def _capture(_actions=None, _patch_args=None, **kw):
        steps.append(enc(kw))
        return True, 'captured'

    original = _entry_mod.entry_with_args
    _entry_mod.entry_with_args = _capture
    try:
        success, msg = _entry_mod.entry_without_daemon(list(sysargs))
        result = {'success': success}
        if not success:
            result['message'] = _norm_text(msg)
    except Exception as e:
        result = {'raises': enc(e)}
    finally:
        _entry_mod.entry_with_args = original
    return {'steps': steps, 'result': result}


def build_cases():
    corpus = []
    seen = set()

    def add(sysargs):
        key = json.dumps(sysargs)
        if key in seen:
            return
        seen.add(key)
        corpus.append(list(sysargs))

    for sysargs in EXPLICIT:
        add(sysargs)
    for action in parser._actions:
        for fragment in _option_samples(action):
            add(fragment)
    ### Pairwise combos of boolean flags after an action.
    bool_flags = [a.option_strings[0] for a in parser._actions if a.option_strings and a.nargs == 0]
    for i, flag in enumerate(bool_flags):
        other = bool_flags[(i * 7 + 3) % len(bool_flags)]
        add(['sync', 'pipes', flag, other, '-c', 'sql:main'])
    return corpus


def encode_parse(sysargs):
    out = {'sysargs': sysargs}
    try:
        args = parse_arguments(list(sysargs))
    except Exception as e:
        out['raises'] = enc(e)
        return out
    out['args'] = enc(args)
    try:
        out['dict_to_sysargs'] = {'ok': parse_dict_to_sysargs(args)}
    except Exception as e:
        out['dict_to_sysargs'] = {'raises': enc(e)}
    return out


def gen_args_fixture():
    cases = [encode_parse(s) for s in build_cases()]
    chains = []
    for sysargs in CHAINS + [c for c in EXPLICIT if '+' in c or ':' in c or '::' in c]:
        pipeline = split_pipeline_sysargs(list(sysargs))
        pipeline_sysargs = get_pipeline_sysargs(pipeline[0], pipeline[1])
        chains.append({
            'sysargs': sysargs,
            'split_pipeline': [pipeline[0], pipeline[1]],
            'split_chained': capture(split_chained_sysargs, list(sysargs)),
            'has_api_executor_keys': capture(sysargs_has_api_executor_keys, list(sysargs)),
            'remove_api_executor_keys': capture(remove_api_executor_keys, list(sysargs)),
            'pipeline_sysargs': pipeline_sysargs,
            'compressed': capture(compress_pipeline_sysargs, pipeline_sysargs),
            'entry': gen_entry_steps(sysargs),
        })
    lines = [{'line': line, 'args': enc(parse_line(line))} for line in LINES]

    from meerschaum.actions import actions as _all_actions
    core_actions = {
        k: f for k, f in _all_actions.items() if f.__module__.startswith('meerschaum.actions')
    }
    remove_leading = [
        {'action': a, 'result': capture(remove_leading_action, list(a), _actions=core_actions)}
        for a in REMOVE_LEADING_ACTION
    ]

    type_functions = {
        'parse_datetime': [{'input': s, **capture(parse_datetime, s)} for s in DATETIME_SAMPLES],
        'string_to_dict': [{'input': s, **capture(string_to_dict, s)} for s in STRING_TO_DICT],
        'parse_executor_keys': [{'input': s, **capture(parse_executor_keys, s)} for s in EXECUTOR_SAMPLES],
        'parse_name': [{'input': s, **capture(parse_name, s)} for s in NAME_SAMPLES],
    }

    return {
        'generator': 'crates/meerschaum-cli/tests/fixtures/gen_args.py',
        'python_version': sys.version.split()[0],
        'meerschaum_version': meerschaum.__version__,
        'now': FIXED_NOW.isoformat(),
        'dateutil_default': FIXED_LOCAL_DEFAULT.isoformat(),
        'local_year': FIXED_LOCAL_YEAR,
        'cases': cases,
        'chains': chains,
        'lines': lines,
        'remove_leading_action': remove_leading,
        'type_functions': type_functions,
    }


def _type_name(t):
    if t is None:
        return None
    return getattr(t, '__name__', repr(t))


def gen_parser_fixture():
    group_of = {}
    for group in parser._action_groups:
        for a in group._group_actions:
            group_of.setdefault(id(a), group.title)
    out = []
    for a in parser._actions:
        out.append({
            'dest': a.dest,
            'option_strings': list(a.option_strings),
            'action': type(a).__name__,
            'nargs': a.nargs,
            'type': _type_name(a.type),
            'default': enc(a.default),
            'const': enc(a.const),
            'choices': list(a.choices) if a.choices is not None else None,
            'required': a.required,
            'help': a.help,
            'metavar': a.metavar,
            'group': group_of.get(id(a)),
        })
    return {
        'prog': parser.prog,
        'usage': parser.usage,
        'description': parser.description,
        'groups': [g.title for g in parser._action_groups],
        'arguments': out,
    }


def gen_actions_fixture():
    from meerschaum.actions import actions as _all_actions, get_subactions
    core = {}
    for name, f in _all_actions.items():
        if not f.__module__.startswith('meerschaum.actions'):
            continue
        subs = get_subactions(name, _actions={k: v for k, v in _all_actions.items() if v.__module__.startswith('meerschaum.actions')})
        core[name] = {
            'function': f.__name__,
            'accepts_executor_keys': 'executor_keys' in inspect.signature(f).parameters,
            'subactions': {
                sub: {
                    'function': sf.__name__,
                    'accepts_executor_keys': 'executor_keys' in inspect.signature(sf).parameters,
                }
                for sub, sf in subs.items()
            },
        }
    return core


def gen_help_fixtures(actions_fixture):
    from meerschaum._internal.arguments._parser import parse_help
    help_dir = HERE / 'help'
    help_dir.mkdir(exist_ok=True)
    for old in help_dir.glob('*.txt'):
        old.unlink()

    def _dump(name, sysargs):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            try:
                parse_help(sysargs)
            except Exception as e:
                print(f"<help raised {type(e).__name__}: {e}>")
        (help_dir / f'{name}.txt').write_text(_norm_message(buf.getvalue()), encoding='utf-8')

    (help_dir / '_global.txt').write_text(parser.format_help(), encoding='utf-8')

    ### An unknown action prints `show actions` (`print_options`, which sizes its columns with
    ### `os.get_terminal_size()`; pin it to `$COLUMNS` for this page).
    _real_get_terminal_size = os.get_terminal_size

    def _no_terminal(*args, **kwargs):
        raise OSError("not a terminal")

    os.get_terminal_size = _no_terminal
    try:
        _dump('_unknown_action', ['foo', '--help'])
    finally:
        os.get_terminal_size = _real_get_terminal_size

    for action, info in sorted(actions_fixture.items()):
        _dump(action, [action, '--help'])
        for sub in sorted(info['subactions']):
            _dump(f'{action}.{sub}', [action, sub, '--help'])


def _assert_core_only():
    plugin_modules = [m for m in sys.modules if m == 'plugins' or m.startswith('plugins.')]
    assert not plugin_modules, f"Plugin modules were imported: {len(plugin_modules)}"
    titles = [g.title for g in parser._action_groups]
    assert not any(str(t).startswith(('Plugin ', 'Custom options')) for t in titles), titles


def main():
    actions_fixture = gen_actions_fixture()
    _assert_core_only()
    with open(HERE / 'actions.json', 'w', encoding='utf-8') as f:
        json.dump(actions_fixture, f, indent=1, ensure_ascii=False, allow_nan=False, sort_keys=True)
        f.write('\n')
    with open(HERE / 'parser.json', 'w', encoding='utf-8') as f:
        json.dump(gen_parser_fixture(), f, indent=1, ensure_ascii=False, allow_nan=False)
        f.write('\n')
    args_fixture = gen_args_fixture()
    with open(HERE / 'args.json', 'w', encoding='utf-8') as f:
        json.dump(args_fixture, f, indent=1, ensure_ascii=False, allow_nan=False)
        f.write('\n')
    gen_help_fixtures(actions_fixture)
    _assert_core_only()
    print(
        f"cases={len(args_fixture['cases'])} chains={len(args_fixture['chains'])} "
        f"lines={len(args_fixture['lines'])} "
        f"type_functions={sum(len(v) for v in args_fixture['type_functions'].values())}"
    )


if __name__ == '__main__':
    main()
