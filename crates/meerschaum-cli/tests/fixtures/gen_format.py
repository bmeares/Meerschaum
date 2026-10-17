#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Generate the formatting-parity fixture for the `meerschaum-cli` crate (`src/format/`).

Run through the reference interpreter from the repository root:

    MRSM_PLUGINS_DIR=$(mktemp -d) scripts/parity/run.sh \
        crates/meerschaum-cli/tests/fixtures/gen_format.py

Output (next to this script): `format.json`, a list of cases. Each case names a function of
`meerschaum.utils.formatting` (or of the shell), its synthetic arguments, the console settings
(`width`, `ansi`, `unicode`) and the exact text the Python implementation printed (or returned).

Determinism:
- `MRSM_ROOT_DIR` points at a fresh temporary directory, so every config value is the default.
- `os.get_terminal_size` always raises, so `get_cols_lines()` falls back to `$COLUMNS`, which is
  set to the case width; the `rich` console is rebuilt per case with an explicit width.
- `formatting._attrs` (`ANSI`, `UNICODE`, `CHARSET`) is set per case.
- Plugins are never loaded and the shell's update checks are stubbed (no network).
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import pathlib
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
_TMP = tempfile.TemporaryDirectory()
### Reuse the venvs of the root `run.sh` provides (packages installed by `attempt_import`
### persist there), but read every config value from a fresh, empty root.
_ORIGINAL_ROOT = os.environ.get('MRSM_ROOT_DIR')
if _ORIGINAL_ROOT and 'MRSM_VENVS_DIR' not in os.environ:
    os.environ['MRSM_VENVS_DIR'] = os.path.join(_ORIGINAL_ROOT, 'venvs')
os.environ['MRSM_ROOT_DIR'] = os.path.join(_TMP.name, 'root')
os.makedirs(os.environ['MRSM_ROOT_DIR'], exist_ok=True)
_PLUGINS_DIR = os.path.join(_TMP.name, 'plugins')
os.makedirs(_PLUGINS_DIR, exist_ok=True)
os.environ['MRSM_PLUGINS_DIR'] = _PLUGINS_DIR
for _key in list(os.environ):
    if _key.startswith('MRSM_') and _key not in (
        'MRSM_ROOT_DIR', 'MRSM_PLUGINS_DIR', 'MRSM_VENVS_DIR', 'MRSM_PARITY_REFERENCE',
    ):
        del os.environ[_key]
os.environ.pop('NO_COLOR', None)
os.environ.pop('TERM', None)


def _no_terminal(*args, **kwargs):
    raise OSError("not a terminal")


os.get_terminal_size = _no_terminal

import meerschaum as mrsm  # noqa: E402
import meerschaum.plugins as _plugins_mod  # noqa: E402

_plugins_mod.load_plugins = lambda *args, **kwargs: None
_plugins_mod.import_plugins = lambda *args, **kwargs: None
_plugins_mod.get_plugins = lambda *args, **kwargs: ()

import meerschaum.utils.formatting as fmt  # noqa: E402
from meerschaum.utils.packages import attempt_import  # noqa: E402

rich_console = attempt_import('rich.console', lazy=False)
pd = attempt_import('pandas', lazy=False)

WIDTHS = [60, 80, 120]
CASES = []


def _configure(width: int, ansi: bool, unicode: bool):
    fmt._attrs['ANSI'] = ansi
    fmt._attrs['UNICODE'] = unicode
    fmt._attrs['CHARSET'] = 'unicode' if unicode else 'ascii'
    fmt.console = rich_console.Console(
        force_terminal=True,
        color_system='truecolor',
        width=width,
    )
    os.environ['COLUMNS'] = str(width)


def _capture(func, *args, **kwargs):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        ret = func(*args, **kwargs)
    return buf.getvalue(), ret


def record(name, args, func, *, settings=None, returns=False):
    """Record `func(**args)` for every console setting."""
    settings = settings or [
        (w, a, u)
        for w in WIDTHS
        for a in (True, False)
        for u in (True, False)
    ]
    for width, ansi, unicode in settings:
        _configure(width, ansi, unicode)
        out, ret = _capture(func)
        CASES.append({
            'fn': name,
            'args': args,
            'width': width,
            'ansi': ansi,
            'unicode': unicode,
            'output': ret if returns else out,
        })


def all_settings(unicode_values=(True, False)):
    return [(w, a, u) for w in WIDTHS for a in (True, False) for u in unicode_values]


### --------------------------------------------------------------------------
### print_tuple / format_success_tuple
### --------------------------------------------------------------------------

TUPLES = [
    [True, "Success"],
    [False, "Failed to sync pipes."],
    [True, "Synced 3 pipes."],
    [True, "Inserted 10, updated 2 rows for Pipe('sql:main', 'weather').\nDone."],
    [False, "Multi-line failure:\nsecond line\n  indented line\n\nafter a blank"],
    [
        False,
        "A very long failure message which certainly exceeds the narrowest console width "
        + "that the fixture renders, so rich wraps it.",
    ],
    [True, "   Success   "],
    [True, "successSucceeded"],
    [False, "Pipe('plugin:noaa', 'weather', 'us', instance='sql:local') failed."],
]

for tup in TUPLES:
    for kwargs in (
        {},
        {'upper_padding': 1},
        {'calm': True},
        {'skip_common': False, 'upper_padding': 1, 'lower_padding': 0},
        {'common_only': True},
    ):
        record(
            'print_tuple',
            {'tuple': tup, **kwargs},
            lambda tup=tup, kwargs=kwargs: fmt.print_tuple(tuple(tup), **kwargs),
        )
    record(
        'format_success_tuple',
        {'tuple': tup, 'upper_padding': 2, 'lower_padding': 1, 'left_padding': 3},
        lambda tup=tup: fmt.format_success_tuple(
            tuple(tup), upper_padding=2, lower_padding=1, left_padding=3,
        ),
        returns=True,
    )


### --------------------------------------------------------------------------
### print_options
### --------------------------------------------------------------------------

SHORT = ['sync', 'show', 'register', 'delete', 'edit']
MANY = [f'option_{i:02d}' for i in range(30)]
LONG = [
    f"a_rather_long_option_name_number_{i}_with_some_padding_text" for i in range(15)
]
MIXED = (
    [f'item{i}' for i in range(10)]
    + ["Pipe('sql:main', 'weather')", "Pipe('plugin:noaa', 'temp', 'us')"]
    + ['x' * 45, 'tiny']
)

OPTIONS_CASES = [
    {'options': SHORT, 'name': 'actions'},
    {'options': SHORT, 'header': 'Installed plugins:'},
    {'options': MANY},
    {'options': MANY, 'sort_options': True, 'name': 'actions'},
    {'options': LONG},
    {'options': MIXED},
    {'options': ['b', 'a', 'c'], 'number_options': True},
    {'options': MANY[:14], 'number_options': True},
    {'options': SHORT, 'nopretty': True},
    {'options': SHORT, 'nopretty': True, 'number_options': True},
    {'options': []},
    {'options': MANY, 'num_cols': 3},
    {'options': LONG, 'adjust_cols': False},
]

for case in OPTIONS_CASES:
    record(
        'print_options',
        case,
        lambda case=case: fmt.print_options(**case),
    )


### --------------------------------------------------------------------------
### pprint
### --------------------------------------------------------------------------

PPRINT_VALUES = [
    {'a': 1, 'b': 'two'},
    {
        'meerschaum': {
            'instance': 'sql:main',
            'repository': 'api:mrsm',
            'connectors': {
                'sql': {
                    'main': {
                        'username': 'mrsm',
                        'password': 'secret',
                        'flavor': 'timescaledb',
                        'host': 'localhost',
                        'database': 'meerschaum',
                        'port': 5432,
                    },
                    'local': {
                        'database': '{MRSM_ROOT_DIR}/sqlite/mrsm_local.db',
                        'flavor': 'sqlite',
                    },
                    'remote': {'uri': 'postgresql://user:hunter2@db.example.com:5432/data'},
                },
                'api': {'mrsm': {'host': 'api.mrsm.io', 'port': 443, 'protocol': 'https'}},
            },
        },
        'flags': [True, False, None],
        'numbers': [0, -1, 3.14, 1e+20, 2.5e-07, 100000000000],
        'empty': {},
        'empty_list': [],
    },
    ['sync', 'pipes', '-c', 'sql:main'],
    {'quotes': "it's", 'both': 'say "hi" it\'s', 'newline': 'line\nnext', 'tab': 'a\tb',
     'unicode': 'café', 'backslash': 'C:\\path'},
    {'long_list': [f'item number {i}' for i in range(12)]},
    {'nested': {'deeper': {'deepest': {'key': 'value', 'n': 1}}}, 'x': [1, [2, [3, [4]]]]},
    {'MRSM_ROOT_DIR': '/tmp/root', 'MRSM_PLUGINS_DIR': '/tmp/plugins'},
    {'url': 'https://example.com/path?q=1', 'ip': '192.168.0.1',
     'uuid': '123e4567-e89b-12d3-a456-426614174000', 'path': '/usr/local/bin/mrsm'},
    {'b_key': 2, 'a_key': 1, 'c_key': {'z': 26, 'y': 25}},
    [],
    {},
]

for value in PPRINT_VALUES:
    for nopretty in (False, True):
        record(
            'pprint',
            {'value': value, 'nopretty': nopretty},
            lambda value=value, nopretty=nopretty: fmt.pprint(value, nopretty=nopretty),
        )

record(
    'pprint_tuple',
    {'tuple': [False, 'Failure from pprint.']},
    lambda: fmt.pprint((False, 'Failure from pprint.')),
)


### --------------------------------------------------------------------------
### make_header, highlight_pipes, pipe_repr, fill_ansi, colored
### --------------------------------------------------------------------------

HEADERS = ['Pipes', '\nActive connectors:', 'Line one\nLonger line two', '']
for msg in HEADERS:
    for kw in ({}, {'left_pad': 4}, {'top': True}, {'ruler': '='}):
        record(
            'make_header',
            {'message': msg, **kw},
            lambda msg=msg, kw=kw: fmt.make_header(msg, **kw),
            returns=True,
            settings=all_settings(),
        )

HIGHLIGHT_MESSAGES = [
    "No pipes here.",
    "Synced Pipe('sql:main', 'weather') in 1.2 seconds.",
    "Pipe('a', 'b', 'c') and Pipe('x', 'y', instance='sql:local') both.",
    "Broken Pipe( without a closing paren",
    "Pipe(not, valid) stays.",
    "Pipe('sql:main', 'weather', instance='sql:main')",
]
for msg in HIGHLIGHT_MESSAGES:
    record(
        'highlight_pipes',
        {'message': msg},
        lambda msg=msg: fmt.highlight_pipes(msg),
        returns=True,
    )

PIPE_DICTS = [
    {'connector_keys': 'sql:main', 'metric_key': 'weather'},
    {'connector_keys': 'plugin:noaa', 'metric_key': 'temp', 'location_key': 'us'},
    {'connector_keys': 'a', 'metric_key': 'b', 'instance_keys': 'sql:local'},
    {'connector_keys': 'a', 'metric_key': 'b', 'location_key': 'c', 'instance_keys': 'api:main'},
]
for pd_ in PIPE_DICTS:
    record(
        'pipe_repr',
        {'pipe': pd_},
        lambda pd_=pd_: fmt.pipe_repr(pd_),
        returns=True,
    )

FILL = [
    ("plain text", "bold red"),
    ("\x1b[32mgreen\x1b[0m then plain", "bold bright_green"),
    ("plain then \x1b[1;35mmagenta\x1b[0m", "bright_magenta"),
    ("a \x1b[33my\x1b[0m b \x1b[34mz\x1b[0m c", "cyan"),
    ("x" * 130, "bold"),
    ("indian red style", "indian red"),
]
for string, style in FILL:
    record(
        'fill_ansi',
        {'string': string, 'style': style},
        lambda string=string, style=style: fmt.fill_ansi(string, style=style),
        returns=True,
        settings=[(w, True, True) for w in WIDTHS],
    )

COLORED = [
    ('hello', {'style': 'bold bright_blue'}),
    ('multi\nline\n', {'style': 'green'}),
    ('x' * 70, {'style': 'cyan'}),
    ('#hex', {'style': '#d177a4'}),
    ('pale', {'style': 'pale_green3'}),
    ('bg', {'style': 'black on magenta'}),
    ('bad', {'style': 'indian red'}),
    ('empty style', {'style': ''}),
]
for text, kw in COLORED:
    record(
        'colored',
        {'text': text, **kw},
        lambda text=text, kw=kw: fmt.colored(text, **kw),
        returns=True,
        settings=[(w, True, True) for w in WIDTHS],
    )


### --------------------------------------------------------------------------
### pprint_pipes
### --------------------------------------------------------------------------

def _pipes_dict(spec):
    out = {}
    for ck, mk, lk, ik in spec:
        out.setdefault(ck, {}).setdefault(mk, {})[lk] = mrsm.Pipe(ck, mk, lk, instance=ik)
    return out


PIPES_SPECS = [
    [('sql:main', 'weather', None, 'sql:main')],
    [
        ('sql:main', 'weather', None, 'sql:main'),
        ('sql:main', 'weather', 'us', 'sql:main'),
        ('sql:main', 'temperature', None, 'sql:main'),
        ('plugin:noaa', 'forecast', 'atlanta', 'sql:main'),
        ('api:remote', 'power', None, 'sql:local'),
    ],
    [
        (f'plugin:source_{i}', f'metric_{j}', None, 'sql:main')
        for i in range(4) for j in range(2)
    ],
]
for spec in PIPES_SPECS:
    record(
        'pprint_pipes',
        {'pipes': [[ck, mk, lk, ik] for ck, mk, lk, ik in spec]},
        lambda spec=spec: fmt.pprint_pipes(_pipes_dict(spec)),
    )


### --------------------------------------------------------------------------
### format_dataframe
### --------------------------------------------------------------------------

FRAMES = [
    {'columns': {'id': [1, 2, 3], 'name': ['a', 'bb', 'ccc'], 'value': [1.5, 22.25, -3.0]}},
    {'columns': {'dt': ['2024-01-01', '2024-01-02'], 'flag': [True, False]}},
    {'columns': {'x': [1, None, 3]}},
    {'columns': {'s': ['x', None, 'zz'], 'f': [0.1, 100.0, None]}},
    {'columns': {'big': [1234567.0, 0.000123, 12.0]}},
    {'columns': {'id': list(range(8))}, 'max_rows': 3},
    {'columns': {'only': ['one']}},
]
for frame in FRAMES:
    df = pd.DataFrame(frame['columns'])
    record(
        'format_dataframe',
        frame,
        lambda df=df, frame=frame: fmt.format_dataframe(df, max_rows=frame.get('max_rows')),
        returns=True,
        settings=[(80, True, True)],
    )


### --------------------------------------------------------------------------
### The shell banner and prompt
### --------------------------------------------------------------------------

import meerschaum._internal.shell.updates as _updates  # noqa: E402

_updates.get_update_message = lambda: ''
_updates.get_plugins_update_message = lambda: ''
_updates.run_version_check_thread = lambda *args, **kwargs: None

import meerschaum._internal.shell.Shell  # noqa: E402,F401

_shell_mod = sys.modules['meerschaum._internal.shell.Shell']

for with_color in (True, False):
    record(
        'shell_intro',
        {'with_color': with_color, 'version': mrsm.__version__},
        lambda with_color=with_color: _shell_mod.get_shell_intro(with_color=with_color),
        returns=True,
        settings=all_settings(),
    )


def _prompt(instance, compose_project=None):
    if compose_project:
        os.environ['MRSM__COMPOSE_CONFIG'] = json.dumps({'project_name': compose_project})
    else:
        os.environ.pop('MRSM__COMPOSE_CONFIG', None)
    _shell_mod.shell_attrs.clear()
    _shell_mod.shell_attrs['session'] = None
    shell = _shell_mod.Shell.__new__(_shell_mod.Shell)
    shell.load_config(instance=instance)
    os.environ.pop('MRSM__COMPOSE_CONFIG', None)
    return shell.prompt


PROMPTS = [
    ('sql:main', None),
    ('sql:local', None),
    ('api:main', None),
    ('sql:main', 'my-project'),
    ('sql:a_very_long_instance_label_for_truncation', None),
    ('sql:main', 'a-compose-project-with-a-very-long-name'),
]
for instance, project in PROMPTS:
    record(
        'shell_prompt',
        {'instance': instance, 'compose_project': project},
        lambda instance=instance, project=project: _prompt(instance, project),
        returns=True,
        settings=all_settings(),
    )

_shell_mod.shell_attrs.clear()


def main():
    plugin_modules = [m for m in sys.modules if m == 'plugins' or m.startswith('plugins.')]
    assert not plugin_modules, plugin_modules
    with open(HERE / 'format.json', 'w', encoding='utf-8') as f:
        json.dump(
            {'version': mrsm.__version__, 'cases': CASES},
            f,
            indent=1,
            ensure_ascii=False,
            allow_nan=False,
        )
        f.write('\n')
    print(f"cases={len(CASES)}")


if __name__ == '__main__':
    main()
