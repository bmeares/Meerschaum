#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate `templates.json`: `replace_pipes_syntax` results for synthetic SQL/parameter strings,
and resolved `Pipe.parameters` for pipes that use templates and references.

Run from the repository root:

    PYTHONPATH=$(pwd) python crates/meerschaum-core/tests/fixtures/gen_templates.py

Referenced pipes are stub pipes registered in memory (see `_stubs.py`); their instance is not a
SQL connector, so a bare `{{ Pipe(...) }}` becomes `pipe.target`. `failures` counts the
"Failed to resolve pipe syntax" warnings.
"""

import json
import pathlib
import sys

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
import _stubs  # noqa: E402

_stubs.isolate()

import meerschaum as mrsm  # noqa: E402
import meerschaum.utils.warnings as _warnings  # noqa: E402
from meerschaum.utils.pipes import replace_pipes_syntax  # noqa: E402

_stubs.install()

REGISTERED = [
    ('sql:main', 'src', None, {
        'columns': {'datetime': 'ts', 'id': 'station'},
        'dtypes': {'value': 'float64', 'meta': 'JSON'},
        'target': 'source_table',
        'tags': ['raw', 'hourly'],
        'upsert': True,
        'fetch': {'backtrack_minutes': 30},
        'n': 7,
        'f': 1.5,
        'big': 1e16,
    }),
    ('sql:main', 'idx', None, {'columns': {'datetime': 'ts'}, 'indices': {'geo': 'shape'}}),
    ('sql:main', 'uni', None, {'columns': {'id': 'estación'}}),
    ('sql:main', 'clone', None, {
        'columns': "{{ Pipe('sql:main', 'src').columns }}",
        'dtypes': "{{ Pipe('sql:main', 'src').dtypes }}",
        'sql': "SELECT * FROM {{ Pipe('sql:main', 'src') }}",
    }),
    ('sql:main', 'clone2', None, {
        'columns': "{{ Pipe('sql:main', 'clone').columns }}",
        'target': 'clone_of_clone',
    }),
    ('sql:main', 'child', None, {
        'reference': "Pipe('sql:main', 'src')",
        'target': 'child_table',
        'tags': ['child'],
    }),
    ('plugin:me', 'me', None, {'a': {'b': 5}, 'x': 'y'}),
]

SELF = ('plugin:me', 'me', None)

TEXTS = [
    ("SELECT * FROM {{ Pipe('sql:main', 'src') }}", None),
    ("{{ Pipe('sql:main', 'src').columns }}", None),
    ("  {{ Pipe('sql:main', 'src').columns }}\n", None),
    ("x={{ Pipe('sql:main','src').columns['datetime'] }} y={{ Pipe('sql:main', 'src').columns }}", None),
    ("{{ Pipe('sql:main', 'src').dtypes }}", None),
    ("{{ Pipe('sql:main', 'src').target }}", None),
    ("{{ Pipe('sql:main', 'idx').indices }}", None),
    ("tags: {{ Pipe('sql:main', 'src').tags }}", None),
    ("{{ Pipe('sql:main', 'src').parameters['fetch']['backtrack_minutes'] }}", None),
    ("{{ Pipe('sql:main', 'src')['columns'] }}", None),
    ("{{ Pipe('sql:main', 'src')['metric'] }}", None),
    ("{{Pipe('sql:main','src').metric}}-{{ Pipe('sql:main', 'src').connector_keys }}", None),
    ("{{ mrsm.Pipe('sql:main', 'src', instance='sql:other') }}", None),
    ("{{ Pipe(\"sql:main\", \"src\", None) }}", None),
    ("u={{ Pipe('sql:main','src').upsert }} n={{ Pipe('sql:main','src').parameters['n'] }} f={{ Pipe('sql:main','src').parameters['f'] }} big={{ Pipe('sql:main','src').parameters['big'] }}", None),
    ("{{ Pipe('sql:main', 'src').parameters['big'] }}", None),
    ("{{ Pipe('sql:main', 'src')[-1] }}", None),
    ("{{ Pipe('sql:main', 'src').columns['missing'] }}", None),
    ("{{ Pipe() }}", None),
    ("{{ Pipe('only_connector') }}", None),
    ("{{ Pipe('sql:main', 'src').columns.datetime }}", None),
    ("{{ Pipe('sql:main', 'src').tags[1] }}", None),
    ("{{ Pipe('sql:main', 'src').tags[5] }}", None),
    ("{{ Pipe('sql:main', 'src').target[0] }}", None),
    ("cols={{ Pipe('sql:main', 'uni').columns }}", None),
    ("{{ Pipe('sql:main', 'clone').columns }}", None),
    ("{{ Pipe('sql:main', 'clone2').columns }}", None),
    ("{{ Pipe('sql:main', 'clone').parameters['sql'] }}", None),
    ("{{ Pipe('sql:main', 'child').columns['id'] }}", None),
    ("{{ Pipe('sql:main', 'child').tags }}", None),
    ("{{ Pipe('sql:main', 'child') }}", None),
    ("{{ Pipe('sql:main','src') }},{{ Pipe('sql:main','src') }}", None),
    ("no templates here", None),
    ("{{ self.metric }}", None),
    ("{{ self.parameters['a']['b'] }}", SELF),
    ("m={{ self.metric }} c={{ self.connector_keys }}", SELF),
    ("{{ self.parameters }}", SELF),
    ("{{ self['parameters']['x'] }}", SELF),
    ("{{ self }}", SELF),
    ("{{ self.location }}", SELF),
    ("loc={{ self.location }}", SELF),
    ("{{ Pipe('myself', 'x') }}", SELF),
    ("{{ Pipe('sql:main', 'src') }} and {{ self.metric }}", SELF),
    ("{{ Pipe('plugin:me', 'me').parameters['a'] }}", SELF),
    ("{{ Pipe('plugin:me', 'me', instance='sql:main').parameters['x'] }}", SELF),
    ("{{ self.parameters_x }}", SELF),
    ("{{ Pipe('sql:main', 'src').columns }}", SELF),
]

RESOLVE = [
    ('sql:main', 'clone', None),
    ('sql:main', 'clone2', None),
    ('sql:main', 'child', None),
    ('plugin:me', 'me', None),
    ('sql:main', 'selfref', None),
]


def main():
    for ck, mk, lk, params in REGISTERED:
        _stubs.register(ck, mk, lk, 'sql:main', params)
    _stubs.register('sql:main', 'selfref', None, 'sql:main', {
        'sql': 'SELECT * FROM {{ self }} WHERE x = {{ self.parameters["x"] }}',
        'x': 1,
        'target': 'self_table',
    })

    failures = []
    real_warn = _warnings.warn

    def counting_warn(msg, *args, **kwargs):
        if str(msg).startswith('Failed to resolve pipe syntax'):
            failures.append(msg)

    _warnings.warn = counting_warn

    results = []
    for text, self_keys in TEXTS:
        failures.clear()
        self_pipe = mrsm.Pipe(*self_keys) if self_keys else None
        value = replace_pipes_syntax(text, _pipe=self_pipe)
        results.append({
            'text': text,
            'self': list(self_keys) if self_keys else None,
            'value': value,
            'failures': len(failures),
        })

    resolved = []
    for keys in RESOLVE:
        failures.clear()
        pipe = mrsm.Pipe(*keys)
        resolved.append({
            'keys': list(keys),
            'parameters': pipe.parameters,
            'failures': len(failures),
        })

    _warnings.warn = real_warn
    with open(HERE / 'templates.json', 'w', encoding='utf-8') as f:
        json.dump({
            'registered': [
                [ck, mk, lk, params] for ck, mk, lk, params in REGISTERED
            ] + [['sql:main', 'selfref', None, _stubs.REGISTERED[('sql:main', 'selfref', None, 'sql:main')]]],
            'replace': results,
            'resolve': resolved,
        }, f, indent=2, ensure_ascii=False)
        f.write('\n')


if __name__ == '__main__':
    main()
