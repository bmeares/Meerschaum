#!/usr/bin/env python3
"""Released-v4 parameter snapshots; no database, plugins, or child processes.
Run with scripts/parity/run.sh crates/meerschaum-core/tests/fixtures/gen_parameter_resolution.py.
"""
import atexit
import copy
import json
import pathlib
import shutil
import _stubs

root = _stubs.isolate()
atexit.register(shutil.rmtree, root, ignore_errors=True)
import meerschaum as mrsm
import meerschaum.config as config
import meerschaum.utils.warnings as warnings
_stubs.install()
warnings.warn = lambda *a, **kw: None
CONFIG = {'fixture': {'name': 'configured', 'number': 17, 'columns': {'id': 'station'}}}
config.set_config(copy.deepcopy(CONFIG))

class SQLStub(_stubs.StubInstance):
    type = 'sql'
    flavor = 'sqlite'
    def get_pipe_schema(self, pipe):
        return None
mrsm.Pipe.instance_connector = property(
    lambda p: SQLStub(p.instance_keys) if p.instance_keys == 'sql:fixture_sqlite'
    else _stubs.StubInstance(p.instance_keys)
)
REGISTERED = [
    {'connector_keys': 'plugin:source', 'metric_key': 'base', 'instance_keys': 'sql:fixture',
     'parameters': {'target': 'source_table', 'columns': {'datetime': 'ts'},
                    'dtypes': {'ts': 'int'}, 'indices': {'time': 'ts'}, 'tags': ['one', 'two']}},
    {'connector_keys': 'plugin:source', 'metric_key': 'base', 'instance_keys': 'sql:other',
     'parameters': {'target': 'other_table', 'columns': {'id': 'other_id'}}},
    {'connector_keys': 'plugin:source', 'metric_key': 'base', 'instance_keys': 'sql:fixture_sqlite',
     'parameters': {'target': 'source table'}},
]
for r in REGISTERED:
    _stubs.register(r['connector_keys'], r['metric_key'], None, r['instance_keys'], r['parameters'])
CASES = [
    ('metric_target', {'target': '{{ self.metric }}'}),
    ('key_aliases', {'target': '{{ self.metric_key }}_{{ self.location_key }}', 'connector': '{{ self.connector_keys }}', 'instance': '{{ self.instance_keys }}'}),
    ('raw_chain', {'a': {'b': [3, True, None]}, 'value': "{{ self.parameters['a']['b'][1] }}"}),
    ('self_columns', {'columns': {'datetime': 'ts'}, 'value': "{{ self.columns['datetime'] }}"}),
    ('self_dtypes', {'dtypes': {'ts': 'int'}, 'value': "{{ self.dtypes['ts'] }}"}),
    ('self_tags', {'tags': ['first', 'second'], 'value': '{{ self.tags[1] }}'}),
    ('columns', {'name': 'event_time', 'columns': {'datetime': "{{ self.parameters['name'] }}"}}),
    ('dtypes', {'kind': 'int', 'dtypes': {'ts': "{{ self.parameters['kind'] }}"}}),
    ('indices', {'name': 'ts', 'indices': {'lookup': ["{{ self.parameters['name'] }}"]}}),
    ('nested', {'nested': [{'metric': '{{ self.metric }}'}, ['{{ self.location }}', '{{ self.metric }}']]}),
    ('whole_map', {'value': "{{ Pipe('plugin:source', 'base').columns }}"}),
    ('whole_list', {'value': "{{ Pipe('plugin:source', 'base').tags }}"}),
    ('reference_columns', {'columns': "{{ Pipe('plugin:source', 'base').columns }}", 'dtypes': "{{ Pipe('plugin:source', 'base').dtypes }}", 'indices': "{{ Pipe('plugin:source', 'base').indices }}"}),
    ('sql_definition', {'sql': "SELECT * FROM {{ Pipe('plugin:source', 'base') }} WHERE {{ Pipe('plugin:source', 'base').columns['datetime'] }} > 0"}),
    ('fetch_definition', {'fetch': {'definition': "SELECT * FROM {{ Pipe('plugin:source', 'base') }}"}}),
    ('cross_instance', {'target': "{{ Pipe('plugin:source', 'base', instance='sql:other').target }}"}),
    ('missing_pipe', {'target': "{{ Pipe('plugin:missing', 'absent') }}", 'columns': "{{ Pipe('plugin:missing', 'absent').columns }}"}),
    ('missing_attr', {'value': "{{ self.parameters['absent'] }}", 'other': '{{ self.nonexistent }}'}),
    ('symlink_string', {'target': 'MRSM{fixture:name}'}),
    ('symlink_values', {'n': 'MRSM{fixture:number}', 'columns': 'MRSM{fixture:columns}'}),
    ('symlink_embedded', {'value': 'prefix_MRSM{fixture:name}_suffix'}),
    ('symlink_nested', {'nested': ['MRSM{fixture:number}', {'value': 'MRSM{fixture:name}'}]}),
    ('symlink_missing', {'value': 'MRSM{fixture:missing}'}),
    ('escaped_braces', {'value': r'\{{ self.metric }}', 'extra': '{{{ self.metric }}}', 'literal': '{ self.metric }'}),
    ('whitespace', {'target': '  {{ self.metric }}\n'}),
    ('embedded_values', {'a': [1, True, None], 'value': "values={{ self.parameters['a'] }}"}),
    ('reference_merge', {'reference': "Pipe('plugin:source', 'base')", 'target': '{{ self.metric }}'}),
]
results = []
for name, params in CASES:
    state = {'connector_keys': 'plugin:fixture', 'metric_key': name, 'location_key': 'north',
             'instance_keys': 'sql:fixture', 'parameters': copy.deepcopy(params)}
    pipe = mrsm.Pipe(**state)
    results.append({'state': state, 'parameters': copy.deepcopy(pipe.parameters), 'target': pipe.target})
state = {'connector_keys': 'plugin:fixture', 'metric_key': 'sql_quoted',
         'instance_keys': 'sql:fixture_sqlite',
         'parameters': {'sql': "SELECT * FROM {{ Pipe('plugin:source', 'base') }}"}}
pipe = mrsm.Pipe(**state)
results.append({'state': state, 'parameters': pipe.parameters, 'target': pipe.target})
path = pathlib.Path(__file__).with_name('parameter_resolution.json')
path.write_text(json.dumps({'config': CONFIG, 'registered': REGISTERED, 'cases': results}, indent=2) + '\n')
