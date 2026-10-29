#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate `pipes.json`: pure-data `Pipe` attributes for synthetic constructor calls, plus
`truncate_item_name` results.

Run from the repository root:

    PYTHONPATH=$(pwd) python crates/meerschaum-core/tests/fixtures/gen_pipes.py

Each case is a constructor call (`args`, `kwargs`). The instance is stubbed (see `_stubs.py`), so
`dtypes` are the configured dtypes and `target` is not truncated. `indices['unique']` is sorted
because Python builds it from a `set`. Timedeltas are written as microseconds.
"""

import json
import pathlib
import sys
from datetime import timedelta

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
import _stubs  # noqa: E402

_stubs.isolate()

import meerschaum as mrsm  # noqa: E402
from meerschaum.utils.sql import truncate_item_name  # noqa: E402

_stubs.install()

CASES = [
    {'args': ['sql:main', 'weather']},
    {'args': ['plugin:noaa', 'weather', 'us'], 'kwargs': {'instance': 'sql:local', 'columns': ['ts', 'station']}},
    {'args': ['sql:main', 'temps'], 'kwargs': {'parameters': {
        'columns': {'datetime': 'ts', 'id': 'station', 'value': None, '': 'x', 'primary': 'pk'},
        'dtypes': {'val': 'decimal', 'meta': 'JSON', 'uid': 'guid', 'shape': 'geom', 'flag': 'boolean', 'n': None},
        'indexes': {'geo': ['lat', 'lon']},
        'tags': ['production', 'hourly'],
        'upsert': True,
        'autoincrement': True,
    }}},
    {'args': ['sql:main', 'named'], 'kwargs': {'parameters': {'target_name': 'custom_table', 'target_table': 'ignored'}}},
    {'args': ['sql:main', 'named2'], 'kwargs': {'parameters': {'target_table_name': ''}}},
    {'args': ['sql:main', 'explicit'], 'kwargs': {'target': 'my_target', 'tags': ['a']}},
    {'args': ['sql:main', 'noloc', 'None']},
    {'args': ['sql:main', 'noloc2', '[None]']},
    {'args': ['sql:main', 'emptyloc', '']},
    {'kwargs': {'connector_keys': 'api:main', 'metric_key': 'm', 'location_key': 'l', 'instance_keys': 'api:other'}},
    {'args': ['plugin:x', 'intaxis'], 'kwargs': {'precision': 'ms', 'parameters': {
        'columns': {'datetime': 'id'}, 'dtypes': {'id': 'int'}, 'verify': {'chunk_days': 1}}}},
    {'args': ['plugin:x', 'chunkrange'], 'kwargs': {'parameters': {
        'columns': {'datetime': 'id'}, 'dtypes': {'id': 'Int64'}, 'verify': {'chunk_range': 5000, 'chunk_days': 1}}}},
    {'args': ['plugin:x', 'intlegacy'], 'kwargs': {'parameters': {
        'columns': {'datetime': 'epoch'}, 'dtypes': {'epoch': 'int'}, 'verify': {'chunk_hours': 2.5}}}},
    {'args': ['plugin:x', 'years'], 'kwargs': {'parameters': {'verify': {'chunk_years': 1, 'chunk_seconds': 5}}}},
    {'args': ['plugin:x', 'weeks'], 'kwargs': {'parameters': {'columns': {'datetime': 'ts'}, 'verify': {'chunk_weeks': 0.5}}}},
    {'args': ['plugin:x', 'seconds'], 'kwargs': {'parameters': {'columns': {'datetime': 'ts'}, 'verify': {'chunk_seconds': 90.0000005}}}},
    {'args': ['plugin:x', 'ms'], 'kwargs': {'parameters': {'columns': {'datetime': 'ts'}, 'dtypes': {'ts': 'datetime64[ms, UTC]'}}}},
    {'args': ['plugin:x', 'int32'], 'kwargs': {'parameters': {'columns': {'datetime': 'ts'}, 'dtypes': {'ts': 'int32'}}}},
    {'args': ['plugin:x', 'dateaxis'], 'kwargs': {'parameters': {'columns': {'datetime': 'd'}, 'dtypes': {'d': 'date'}}}},
    {'args': ['plugin:x', 'autotime'], 'kwargs': {'autotime': True, 'dtypes': {'ts': 'datetime64[ns]'}}},
    {'args': ['plugin:x', 'precdict'], 'kwargs': {'precision': {'unit': 'h', 'interval': 15}, 'columns': {'datetime': 'ts'}}},
    {'args': ['plugin:x', 'precbad'], 'kwargs': {'precision': 'fortnight', 'columns': {'datetime': 'ts'}}},
    {'args': ['plugin:x', 'timestamp'], 'kwargs': {'columns': {'datetime': 'ts'}, 'dtypes': {'ts': 'timestamp'}}},
    {'args': ['plugin:x', 'intprec'], 'kwargs': {'precision': 'minute', 'parameters': {
        'columns': {'datetime': 'id'}, 'dtypes': {'id': 'int'}, 'verify': {'chunk_minutes': 30}}}},
    {'args': ['sql:main', 'graph'], 'kwargs': {'parameters': {
        'parents': [
            "Pipe('sql:main', 'a')",
            {'connector_keys': 'sql:main', 'metric_key': 'b', 'location_key': None, 'instance_keys': 'sql:other'},
            {'connector': 'plugin:c', 'metric': 'c', 'location': 'here'},
            'mrsm.Pipe("plugin:d", "d", instance="sql:x") extra text',
            'not a pipe',
            {'metric': 'missing_connector'},
        ],
        'child': 'Pipe("sql:main", "kid", "loc")',
        'reference': {'connector': 'sql:main', 'metric': 'base'},
    }}},
    {'args': ['sql:main', 'graph2'], 'kwargs': {'instance': 'sql:inst', 'parent': "Pipe('a', 'b')", 'children': [{'connector': 'c', 'metric': 'd', 'instance': 'sql:main'}]}},
    {'args': ['sql:main', 'flags'], 'kwargs': {
        'static': True, 'enforce': False, 'null_indices': False, 'mixed_numerics': False,
        'compress': {'after': 10}, 'upsert': False}},
    {'args': ['sql:main', 'templ'], 'kwargs': {'columns': "{{ Pipe('sql:main', 'other').columns }}"}},
    {'args': ['sql:main', 'idx'], 'kwargs': {'indices': {'a': 'b'}, 'parameters': {'indexes': {'old': 'x'}, 'columns': {'id': 'x'}}}},
    {'args': ['sql:main', 'idx2'], 'kwargs': {'indexes': {'a': ['b', 'c']}, 'columns': {'datetime': 'dt', 'id': 'dt'}}},
    {'args': ['sql:main', 'memory'], 'kwargs': {'instance': 'sql:memory'}},
    {'args': ['', 'm']},
    {'args': ['_sql', 'm']},
    {'args': ['sql:main', 'm'], 'kwargs': {'tags': ['ok', '_negated']}},
    {'args': ['sql:main', 'm'], 'kwargs': {'foo': 1}},
    {'args': ['sql:main', 'm'], 'kwargs': {'connector': 'x'}},
]

TRUNCATE = [
    ('sql_main_weather', 'postgresql'),
    ('a' * 63, 'postgresql'),
    ('a' * 64, 'postgresql'),
    ('plugin_noaa_' + 'x' * 40 + '_' + 'y' * 40, 'postgresql'),
    ('plugin_noaa_' + 'x' * 40 + '_' + 'y' * 40, 'oracle'),
    ('plugin_noaa_' + 'x' * 40 + '_' + 'y' * 40, 'mssql'),
    ('plugin_noaa_' + 'x' * 40 + '_' + 'y' * 40, 'duckdb'),
    ('a_b_c_d_e_f_g_h_i_j_k_l_m_n_o_p_q_r_s_t_u_v_w_x_y_z_a_b_c_d_e_f_g', 'oracle'),
    ('é' * 70, 'mysql'),
]


def micros(td):
    return td // timedelta(microseconds=1)


def attempt(func):
    try:
        return {'ok': func()}
    except Exception as e:
        return {'error': type(e).__name__}


def chunk(pipe, arg):
    value = pipe.get_chunk_interval(arg)
    if isinstance(value, timedelta):
        return {'delta_us': micros(value)}
    return {'int': value}


def refs(pipes):
    return [p.keys() if p is not None else None for p in pipes]


def describe(pipe):
    out = {
        'str': str(pipe),
        'keys': pipe.keys(),
        'state': pipe.__getstate__(),
        'parameters': pipe._attributes['parameters'],
        'columns': pipe.columns,
        'dtypes': pipe.dtypes,
        'tags': pipe.tags,
        'target': pipe.target,
        'flags': {
            'upsert': pipe.upsert,
            'static': pipe.static,
            'autoincrement': pipe.autoincrement,
            'autotime': pipe.autotime,
            'enforce': pipe.enforce,
            'null_indices': pipe.null_indices,
            'mixed_numerics': pipe.mixed_numerics,
        },
        'cache': pipe.cache,
    }
    indices = dict(pipe.indices)
    if 'unique' in indices:
        indices['unique'] = sorted(indices['unique'])
    out['indices'] = indices
    out['precision'] = attempt(lambda: dict(pipe.precision))
    out['chunk'] = {
        'none': attempt(lambda: chunk(pipe, None)),
        'delta_90m': attempt(lambda: chunk(pipe, timedelta(minutes=90))),
        'int_7': attempt(lambda: chunk(pipe, 7)),
    }
    out['parents'] = attempt(lambda: refs(pipe.parents))
    out['children'] = attempt(lambda: refs(pipe.children))
    out['references'] = attempt(lambda: refs(pipe.references))
    return out


def main():
    results = []
    for case in CASES:
        args = case.get('args', [])
        kwargs = case.get('kwargs', {})
        try:
            pipe = mrsm.Pipe(*args, **kwargs)
        except Exception as e:
            results.append({'case': case, 'error': type(e).__name__})
            continue
        results.append({'case': case, 'pipe': describe(pipe)})

    truncations = []
    for item, flavor in TRUNCATE:
        try:
            truncations.append({'item': item, 'flavor': flavor, 'ok': truncate_item_name(item, flavor)})
        except Exception as e:
            truncations.append({'item': item, 'flavor': flavor, 'error': type(e).__name__})

    with open(HERE / 'pipes.json', 'w', encoding='utf-8') as f:
        json.dump({'cases': results, 'truncate': truncations}, f, indent=2, ensure_ascii=False)
        f.write('\n')


if __name__ == '__main__':
    main()
