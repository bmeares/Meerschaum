#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate the Valkey parity fixtures for `meerschaum-valkey`.

Runs the Python `ValkeyConnector` (v4.1.4, this repository) against the Valkey test container
(`tests/docker-compose.yaml`, port 6399) and records, after every step of a synthetic pipe
sequence, the full key/value state of a dedicated database (index 11) plus each method's return
value. The Rust integration tests replay the same sequence and compare.

Usage (from the repository root):

    PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \
        python crates/meerschaum-valkey/tests/fixtures/gen_valkey.py

If the container is not running: `docker compose -f tests/docker-compose.yaml up -d valkey`.
All data is synthetic.
"""

import json
import os
import pathlib
from datetime import datetime, timezone

import meerschaum as mrsm
import meerschaum.utils.misc as misc

HERE = pathlib.Path(__file__).parent
OUT = HERE / 'valkey_sequence.json'
HOST = os.environ.get('MRSM_VALKEY_FIXTURE_HOST', 'localhost')
PORT = int(os.environ.get('MRSM_VALKEY_FIXTURE_PORT', '6399'))
DB = int(os.environ.get('MRSM_VALKEY_FIXTURE_DB', '11'))

### Deterministic user IDs (`register_user` calls `generate_password(12)`).
_ids = iter(f"fixtureid{i:03d}" for i in range(1000))
misc.generate_password = lambda *a, **k: next(_ids)



TRACED = [
    'register_pipe', 'get_pipe_id', 'get_pipe_attributes', 'edit_pipe', 'pipe_exists',
    'drop_pipe', 'delete_pipe', 'get_pipe_data', 'sync_pipe', 'get_pipe_columns_types',
    'clear_pipe', 'get_sync_time', 'get_pipe_rowcount', 'fetch_pipes_keys',
    'register_user', 'get_user_id', 'edit_user', 'get_user_attributes', 'delete_user',
    'get_users', 'get_user_password_hash', 'get_user_type',
    'register_plugin', 'get_plugin_id', 'get_plugin_version', 'get_plugin_user_id',
    'get_plugin_username', 'get_plugin_attributes', 'delete_plugin', 'get_plugins',
]
_depth = [0]
_calls = []


def tag(x):
    """Encode a Python value as JSON with type tags for rich scalars."""
    import math
    import uuid
    import decimal
    import base64
    from datetime import date
    import pandas as pd
    if isinstance(x, mrsm.Pipe):
        return {'__pipe__': {
            'connector_keys': x.connector_keys,
            'metric_key': x.metric_key,
            'location_key': x.location_key,
            'instance_keys': str(x.instance_keys),
            'parameters': tag(x.get_parameters(apply_symlinks=False)),
            'temporary': bool(x.temporary),
        }}
    if isinstance(x, mrsm.core.User):
        return {'__user__': {
            'username': x.username,
            'password': x.password,
            'email': x.email,
            'type': x.type,
            'attributes': tag(x.attributes),
            'user_id': tag(x.user_id),
            'password_hash': x._password_hash,
        }}
    if isinstance(x, mrsm.core.Plugin):
        return {'__plugin__': {
            'name': x.name,
            'version': x.version,
            'user_id': tag(x.user_id),
            'attributes': tag(x.attributes),
        }}
    if isinstance(x, pd.DataFrame):
        return {'__df__': {
            'columns': [str(c) for c in x.columns],
            'dtypes': {str(c): str(t) for c, t in x.dtypes.items()},
            'data': {str(c): [tag(v) for v in x[c].tolist()] for c in x.columns},
        }}
    if x is None:
        return None
    if x is pd.NaT:
        return {'t': 'nat'}
    if x is pd.NA:
        return {'t': 'na'}
    if isinstance(x, bool):
        return x
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        if math.isnan(x):
            return {'t': 'nan'}
        return x
    if isinstance(x, str):
        return x
    if hasattr(x, 'tzinfo') and hasattr(x, 'isoformat'):
        return {'t': 'dt', 'v': x.isoformat()}
    if isinstance(x, date):
        return {'t': 'date', 'v': x.isoformat()}
    if isinstance(x, decimal.Decimal):
        return {'t': 'dec', 'v': str(x)}
    if isinstance(x, uuid.UUID):
        return {'t': 'uuid', 'v': str(x)}
    if isinstance(x, bytes):
        return {'t': 'bytes', 'v': base64.b64encode(x).decode()}
    if isinstance(x, tuple):
        return [tag(v) for v in x]
    if isinstance(x, list):
        return [tag(v) for v in x]
    if isinstance(x, dict):
        return {str(k): tag(v) for k, v in x.items()}
    if hasattr(x, 'item'):
        return tag(x.item())
    return {'t': 'repr', 'v': repr(x)}


def install_tracer():
    from meerschaum.connectors.valkey import ValkeyConnector
    for name in TRACED:
        original = getattr(ValkeyConnector, name)

        def make(name, original):
            def wrapper(self, *args, **kwargs):
                if _depth[0] > 0:
                    return original(self, *args, **kwargs)
                _depth[0] += 1
                try:
                    call = {
                        'method': name,
                        'args': [tag(a) for a in args],
                        'kwargs': {k: tag(v) for k, v in kwargs.items() if k != 'debug'},
                    }
                    result = original(self, *args, **kwargs)
                    call['result'] = tag(result)
                    _calls.append(call)
                    return result
                finally:
                    _depth[0] -= 1
            return wrapper

        setattr(ValkeyConnector, name, make(name, original))


def dump(conn):
    """Return every key of the fixture database with its type and value."""
    client = conn.client
    state = {}
    for key_bytes in sorted(client.keys('*')):
        key = key_bytes.decode('utf-8')
        typ = client.type(key).decode('utf-8')
        if typ == 'string':
            val = client.get(key).decode('utf-8')
        elif typ == 'zset':
            val = [
                [member.decode('utf-8'), score]
                for member, score in client.zrange(key, 0, -1, withscores=True)
            ]
        elif typ == 'set':
            val = sorted(m.decode('utf-8') for m in client.smembers(key))
        elif typ == 'list':
            val = [m.decode('utf-8') for m in client.lrange(key, 0, -1)]
        elif typ == 'hash':
            val = {k.decode('utf-8'): v.decode('utf-8') for k, v in client.hgetall(key).items()}
        else:
            val = None
        state[key] = {'type': typ, 'value': val}
    return state


def df_docs(df):
    """Serialize a DataFrame result as JSON-safe docs with dtypes."""
    if df is None:
        return None
    from meerschaum.utils.dtypes import json_serialize_value
    return {
        'dtypes': {str(k): str(v) for k, v in df.dtypes.items()},
        'docs': json.loads(json.dumps(
            df.to_dict(orient='records'),
            default=json_serialize_value,
        )),
    }


def jsonable(x):
    from meerschaum.utils.dtypes import json_serialize_value
    return json.loads(json.dumps(x, default=json_serialize_value))


def main():
    conn = mrsm.get_connector('valkey:fixture', host=HOST, port=PORT, db=DB)
    conn.client.flushdb()
    steps = []

    install_tracer()

    def record(name, result=None, **extra):
        calls = list(_calls)
        _calls.clear()
        steps.append({
            'step': name,
            'result': jsonable(result),
            'calls': calls,
            'state': dump(conn),
            **extra,
        })

    temp = mrsm.Pipe(
        'demo', 'temperature', 'home',
        instance=conn,
        columns={'datetime': 'ts', 'id': 'station'},
    )
    record('register_temperature', temp.register())

    docs1 = [
        {'ts': '2024-01-01 00:00:00', 'station': 'a', 'val': 1.5},
        {'ts': '2024-01-01 00:00:00', 'station': 'b', 'val': 2.0},
        {'ts': '2024-01-02 12:30:00', 'station': 'a', 'val': 3.25},
    ]
    record('sync_temperature_1', temp.sync(docs1), docs=docs1)

    docs2 = [
        {'ts': '2024-01-02 12:30:00', 'station': 'a', 'val': 30.0},
        {'ts': '2024-01-03 00:00:00', 'station': 'c', 'val': -4.0},
    ]
    record('sync_temperature_2', temp.sync(docs2), docs=docs2)

    queries = []
    for kwargs in [
        {},
        {'params': {'station': 'a'}},
        {'params': {'station': ['a', 'c']}},
        {'params': {'station': '_a'}},
        {'begin': '2024-01-02', 'end': '2024-01-03'},
        {'begin': '2024-01-02'},
        {'select_columns': ['ts', 'val']},
        {'omit_columns': ['val']},
    ]:
        call = {
            k: (datetime.fromisoformat(v).replace(tzinfo=timezone.utc) if k in ('begin', 'end') else v)
            for k, v in kwargs.items()
        }
        queries.append({
            'kwargs': kwargs,
            'get_data': df_docs(temp.get_data(**call)),
            'rowcount': temp.get_rowcount(**{
                k: v for k, v in call.items() if k in ('begin', 'end', 'params')
            }),
        })
    record(
        'queries_temperature',
        {
            'queries': queries,
            'sync_time_newest': temp.get_sync_time(newest=True, round_down=False),
            'sync_time_oldest': temp.get_sync_time(newest=False, round_down=False),
            'columns_types': conn.get_pipe_columns_types(temp),
            'exists': temp.exists(),
            'id': temp.id,
            'attributes': conn.get_pipe_attributes(temp),
        },
    )

    humid = mrsm.Pipe('demo', 'humidity', instance=conn, columns={'id': 'room'})
    record('register_humidity', humid.register())
    docs_h = [
        {'room': 'kitchen', 'pct': 40},
        {'room': 'garage', 'pct': 55},
    ]
    record('sync_humidity_1', humid.sync(docs_h), docs=docs_h)
    docs_h2 = [{'room': 'kitchen', 'pct': 42}, {'room': 'attic', 'pct': 61}]
    record('sync_humidity_2', humid.sync(docs_h2), docs=docs_h2)
    record('get_data_humidity', df_docs(humid.get_data()))

    counter = mrsm.Pipe(
        'demo', 'counter',
        instance=conn,
        columns={'datetime': 'step', 'id': 'name'},
        dtypes={'step': 'int'},
        upsert=True,
    )
    record('register_counter', counter.register())
    docs_c = [{'step': 1, 'name': 'x', 'n': 10}, {'step': 2, 'name': 'x', 'n': 20}]
    record('sync_counter_1', counter.sync(docs_c), docs=docs_c)
    docs_c2 = [{'step': 2, 'name': 'x', 'n': 25}, {'step': 3, 'name': 'y', 'n': 30}]
    record('sync_counter_2', counter.sync(docs_c2), docs=docs_c2)
    record(
        'queries_counter',
        {
            'get_data': df_docs(counter.get_data()),
            'sync_time_newest': counter.get_sync_time(newest=True),
            'sync_time_oldest': counter.get_sync_time(newest=False),
            'rowcount': counter.get_rowcount(),
            'rowcount_begin': counter.get_rowcount(begin=2),
        },
    )

    keys_queries = []
    for kwargs in [
        {},
        {'connector_keys': ['demo']},
        {'metric_keys': ['humidity']},
        {'location_keys': ['home']},
        {'metric_keys': ['_humidity']},
    ]:
        keys_queries.append({
            'kwargs': kwargs,
            'keys': [list(k) for k in conn.fetch_pipes_keys(**kwargs)],
        })
    record('fetch_pipes_keys', keys_queries)

    temp.parameters['tags'] = ['outdoor']
    record('edit_temperature', temp.edit())
    record('fetch_pipes_keys_tags', [
        list(k) for k in conn.fetch_pipes_keys(tags=['outdoor'])
    ])

    record(
        'clear_temperature',
        temp.clear(begin=datetime(2024, 1, 2, tzinfo=timezone.utc)),
    )
    record('clear_humidity_params', humid.clear(params={'room': 'garage'}))
    record('drop_humidity', humid.drop())
    record('delete_counter', counter.delete())

    user = mrsm.core.User('fixture_user', 'fixture_password', email='user@example.com', type='admin')
    user._password_hash = 'pbkdf2-sha256$1$c2FsdA$aGFzaA'
    record('register_user', conn.register_user(user))
    record('user_queries', {
        'user_id': conn.get_user_id(user),
        'users': conn.get_users(),
        'type': conn.get_user_type(user),
        'attributes': conn.get_user_attributes(user),
        'password_hash': conn.get_user_password_hash(user),
    })
    user.email = 'new@example.com'
    record('edit_user', conn.edit_user(user))

    plugin = mrsm.core.Plugin('fixture_plugin', version='1.2.3', user_id=user.user_id)
    plugin._attributes = {'description': 'synthetic'}
    record('register_plugin', conn.register_plugin(plugin))
    record('plugin_queries', {
        'id': conn.get_plugin_id(plugin),
        'version': conn.get_plugin_version(plugin),
        'user_id': conn.get_plugin_user_id(plugin),
        'username': conn.get_plugin_username(plugin),
        'attributes': conn.get_plugin_attributes(plugin),
        'plugins': conn.get_plugins(),
    })
    record('delete_plugin', conn.delete_plugin(plugin))
    record('delete_user', conn.delete_user(user))

    record('drop_temperature', temp.drop())
    record('delete_temperature', temp.delete())
    record('delete_humidity', humid.delete())

    OUT.write_text(json.dumps({
        'uri': conn.URI,
        'db': DB,
        'steps': steps,
    }, indent=2, sort_keys=False) + '\n')
    conn.client.flushdb()
    print(f"Wrote {len(steps)} steps to {OUT}.")


if __name__ == '__main__':
    main()
