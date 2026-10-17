#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Wave 16 (task S): `meerschaum.connectors.valkey.ValkeyConnector` is a façade over the Rust client
(`meerschaum._core.ValkeyConnector`).

The key helpers and URIs are compared with the released 4.1.4 (`.venv-ref`; it needs no `valkey`
package for them). The commands run against the disposable container on port 6399 when the
`valkey` flavor is selected (`MRSM_TEST_FLAVORS=...,valkey`); every key a test writes starts with
`w16s` and is deleted afterwards (no `FLUSHDB`).
"""

from __future__ import annotations

import os
import sys
import json
import uuid
import socket
import pathlib
import subprocess
import tempfile
from datetime import datetime, timezone

import pytest

import meerschaum as mrsm
from meerschaum.connectors.valkey import ValkeyConnector

REPO = pathlib.Path(__file__).resolve().parents[1]
REF_PYTHON = REPO / '.venv-ref' / 'bin' / 'python'
PORT = 6399
FLAVOR_SELECTED = 'valkey' in os.environ.get('MRSM_TEST_FLAVORS', 'sqlite').split(',')

pytestmark = pytest.mark.skipif(
    not hasattr(__import__('meerschaum._core', fromlist=['x']), 'ValkeyConnector'),
    reason="the extension with `ValkeyConnector` is not built (scripts/rust/build_py.sh)",
)


def _valkey(label, **kw):
    """A `valkey:<label>` connector the registry knows."""
    return mrsm.get_connector('valkey', label, **kw)


def _scratch() -> str:
    base = pathlib.Path.home() / '.cache' / 'mrsm-w16-s'
    base.mkdir(parents=True, exist_ok=True)
    return str(base)


def _v4(code: str, venvs: bool = False) -> str:
    if not REF_PYTHON.exists():
        pytest.skip("the 4.1.4 reference is not built (scripts/parity/setup_reference.sh)")
    root = tempfile.mkdtemp(prefix='mrsm-w16s-ref-', dir=_scratch())
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('PYTHONPATH', 'TMUX', 'TMUX_PANE')
    }
    env.update({
        'MRSM_ROOT_DIR': root,
        'MRSM_PLUGINS_DIR': str(pathlib.Path(root) / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
    })
    ### The packages the v4 connector needs (`valkey`, `pandas`) live in the shared test venv.
    shared = REPO / 'test_root' / 'venvs'
    if venvs and (shared / 'mrsm').is_dir():
        env['MRSM_VENVS_DIR'] = str(shared)
    result = subprocess.run(
        [str(REF_PYTHON), '-s', '-E', '-c', code],
        env=env, cwd=root, capture_output=True, text=True, timeout=180,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return result.stdout


# ---------------------------------------------------------------------------------------------
# No server


def test_keys_and_uris_match_v4():
    ref = json.loads(_v4(
        "import json\n"
        "from meerschaum.connectors.valkey import ValkeyConnector as V\n"
        "c = V(label='w16s', host='h', port=6380, username='u', password='p@ss', db=2)\n"
        "d = V(label='w16s2', host='h')\n"
        "e = V(label='w16s3', uri='valkey://x:y@h2:1/3')\n"
        "doc = {'dt': 1, 'id': 'a:b', 'val': 2.5}\n"
        "print(json.dumps({\n"
        "  'uri': [c.URI, d.URI, e.URI],\n"
        "  'quote': [V.quote_table(t) for t in ('a', 'a b', \"it's\", '', 'x/y:z')],\n"
        "  'counter': V.get_counter_key('a b'),\n"
        "  'dt_key': c.get_datetime_column_key('a b'),\n"
        "  'entity': V.get_entity_key('a', 'b', 'c'),\n"
        "  'user': [V.get_user_key('abc', 'attributes'), V.get_user_key('u', 'user_id', True)],\n"
        "  'plugin': V.get_plugin_key('p', 'version'),\n"
        "  'docs': [V.get_document_key(doc, ['id', 'dt']), V.get_document_key(doc, [], 't t')],\n"
        "}))\n"
    ).strip().splitlines()[-1])
    c = ValkeyConnector(label='w16s', host='h', port=6380, username='u', password='p@ss', db=2)
    d = ValkeyConnector(label='w16s2', host='h')
    e = ValkeyConnector(label='w16s3', uri='valkey://x:y@h2:1/3')
    doc = {'dt': 1, 'id': 'a:b', 'val': 2.5}
    got = {
        'uri': [c.URI, d.URI, e.URI],
        'quote': [ValkeyConnector.quote_table(t) for t in ('a', 'a b', "it's", '', 'x/y:z')],
        'counter': ValkeyConnector.get_counter_key('a b'),
        'dt_key': c.get_datetime_column_key('a b'),
        'entity': ValkeyConnector.get_entity_key('a', 'b', 'c'),
        'user': [
            ValkeyConnector.get_user_key('abc', 'attributes'),
            ValkeyConnector.get_user_key('u', 'user_id', True),
        ],
        'plugin': ValkeyConnector.get_plugin_key('p', 'version'),
        'docs': [
            ValkeyConnector.get_document_key(doc, ['id', 'dt']),
            ValkeyConnector.get_document_key(doc, [], 't t'),
        ],
    }
    assert got == ref
    with pytest.raises(Exception):
        ValkeyConnector.get_entity_key('a:b', 'c')


def test_the_connector_does_not_connect_until_it_is_used():
    conn = _valkey('w16s_down', host='127.0.0.1', port=1)
    assert conn.URI.startswith('valkey://')
    assert conn.quote_table('a b') == "'a b'"
    with pytest.raises(ConnectionError):
        conn.get('k')
    with pytest.raises(ConnectionError):
        conn.test_connection()


def test_client_is_the_built_in_one_without_the_valkey_package(monkeypatch):
    real = mrsm.attempt_import

    def attempt_import(name, *args, **kwargs):
        return None if name == 'valkey' else real(name, *args, **kwargs)

    monkeypatch.setattr(mrsm, 'attempt_import', attempt_import)
    conn = _valkey('w16s_client', host='127.0.0.1', port=1)
    from meerschaum.connectors.valkey._client import ValkeyClient
    assert isinstance(conn.client, ValkeyClient) and conn.client is conn.client
    with pytest.raises(NotImplementedError, match='valkey'):
        conn.client.pubsub
    with pytest.raises(ConnectionError):
        conn.client.get('k')


def test_the_connector_can_be_copied_and_pickled():
    import copy
    import pickle
    conn = _valkey('w16s_copy', host='127.0.0.1', port=1)
    conn._rust_client()
    assert copy.deepcopy(conn).URI == conn.URI
    assert pickle.loads(pickle.dumps(conn)).URI == conn.URI


# ---------------------------------------------------------------------------------------------
# The container


def _resp(*args) -> list:
    """A command over a raw RESP socket (no `valkey` package)."""
    payload = f'*{len(args)}\r\n'.encode()
    for arg in args:
        data = str(arg).encode()
        payload += b'$' + str(len(data)).encode() + b'\r\n' + data + b'\r\n'
    with socket.create_connection(('localhost', PORT), timeout=10) as sock:
        sock.sendall(payload)
        sock.settimeout(2)
        buf = b''
        try:
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                buf += chunk
                if buf.endswith(b'\r\n') and (buf[:1] != b'*' or buf.count(b'\r\n') > 1):
                    break
        except socket.timeout:
            pass
    return buf.decode(errors='replace').split('\r\n')


def _keys(pattern: str) -> list:
    reply = _resp('KEYS', pattern)
    return [line for line in reply[1:] if line and not line.startswith('$')]


def _delete(*patterns: str) -> None:
    for pattern in patterns:
        for key in _keys(pattern):
            _resp('DEL', key)


@pytest.fixture
def conn():
    if not FLAVOR_SELECTED:
        pytest.skip("set MRSM_TEST_FLAVORS to include valkey")
    connection = _valkey('w16s_valkey', host='localhost', port=PORT)
    try:
        assert connection.test_connection() is True
    except ConnectionError:
        pytest.skip("the Valkey container on 6399 is not running")
    yield connection
    _delete('w16s*', 'mrsm_pipe:w16s:*', 'mrsm_user:*w16s*', 'mrsm_plugin:w16s*')


def _pipe(conn, name=None, **kw):
    kw.setdefault('columns', {'datetime': 'dt', 'id': 'id'})
    kw.setdefault('dtypes', {'dt': 'datetime', 'id': 'int', 'val': 'float', 'name': 'str'})
    return mrsm.Pipe('w16s', name or 'p' + uuid.uuid4().hex[:8], instance=conn, **kw)


def _docs(n=5, offset=0):
    return [
        {
            'dt': datetime(2024, 1, 1 + i + offset),
            'id': i + offset,
            'val': float(i + offset) / 2,
            'name': f'n{i + offset}',
        }
        for i in range(n)
    ]


def test_set_get_and_expiration(conn):
    key = 'w16s_' + uuid.uuid4().hex[:6]
    assert conn.get(key) is None
    assert conn.set(key, 'value') is True
    assert conn.get(key) == 'value'
    assert conn.get(key, decode=False) == b'value'
    assert conn.set(key, 5, ex=100) is True
    assert conn.get(key) == '5'
    assert int(_resp('TTL', key)[0][1:]) > 0
    # Other options go to the raw client.
    assert not conn.set(key, 'x', nx=True, px=1000)
    assert conn.get(key) == '5'


def test_push_and_read_documents(conn):
    table = 'w16s_t' + uuid.uuid4().hex[:6]
    docs = [{'dt': 10 * i, 'a': i, 'b': f'x{i}'} for i in range(5)]
    assert conn.push_docs(docs, table, datetime_column='dt') == 5
    assert conn.push_docs(docs, table) == 0
    assert conn.get(conn.get_datetime_column_key(table)) == 'dt'
    assert len(conn.read_docs(table)) == 5
    # `end` is inclusive (`ZRANGEBYSCORE`), `begin` too.
    assert [d['a'] for d in conn.read_docs(table, begin=10, end=30)] == [1, 2, 3]
    # ... but `read()` filters with `query_df`, where `end` is exclusive (v4).
    df = conn.read(table, begin=10, end=30, params={'a': [2, 3]}, select_columns=['a', 'b'])
    assert list(df.columns) == ['a', 'b'] and list(df['a']) == [2]
    omitted = conn.read(table, omit_columns=['b'])
    assert 'b' not in omitted.columns
    conn.drop_table(table)
    assert conn.read_docs(table) == []
    assert conn.get(conn.get_datetime_column_key(table)) is None


def test_push_df(conn):
    pd = pytest.importorskip('pandas')
    table = 'w16s_df' + uuid.uuid4().hex[:6]
    df = pd.DataFrame(_docs(3))
    assert conn.push_df(df, table, datetime_column='dt') == 3
    assert len(conn.read(table)) == 3
    conn.drop_table(table)


def test_pipe_lifecycle(conn):
    pipe = _pipe(conn)
    assert conn.pipe_exists(pipe) is False
    assert conn.get_pipe_id(pipe) is None
    success, msg = conn.register_pipe(pipe)
    assert success, msg
    pipe_id = conn.get_pipe_id(pipe)
    assert isinstance(pipe_id, int)
    attributes = conn.get_pipe_attributes(pipe)
    assert attributes['parameters']['columns'] == {'datetime': 'dt', 'id': 'id'}
    assert conn.register_pipe(pipe)[0] is False

    success, msg = conn.sync_pipe(pipe, _docs(5))
    assert success, msg
    assert conn.pipe_exists(pipe) is True
    assert conn.get_pipe_rowcount(pipe) == 5
    df = conn.get_pipe_data(pipe)
    assert sorted(df['id']) == [0, 1, 2, 3, 4]
    assert set(conn.get_pipe_columns_types(pipe)) == {'dt', 'id', 'val', 'name'}
    utc = timezone.utc
    assert conn.get_sync_time(pipe) == datetime(2024, 1, 5, tzinfo=utc)
    assert conn.get_sync_time(pipe, newest=False) == datetime(2024, 1, 1, tzinfo=utc)
    # `params` is ignored by `get_sync_time` (v4).
    assert conn.get_sync_time(pipe, params={'id': 0}) == datetime(2024, 1, 5, tzinfo=utc)

    # The same rows again add nothing; a DataFrame syncs too.
    assert conn.sync_pipe(pipe, _docs(5))[0]
    assert conn.get_pipe_rowcount(pipe) == 5
    pd = pytest.importorskip('pandas')
    assert conn.sync_pipe(pipe, pd.DataFrame(_docs(2, offset=5)))[0]
    assert conn.get_pipe_rowcount(pipe) == 7
    success, msg = conn.sync_pipe(pipe, None)
    assert not success and 'None' in msg

    selected = conn.get_pipe_data(
        pipe, select_columns=['id'], begin=datetime(2024, 1, 2), end=datetime(2024, 1, 4),
    )
    assert list(selected.columns) == ['id']
    assert sorted(selected['id']) == [1, 2]

    success, msg = conn.clear_pipe(pipe, begin=datetime(2024, 1, 6))
    assert success, msg
    assert len(conn.get_pipe_data(pipe)) == 5
    # v4 quirk: the index entries of cleared rows stay, and the unfiltered count includes them.
    assert conn.get_pipe_rowcount(pipe) == 7
    success, msg = conn.drop_pipe(pipe)
    assert success, msg
    assert conn.pipe_exists(pipe) is False
    assert conn.get_pipe_id(pipe) == pipe_id
    success, msg = conn.delete_pipe(pipe)
    assert success, msg
    assert conn.get_pipe_id(pipe) is None


def test_edit_pipe_and_fetch_pipes_keys(conn):
    pipes = [_pipe(conn, tags=['w16s-tag']) for _ in range(2)]
    for pipe in pipes:
        assert conn.register_pipe(pipe)[0]
    pipes[0].parameters = {**pipes[0].parameters, 'tags': ['w16s-tag', 'edited']}
    success, msg = conn.edit_pipe(pipes[0])
    assert success, msg
    assert 'edited' in conn.get_pipe_attributes(pipes[0])['parameters']['tags']
    keys = conn.fetch_pipes_keys(connector_keys=['w16s'])
    assert isinstance(keys, list)
    ours = {p.metric_key for p in pipes}
    assert {k[1] for k in keys} >= ours
    assert all(len(k) == 4 and isinstance(k[3], dict) for k in keys)
    assert [k[2] for k in keys if k[1] in ours] == [None, None] or all(
        k[2] in (None, 'None') for k in keys if k[1] in ours
    )
    # v4 quirk: the pipes table holds no parameters, so a tag filter never matches.
    assert conn.fetch_pipes_keys(connector_keys=['w16s'], tags=['w16s-tag']) == []
    assert conn.fetch_pipes_keys(metric_keys=['no_such_metric_w16s']) == []
    for pipe in pipes:
        conn.delete_pipe(pipe)


def test_users(conn):
    from meerschaum.core import User
    name = 'w16s_u' + uuid.uuid4().hex[:6]
    user = User(
        name, 'secret-' + uuid.uuid4().hex[:6], instance=conn, email=f'{name}@example.com',
        type='user',
    )
    assert conn.get_user_id(user) is None
    success, msg = conn.register_user(user)
    assert success, msg
    user_id = conn.get_user_id(user)
    assert isinstance(user_id, str) and len(user_id) == 12
    assert name in conn.get_users()
    assert conn.get_user_password_hash(user)
    assert conn.get_user_attributes(user) == {}
    success, msg = conn.edit_user(user)
    assert success, msg
    success, msg = conn.delete_user(user)
    assert success, msg
    assert conn.get_user_id(user) is None
    assert name not in conn.get_users()
    keys = ValkeyConnector.get_user_keys_vals(user, mutable_only=True)
    assert set(keys) == {
        ValkeyConnector.get_user_key(user.user_id, sub)
        for sub in ('attributes', 'email', 'type', 'password_hash')
    }


def test_plugins(conn):
    from types import SimpleNamespace
    name = 'w16s' + uuid.uuid4().hex[:6]
    class Plugin:
        version = '1.2.3'
        attributes = {'description': 'x'}
        user_id = None

        def __init__(self):
            self.name = name

        def __str__(self):
            return name

    plugin = Plugin()
    assert conn.get_plugin_id(plugin) is None
    success, msg = conn.register_plugin(plugin)
    assert success, msg
    assert conn.get_plugin_version(plugin) == '1.2.3'
    assert conn.get_plugin_attributes(plugin) == {'description': 'x'}
    assert conn.get_plugin_user_id(plugin) is None or isinstance(conn.get_plugin_user_id(plugin), str)
    keys = ValkeyConnector.get_plugin_keys_vals(plugin)
    assert ValkeyConnector.get_plugin_key(name, 'version') in keys
    success, msg = conn.delete_plugin(plugin)
    assert success, msg
    assert conn.get_plugin_version(plugin) is None


def test_fetch_reads_the_source_key(conn):
    key = 'w16s_src' + uuid.uuid4().hex[:6]
    conn.push_docs([{'dt': 1, 'a': 1}, {'dt': 2, 'a': 2}], key, datetime_column='dt')
    pipe = _pipe(conn, parameters={'valkey': {'key': key}})
    docs = conn.fetch(pipe, begin=2)
    assert [d['a'] for d in docs] == [2]
    assert len(conn.fetch(pipe)) == 2
    plain = 'w16s_plain' + uuid.uuid4().hex[:6]
    conn.set(plain, 'v')
    assert conn.fetch(_pipe(conn, parameters={'valkey': {'key': plain}})) == [{plain: 'v'}]
    assert conn.fetch(_pipe(conn)) == []


def test_a_pipe_whose_instance_is_the_connector(conn):
    pipe = _pipe(conn)
    success, msg = pipe.sync(_docs(4))
    assert success, msg
    assert pipe.get_rowcount() == 4
    assert sorted(pipe.get_data()['id']) == [0, 1, 2, 3]
    pipe.delete()
    assert not pipe.exists()


def test_the_built_in_client_runs_the_raw_commands_plugins_use(conn):
    from meerschaum.connectors.valkey._client import ValkeyClient
    client = ValkeyClient(conn)
    prefix = 'w16s_c' + uuid.uuid4().hex[:6]
    assert client.ping() is True
    assert client.get(prefix + 'a') is None
    assert client.set(prefix + 'a', 'x') is True
    assert client.get(prefix + 'a') == b'x'
    assert client.set(prefix + 'a', 'y', nx=True, px=1000) is None
    with pytest.raises(ValueError, match='NX needs'):
        client.set(prefix + 'a', 'y', nx=True)
    assert client.exists(prefix + 'a', prefix + 'b') == 1
    assert client.incr(prefix + 'n') == 1 and client.incr(prefix + 'n') == 2
    assert client.type(prefix + 'a') == b'string'
    assert sorted(client.keys(prefix + '*')) == sorted([prefix.encode() + b'a', prefix.encode() + b'n'])
    assert client.zadd(prefix + 'z', {'one': 1, 'two': 2}) == 2
    assert client.zcard(prefix + 'z') == 2
    assert client.zrange(prefix + 'z', 0, -1) == [b'one', b'two']
    assert client.zrange(prefix + 'z', 0, -1, withscores=True) == [(b'one', 1.0), (b'two', 2.0)]
    assert client.zrangebyscore(prefix + 'z', '-inf', 1) == [b'one']
    assert client.zrem(prefix + 'z', 'one') == 1
    assert client.sadd(prefix + 's', 'a', 'b') == 2
    assert client.smembers(prefix + 's') == {b'a', b'b'} and client.scard(prefix + 's') == 2
    assert client.srem(prefix + 's', 'a') == 1
    assert client.rpush(prefix + 'l', 'p', 'q') == 2 and client.llen(prefix + 'l') == 2
    assert client.lrange(prefix + 'l', 0, -1) == [b'p', b'q']
    assert client.hset(prefix + 'h', mapping={'f': 'v', 'g': 'w'}) == 2
    assert client.hgetall(prefix + 'h') == {b'f': b'v', b'g': b'w'}
    assert client.unlink(prefix + 'a') == 1 and client.delete(prefix + 'n', prefix + 'z') == 2
    for suffix in ('s', 'l', 'h'):
        client.delete(prefix + suffix)
    assert client.keys(prefix + '*') == []


# ---------------------------------------------------------------------------------------------
# The released 4.1.4 connector (with the `valkey` package of the shared test venv) is the oracle

BATTERY = r'''
import json, re, uuid, sys
from datetime import datetime
import meerschaum as mrsm
from meerschaum.connectors.valkey import ValkeyConnector
from meerschaum.core import User


def family(dtype):
    text = str(dtype).lower()
    ### A Valkey frame has `object` columns in v4 (the documents' values) where the Rust
    ### connector builds typed ones (`Pipe.get_data()` enforces the dtypes in both).
    for key in ('int', 'bool'):
        if key in text:
            return key
    return 'other'


def norm(x):
    if isinstance(x, datetime):
        return 'dt:' + x.isoformat()
    if isinstance(x, bytes):
        return x.decode()
    if isinstance(x, (list, tuple)):
        return [norm(i) for i in x]
    if isinstance(x, dict):
        return {str(k): norm(v) for k, v in x.items()}
    if hasattr(x, 'columns') and hasattr(x, 'dtypes'):
        return {
            'df': {c: family(x[c].dtype) for c in x.columns},
            'rows': sorted(
                [
                    [norm(None if v != v else v.isoformat() if hasattr(v, 'isoformat') else v)
                     for v in row]
                    for row in x.astype(object).values.tolist()
                ],
                key=repr,
            ),
        }
    if x is None or isinstance(x, (bool, int, float, str)):
        return x
    return repr(x)


def run(port, tag):
    out = {}
    conn = mrsm.get_connector('valkey', 'w16sor' + tag, host='localhost', port=port)

    def rec(name, fn):
        try:
            out[name] = norm(fn())
        except Exception as e:
            kind = type(e).__name__
            out[name] = {'raised': 'ConnectionError' if 'Connection' in kind else kind}

    key = 'w16s_k' + tag
    rec('get_0', lambda: conn.get(key))
    rec('set', lambda: conn.set(key, 'v'))
    rec('get', lambda: conn.get(key))
    rec('get_raw', lambda: conn.get(key, decode=False))
    rec('set_ex', lambda: conn.set(key, 5, ex=100))
    rec('get_ex', lambda: conn.get(key))
    rec('test_connection', lambda: conn.test_connection())

    table = 'w16s_t' + tag
    docs = [{'dt': 10 * i, 'a': i, 'b': f'x{i}'} for i in range(5)]
    rec('push_docs', lambda: conn.push_docs(docs, table, datetime_column='dt'))
    rec('push_docs_again', lambda: conn.push_docs(docs, table))
    rec('dt_key', lambda: conn.get(conn.get_datetime_column_key(table)))
    rec('read_docs', lambda: sorted(conn.read_docs(table), key=lambda d: d['a']))
    rec('read_docs_range', lambda: [d['a'] for d in conn.read_docs(table, begin=10, end=30)])
    rec('read', lambda: conn.read(table))
    rec('read_params', lambda: conn.read(table, begin=10, end=30, params={'a': [2, 3]}, select_columns=['a', 'b']))
    rec('read_omit', lambda: conn.read(table, omit_columns=['b']))
    rec('drop_table', lambda: conn.drop_table(table))
    rec('read_docs_dropped', lambda: conn.read_docs(table))

    import pandas as pd
    pipe = mrsm.Pipe(
        'w16s', 'or' + tag, instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
        dtypes={'dt': 'datetime', 'id': 'int', 'val': 'float', 'name': 'str'},
    )
    pdocs = [
        {'dt': datetime(2024, 1, 1 + i), 'id': i, 'val': i / 2, 'name': f'n{i}'}
        for i in range(5)
    ]
    df = pd.DataFrame(pdocs)
    rec('exists_0', lambda: conn.pipe_exists(pipe))
    rec('id_0', lambda: conn.get_pipe_id(pipe))
    rec('attributes_0', lambda: conn.get_pipe_attributes(pipe))
    rec('data_0', lambda: conn.get_pipe_data(pipe))
    rec('rowcount_0', lambda: conn.get_pipe_rowcount(pipe))
    rec('sync_time_0', lambda: conn.get_sync_time(pipe))
    rec('types_0', lambda: conn.get_pipe_columns_types(pipe))
    rec('register', lambda: conn.register_pipe(pipe))
    rec('register_again', lambda: conn.register_pipe(pipe))
    rec('id_type', lambda: type(conn.get_pipe_id(pipe)).__name__)
    rec('attributes', lambda: conn.get_pipe_attributes(pipe)['parameters'])
    rec('sync', lambda: conn.sync_pipe(pipe, df))
    rec('sync_again', lambda: conn.sync_pipe(pipe, df))
    rec('exists', lambda: conn.pipe_exists(pipe))
    rec('rowcount', lambda: conn.get_pipe_rowcount(pipe))
    rec('rowcount_begin', lambda: conn.get_pipe_rowcount(pipe, begin=datetime(2024, 1, 3)))
    rec('data', lambda: conn.get_pipe_data(pipe))
    rec('data_select', lambda: conn.get_pipe_data(
        pipe, select_columns=['id', 'val'], begin=datetime(2024, 1, 2), end=datetime(2024, 1, 4)))
    rec('data_omit', lambda: conn.get_pipe_data(pipe, omit_columns=['name']))
    rec('data_params', lambda: conn.get_pipe_data(pipe, params={'id': 3}))
    rec('sync_time', lambda: conn.get_sync_time(pipe))
    rec('sync_time_oldest', lambda: conn.get_sync_time(pipe, newest=False))
    rec('types', lambda: conn.get_pipe_columns_types(pipe))
    rec('keys', lambda: sorted(
        (v[0], v[1], v[2]) for v in conn.fetch_pipes_keys(connector_keys=['w16s'], metric_keys=['or' + tag])
    ))
    rec('clear', lambda: conn.clear_pipe(pipe, begin=datetime(2024, 1, 4)))
    rec('rowcount_after_clear', lambda: conn.get_pipe_rowcount(pipe))
    rec('drop', lambda: conn.drop_pipe(pipe))
    rec('exists_after_drop', lambda: conn.pipe_exists(pipe))
    rec('edit', lambda: conn.edit_pipe(pipe))
    rec('delete', lambda: conn.delete_pipe(pipe))
    rec('id_after_delete', lambda: conn.get_pipe_id(pipe))

    name = 'w16sor' + tag
    user = User(name, 'pw-' + tag, instance=conn, email=name + '@example.com', type='user')
    rec('user_id_0', lambda: conn.get_user_id(user))
    rec('register_user', lambda: conn.register_user(user))
    rec('user_id_len', lambda: len(conn.get_user_id(user)))
    rec('users', lambda: name in conn.get_users())
    rec('user_attributes', lambda: conn.get_user_attributes(user))
    rec('user_type', lambda: conn.get_user_type(user))
    rec('user_hash_type', lambda: type(conn.get_user_password_hash(user)).__name__)
    rec('edit_user', lambda: conn.edit_user(user))
    rec('delete_user', lambda: conn.delete_user(user))
    rec('users_after', lambda: name in conn.get_users())

    class P:
        def __init__(self):
            self.name = 'w16sp' + tag
            self.version = '1.2.3'
            self.attributes = {'description': 'x'}
            self.user_id = None
        def __str__(self):
            return self.name
    plugin = P()
    rec('plugin_id_0', lambda: conn.get_plugin_id(plugin))
    rec('register_plugin', lambda: conn.register_plugin(plugin))
    rec('plugin_version', lambda: conn.get_plugin_version(plugin))
    rec('plugin_attributes', lambda: conn.get_plugin_attributes(plugin))
    rec('plugin_user_id', lambda: conn.get_plugin_user_id(plugin))
    rec('plugin_username', lambda: conn.get_plugin_username(plugin))
    rec('delete_plugin', lambda: conn.delete_plugin(plugin))
    rec('plugin_version_after', lambda: conn.get_plugin_version(plugin))

    source = 'w16s_src' + tag
    conn.push_docs([{'dt': 1, 'a': 1}, {'dt': 2, 'a': 2}], source, datetime_column='dt')
    spipe = mrsm.Pipe('w16s', 'src' + tag, instance=conn, parameters={'valkey': {'key': source}})
    rec('fetch', lambda: sorted(conn.fetch(spipe), key=lambda d: d['a']))
    rec('fetch_begin', lambda: [d['a'] for d in conn.fetch(spipe, begin=2)])
    rec('fetch_nokey', lambda: conn.fetch(pipe))
    return out
'''


def test_every_call_matches_the_v4_connector(conn):
    tag = uuid.uuid4().hex[:6]

    def clean():
        _delete('w16s*', 'mrsm_pipe:w16s:*', 'mrsm_user:*w16s*', 'mrsm_plugin:w16s*')

    clean()
    ref = json.loads(_v4(
        BATTERY + f"\nprint('RESULT' + json.dumps(run({PORT}, {tag!r}), default=str))\n",
        venvs=True,
    ).split('RESULT')[-1])
    clean()
    namespace = {}
    exec(BATTERY, namespace)
    ours = json.loads(json.dumps(namespace['run'](PORT, tag), default=str))
    differences = {
        key: (ref.get(key), ours.get(key))
        for key in sorted(set(ref) | set(ours))
        if ref.get(key) != ours.get(key)
    }
    assert not differences, json.dumps(differences, indent=1, default=str)


def test_native_calls_reuse_the_connectors_connection(conn):
    """A pipe read through the native flow does not open a connection per call."""
    pipe = _pipe(conn)
    try:
        assert conn.register_pipe(pipe)[0]
        assert conn.sync_pipe(pipe, _docs(3))[0]
        pipe.get_rowcount()

        def connections() -> int:
            for line in _resp('INFO', 'stats'):
                if line.startswith('total_connections_received:'):
                    return int(line.split(':')[1])
            raise AssertionError("INFO stats has no connection counter")

        before = connections()
        for _ in range(5):
            assert pipe.get_rowcount() == 3
            pipe.get_sync_time()
            pipe.exists()
        after = connections()
        # `INFO` opens one connection itself (the first `connections()` call is counted by
        # the second).
        assert after - before == 1, (before, after)
    finally:
        conn.delete_pipe(pipe)


PIPE_BATTERY = r"""
import json
from datetime import datetime
import meerschaum as mrsm


def run(port, tag):
    conn = mrsm.get_connector('valkey', 'w16sdt' + tag, host='localhost', port=port)
    docs = [
        {
            'dt': datetime(2024, 1, 1 + i), 'id': i, 'val': i / 2, 'n': None if i == 2 else i,
            'name': None if i == 1 else 'x', 'b': i % 2 == 0,
        }
        for i in range(4)
    ]
    declared = {
        'dt': 'datetime', 'id': 'int', 'val': 'float', 'n': 'int', 'name': 'str', 'b': 'bool',
    }
    out = {}
    for label, dtypes in (('declared', declared), ('inferred', None)):
        keys = dict(columns={'datetime': 'dt', 'id': 'id'})
        writer = mrsm.Pipe('w16s', 'dt' + label + tag, instance=conn, **keys, **(
            {'dtypes': dtypes} if dtypes else {}
        ))
        writer.sync(docs)
        pipe = mrsm.Pipe('w16s', 'dt' + label + tag, instance=conn, **keys)
        out[label + '_pipe_dtypes'] = {c: str(t) for c, t in pipe.dtypes.items()}
        for name, kwargs in (
            ('df', {}), ('select', {'select_columns': ['id', 'val']}),
            ('chunks', {'as_iterator': True}),
        ):
            result = pipe.get_data(**kwargs)
            if kwargs.get('as_iterator'):
                result = next(iter(result))
            ### (W16S-021: v4 casts an inferred datetime column to the stored Arrow timestamp type.)
            out[label + '_' + name] = {
                c: ('datetime' if str(t).startswith(('timestamp[', 'datetime64[')) else str(t))
                for c, t in sorted(result.dtypes.items())
            }
        writer.delete()
    return out
"""


def test_pipe_get_data_dtypes_match_the_v4_connector(conn):
    shared = REPO / 'test_root' / 'venvs'
    if not (shared / 'mrsm').is_dir():
        pytest.skip("the shared test venv (test_root/venvs/mrsm) is not built")
    tag = uuid.uuid4().hex[:6]
    _delete('w16s*', 'mrsm_pipe:w16s:*')
    code = lambda t: (  # noqa: E731
        PIPE_BATTERY + f"\nprint('RESULT' + json.dumps(run({PORT}, {t!r})))\n"
    )
    ref = json.loads(_v4(code(tag), venvs=True).split('RESULT')[-1])
    _delete('w16s*', 'mrsm_pipe:w16s:*')
    root = tempfile.mkdtemp(prefix='mrsm-w16s-child-', dir=_scratch())
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO), 'MRSM_ROOT_DIR': root,
        'MRSM_PLUGINS_DIR': str(pathlib.Path(root) / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1', 'MRSM_VENVS_DIR': str(shared),
    })
    result = subprocess.run(
        [sys.executable, '-c', code(tag + 'x')], env=env, cwd=root,
        capture_output=True, text=True, timeout=180,
    )
    assert 'RESULT' in result.stdout, result.stderr[-3000:]
    ours = json.loads(result.stdout.split('RESULT')[-1])
    differences = {
        key: (ref.get(key), ours.get(key))
        for key in sorted(set(ref) | set(ours))
        if ref.get(key) != ours.get(key)
    }
    assert not differences, json.dumps(differences, indent=1)
