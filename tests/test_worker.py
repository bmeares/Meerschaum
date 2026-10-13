#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Tests for the Python side of the worker protocol (`meerschaum.worker`): the MessagePack codec,
framing, fetch-result normalization, Arrow conversion, the callback path, the manifest builder,
and an end-to-end session against a real worker process (acting as the host).

Run with:
    PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \\
        python -m pytest tests/test_worker.py
"""

import io
import os
import socket
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

import meerschaum as mrsm
from meerschaum.worker import PROTOCOL_VERSION
from meerschaum.worker import _msgpack
from meerschaum.worker._frames import (
    KIND_ARROW, KIND_CONTROL, encode_message, read_frame, read_message,
)

pd = mrsm.attempt_import('pandas')
pa = mrsm.attempt_import('pyarrow')

RICH_VALUES = {
    'none': None,
    'bools': [True, False],
    'ints': [0, 1, 127, 128, 255, 256, 65535, 65536, 2**32, 2**63 - 1, -1, -32, -33, -129, -2**31 - 1],
    'float': 1.5,
    'str': 'héllo' * 10,
    'bytes': b'\x00\x01\x02',
    'nested': {'a': [1, {'b': None}], 'c': {}},
    'int_keys': {1: 'one', 2: 'two'},
    'dt_aware': datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=timezone.utc),
    'dt_naive': datetime(2024, 1, 2, 3, 4, 5),
    'date': date(2024, 1, 2),
    'decimal': Decimal('1.10'),
    'uuid': UUID('12345678-1234-5678-1234-567812345678'),
    'timedelta': timedelta(minutes=90),
    'tuple': (1, 2),
}


def _expected(value):
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, dict):
        return {k: _expected(v) for k, v in value.items()}
    return value


@pytest.mark.parametrize('default_name', ['to_ext', '_default'])
def test_fallback_codec_roundtrip(default_name):
    ### `_default` returns `msgpack.ExtType` when the `msgpack` package was importable at load
    ### time (e.g. after another test activated the `mrsm` venv); the fallback accepts both.
    default = getattr(_msgpack, default_name)
    data = _msgpack._fallback_packb(RICH_VALUES, default)
    decoded = _msgpack._fallback_unpackb(data, _msgpack.from_ext)
    assert decoded == _expected(RICH_VALUES)


def test_public_codec_roundtrip():
    assert _msgpack.unpackb(_msgpack.packb(RICH_VALUES)) == _expected(RICH_VALUES)


def test_fallback_codec_matches_msgpack():
    msgpack = mrsm.attempt_import('msgpack', install=False, lazy=False)
    if msgpack is None:
        pytest.skip("msgpack is not installed")

    def default(obj):
        converted = _msgpack.to_ext(obj)
        if isinstance(converted, _msgpack.ExtType):
            return msgpack.ExtType(converted.code, converted.data)
        return converted

    with_package = msgpack.packb(RICH_VALUES, default=default, use_bin_type=True, strict_types=False)
    assert with_package == _msgpack._fallback_packb(RICH_VALUES, _msgpack.to_ext)
    assert with_package == _msgpack._fallback_packb(RICH_VALUES, _msgpack._default)


def test_frames_roundtrip():
    message = {'t': 'call', 'id': 7, 'op': 'ping', 'args': {'echo': 'hi'}}
    data = encode_message(message, [b'abc', b''])
    stream = io.BytesIO(data)
    decoded, frames = read_message(stream)
    assert decoded['op'] == 'ping'
    assert decoded['frames'] == 2
    assert frames == [b'abc', b'']
    assert stream.read() == b''

    stream = io.BytesIO(data)
    kind, _ = read_frame(stream)
    assert kind == KIND_CONTROL
    kind, payload = read_frame(stream)
    assert (kind, payload) == (KIND_ARROW, b'abc')


def test_to_payload_shapes():
    from meerschaum.worker._arrow import to_payload
    docs = [{'a': 1}, {'a': 2}]
    assert to_payload(None) == ('none', None, [])
    assert to_payload(docs) == ('docs', docs, [])
    assert to_payload({'a': [1, 2]}) == ('columns', {'a': [1, 2]}, [])
    assert to_payload('[{"a": 1}]') == ('text', '[{"a": 1}]', [])
    kind, value, frames = to_payload(pd.DataFrame(docs))
    assert kind == 'arrow' and value is None and len(frames) == 1
    with pytest.raises(TypeError):
        to_payload(object())


def test_arrow_special_columns_roundtrip():
    from meerschaum.worker._arrow import df_to_ipc, ipc_to_df, ipc_to_table
    df = pd.DataFrame([{
        'dt': datetime(2024, 1, 1, tzinfo=timezone.utc),
        'id': 1,
        'meta': {'a': [1, 2]},
        'amount': Decimal('1.10'),
        'uid': UUID('12345678-1234-5678-1234-567812345678'),
        'raw': b'\x00\x01',
    }])
    data = df_to_ipc(df)
    table = ipc_to_table(data)
    tags = {
        field.name: (field.metadata or {}).get(b'meerschaum:dtype', b'').decode()
        for field in table.schema
    }
    assert tags == {'dt': '', 'id': '', 'meta': 'json', 'amount': 'numeric', 'uid': 'uuid', 'raw': 'bytes'}
    ### Newer pyarrow versions read `arrow.json` as a canonical extension type.
    meta_field = table.schema.field('meta')
    assert (
        meta_field.metadata.get(b'ARROW:extension:name', None) == b'arrow.json'
        or getattr(meta_field.type, 'extension_name', None) == 'arrow.json'
    )

    out = ipc_to_df(data)
    assert out['meta'][0] == {'a': [1, 2]}
    assert out['amount'][0] == Decimal('1.10')
    assert out['uid'][0] == UUID('12345678-1234-5678-1234-567812345678')
    assert out['raw'][0] == b'\x00\x01'
    assert out['dt'][0] == pd.Timestamp('2024-01-01', tz='UTC')


class _FakeConnection:
    """Records the callbacks a worker would send to the host."""

    def __init__(self):
        self.calls = []

    def callback(self, op, args, frames=()):
        self.calls.append((op, args, list(frames)))
        return [True, f"synced {args['kind']}"], []


def test_pipe_sync_is_forwarded_to_the_host():
    from meerschaum.worker._ops import _patch_pipe_sync
    conn = _FakeConnection()
    pipe = mrsm.Pipe('plugin:stress', 'cb', instance='sql:main', parameters={'columns': {'id': 'id'}})
    _patch_pipe_sync(conn, pipe)

    assert pipe.sync([{'id': 1}], check_existing=False, callback=print) == (True, 'synced docs')
    op, args, frames = conn.calls[-1]
    assert op == 'sync'
    assert args['value'] == [{'id': 1}]
    assert args['pipe']['parameters'] == {'columns': {'id': 'id'}}
    ### Callables never cross the boundary.
    assert args['kwargs'] == {'check_existing': False}

    success, msg = pipe.sync(pd.DataFrame([{'id': 1}]))
    assert success and msg == 'synced arrow'
    assert len(conn.calls[-1][2]) == 1

    def gen():
        yield [{'id': 1}]
        yield {'id': [2]}
    success, msg = pipe.sync(gen())
    assert success
    assert [c[1]['kind'] for c in conn.calls[-2:]] == ['docs', 'columns']

    success, msg = pipe.sync(None)
    assert not success and 'instead of data' in msg


def test_build_manifest_includes_test_plugins():
    from meerschaum.worker._manifest import build_manifest
    manifest = build_manifest()
    assert manifest['schema_version'] == 1
    assert 'stress' in manifest['plugins']
    stress = manifest['plugins']['stress']
    assert stress['version'] == '0.4.0'
    assert stress['data_functions'] == ['fetch', 'register']
    assert stress['import_error'] is None


def _start_worker(tmpdir):
    path = os.path.join(tmpdir, 's.sock')
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(path)
    server.listen(1)
    server.settimeout(120)
    proc = subprocess.Popen(
        [sys.executable, '-m', 'meerschaum.worker', '--socket', path],
        env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'},
    )
    conn, _ = server.accept()
    server.close()
    return proc, conn.makefile('rwb')


@pytest.mark.skipif(not hasattr(socket, 'AF_UNIX'), reason="Unix sockets only")
def test_worker_session_end_to_end():
    with tempfile.TemporaryDirectory() as tmpdir:
        proc, stream = _start_worker(tmpdir)
        try:
            hello, _ = read_message(stream)
            assert hello['t'] == 'hello'
            assert hello['protocol'] == PROTOCOL_VERSION
            assert hello['pid'] == proc.pid

            def call(call_id, op, args):
                stream.write(encode_message({'t': 'call', 'id': call_id, 'op': op, 'args': args}))
                stream.flush()
                chunks = []
                while True:
                    msg, frames = read_message(stream)
                    assert msg['id'] == call_id
                    if msg['t'] == 'chunk':
                        chunks.append((msg, frames))
                        continue
                    return msg, chunks

            msg, _ = call(1, 'ping', {'echo': 'x'})
            assert msg['t'] == 'result' and msg['value']['echo'] == 'x'

            msg, _ = call(2, 'describe_connector', {'connector': 'plugin:stress'})
            assert msg['value']['has_fetch'] and msg['value']['has_register']
            assert not msg['value']['has_sync']

            begin = datetime(2024, 1, 1, tzinfo=timezone.utc)
            msg, chunks = call(3, 'fetch', {
                'connector': 'plugin:stress',
                'pipe': {
                    'connector_keys': 'plugin:stress',
                    'metric_key': 'e2e',
                    'location_key': None,
                    'instance_keys': 'sql:main',
                    'parameters': {'columns': {'datetime': 'datetime', 'id': 'id'}},
                },
                'begin': begin,
                'end': begin + timedelta(minutes=10),
                'kwargs': {'chunksize': 4},
            })
            assert msg['t'] == 'result', msg
            assert msg['value']['generator'] is True
            assert msg['value']['chunks'] == len(chunks) == 3
            rows = [doc for chunk, _ in chunks for doc in chunk['value']]
            assert len(rows) == 10
            assert rows[0]['datetime'] == begin
            assert all(chunk['kind'] == 'docs' for chunk, _ in chunks)

            msg, _ = call(4, 'fetch', {'connector': 'plugin:nonexistent_plugin', 'pipe': None})
            assert msg['t'] == 'error'

            msg, _ = call(5, 'shutdown', {})
            assert msg['t'] == 'result'
            assert proc.wait(timeout=30) == 0
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
