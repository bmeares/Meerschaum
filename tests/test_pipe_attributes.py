"""Metadata first reads must not poison a shared Pipe's target cache."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Lock
import time
import uuid
import json

import pytest
import meerschaum as mrsm


def test_import_uses_working_tree():
    assert Path(mrsm.__file__).resolve().parent == Path(__file__).resolve().parents[1] / 'meerschaum'


@pytest.mark.parametrize('target', [None, 'explicit_self_table'])
def test_self_table_templates_do_not_reenter_parameter_resolution(tmp_path, target):
    conn = mrsm.get_connector('sql', 'self_' + uuid.uuid4().hex,
                              flavor='sqlite', database=str(tmp_path / 'self.db'))
    raw_sql = "SELECT MAX(id) FROM {{ Pipe('test', 'incremental') }}"
    parameters = {'sql': raw_sql, 'columns': {'id': 'id'}}
    if target:
        parameters['target'] = target
    pipe = mrsm.Pipe('test', 'incremental', instance=conn, parameters=parameters, cache=False)
    ok, message = pipe.register()
    assert ok, message
    for candidate in (pipe, mrsm.Pipe('test', 'incremental', instance=conn, cache=False)):
        resolved = candidate.get_parameters()
        assert '{{' not in resolved['sql']
        assert (target or 'test_incremental') in resolved['sql']
        assert candidate.target == (target or 'test_incremental')
        assert candidate.get_parameters(apply_symlinks=False)['sql'] == raw_sql
        assert candidate.get_parameters()['sql'] == resolved['sql']


@pytest.fixture
def registered_pipe(tmp_path):
    conn = mrsm.get_connector('sql', 'attrs_' + uuid.uuid4().hex,
                              flavor='sqlite', database=str(tmp_path / 'metadata.db'))
    original = mrsm.Pipe('test', 'attributes', instance=conn,
                         parameters={'target': 'registered_target'}, cache=False)
    ok, msg = original.register()
    assert ok, msg
    return mrsm.Pipe('test', 'attributes', instance=conn, cache=False)


def test_concurrent_first_reads_use_registered_target(registered_pipe, monkeypatch):
    pipe = registered_pipe
    read = pipe.instance_connector.get_pipe_attributes
    count_lock, start = Lock(), Barrier(8)
    calls = []
    def delayed(*args, **kwargs):
        with count_lock:
            calls.append(1)
        time.sleep(0.1)
        return read(*args, **kwargs)
    monkeypatch.setattr(pipe.instance_connector, 'get_pipe_attributes', delayed)
    def target(_):
        start.wait(timeout=5)
        return pipe.target
    with ThreadPoolExecutor(max_workers=8) as pool:
        targets = list(pool.map(target, range(8)))
    assert targets == ['registered_target'] * 8
    assert pipe.target == 'registered_target'
    assert len(calls) == 1
    assert pipe.__dict__.get('_attributes_sync_time') is not None


def test_failed_first_read_retries_without_stamping(registered_pipe, monkeypatch):
    pipe = registered_pipe
    read = pipe.instance_connector.get_pipe_attributes
    calls = []
    def transient(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError('metadata connection interrupted')
        return read(*args, **kwargs)
    monkeypatch.setattr(pipe.instance_connector, 'get_pipe_attributes', transient)
    with pytest.warns(UserWarning, match='Failed to load attributes') as captured:
        assert pipe.attributes.get('parameters', {}) == {}
    assert 'interrupted' in str(captured[0].message)
    assert pipe.__dict__.get('_attributes_sync_time') is None
    assert pipe.target == 'registered_target'
    assert len(calls) == 2


def test_registered_empty_parameters_do_not_cache_default(registered_pipe, monkeypatch):
    pipe = registered_pipe
    read = pipe.instance_connector.get_pipe_attributes
    assert pipe.id is not None
    monkeypatch.setattr(pipe.instance_connector, 'get_pipe_attributes',
                        lambda *a, **k: {'pipe_id': pipe.id, 'parameters': {}})
    assert pipe.target == pipe._target_legacy()
    assert '_target' not in pipe.__dict__
    monkeypatch.setattr(pipe.instance_connector, 'get_pipe_attributes', read)
    pipe._clear_cache_key('_attributes_sync_time')
    assert pipe.target == 'registered_target'


def test_unregistered_and_temporary_defaults_are_unchanged(registered_pipe):
    for temporary in (False, True):
        pipe = mrsm.Pipe('test', 'not_registered', instance=registered_pipe.instance_connector,
                         temporary=temporary, cache=False)
        assert pipe.target == 'test_not_registered'
        assert pipe.__dict__['_target'] == 'test_not_registered'


def test_refresh_discards_target_and_freshness(registered_pipe, monkeypatch):
    pipe = registered_pipe
    assert pipe.target == 'registered_target'
    pipe.__dict__['_target'] = 'poisoned'
    read = pipe.instance_connector.get_pipe_attributes
    calls = []
    def refreshed(*args, **kwargs):
        calls.append(1)
        return read(*args, **kwargs)
    monkeypatch.setattr(pipe.instance_connector, 'get_pipe_attributes', refreshed)
    assert pipe.get_parameters(refresh=True)['target'] == 'registered_target'
    assert calls == [1]
    assert pipe.target == 'registered_target'


def test_connector_can_read_parameters_during_metadata_load(registered_pipe, monkeypatch):
    pipe = registered_pipe
    read = pipe.instance_connector.get_pipe_attributes
    calls = []
    def reentrant(requested, **kwargs):
        calls.append(requested.parameters)
        return read(requested, **kwargs)
    monkeypatch.setattr(pipe.instance_connector, 'get_pipe_attributes', reentrant)
    assert pipe.target == 'registered_target'
    assert calls == [{}]
    assert not pipe._attributes_loading
    assert pipe.__dict__.get('_attributes_sync_time') is not None


def test_failed_refresh_preserves_cached_attributes(registered_pipe, monkeypatch):
    pipe = registered_pipe
    previous = pipe.attributes
    pipe._clear_cache_key('_attributes_sync_time')
    def fail(*args, **kwargs):
        raise RuntimeError('metadata unavailable')
    monkeypatch.setattr(pipe.instance_connector, 'get_pipe_attributes', fail)
    with pytest.warns(UserWarning, match='Failed to load attributes'):
        assert pipe.attributes == previous
    assert pipe.__dict__.get('_attributes_sync_time') is None
    assert not pipe._attributes_loading


@pytest.fixture
def attributes_server():
    """
    A loopback HTTP server answering `GET /pipes/<keys>/attributes`: the first answer is the
    failure the test asks for, every later one is valid. Records each request's query.
    """
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from urllib.parse import urlparse, parse_qs

    state = {'failure': None, 'queries': []}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            parsed = urlparse(self.path)
            if not parsed.path.endswith('/attributes'):
                self.send_response(404)
                self.end_headers()
                return
            state['queries'].append({k: v[0] for k, v in parse_qs(parsed.query).items()})
            status, body = 200, json.dumps({'parameters': {'target': 'api_target'}})
            if len(state['queries']) == 1:
                if state['failure'] == 'http':
                    status, body = 503, 'unavailable'
                elif state['failure'] == 'invalid_json':
                    body = 'not json'
                elif state['failure'] == 'error_object':
                    body = json.dumps({'detail': 'unavailable'})
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(body.encode())

    server = HTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state['port'] = server.server_address[1]
    yield state
    server.shutdown()
    server.server_close()


@pytest.mark.parametrize('failure', ['http', 'invalid_json', 'error_object'])
def test_api_metadata_failure_retries(attributes_server, failure):
    attributes_server['failure'] = failure
    conn = mrsm.get_connector('api', 'attrs_' + uuid.uuid4().hex, host='127.0.0.1',
                              port=attributes_server['port'], protocol='http',
                              instance_keys='sql:server_default')
    pipe = mrsm.Pipe('test', 'api_attributes', instance=conn, cache=False,
                     parameters={'instance_keys': 'sql:explicit'})
    with pytest.warns(UserWarning, match='Failed to load attributes'):
        assert pipe.attributes['parameters'] == {'instance_keys': 'sql:explicit'}
    assert pipe.__dict__.get('_attributes_sync_time') is None
    assert pipe.target == 'api_target'
    assert attributes_server['queries'] == [{'instance': 'sql:explicit'}] * 2


@pytest.mark.parametrize('recover_during_resolution', [False, True])
def test_failed_load_does_not_cache_constructor_only_default(
        registered_pipe, monkeypatch, recover_during_resolution):
    pipe = registered_pipe
    pipe._attributes['parameters'] = {'columns': {'id': 'id'}}
    assert pipe.id is not None
    read = pipe.instance_connector.get_pipe_attributes
    calls = []
    def transient(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1 or not recover_during_resolution:
            raise RuntimeError('metadata unavailable')
        return read(*args, **kwargs)
    monkeypatch.setattr(pipe.instance_connector, 'get_pipe_attributes', transient)
    with pytest.warns(UserWarning):
        pipe.target
    assert pipe.__dict__.get('_target') != pipe._target_legacy()
    monkeypatch.setattr(pipe.instance_connector, 'get_pipe_attributes', read)
    assert pipe.target == 'registered_target'
