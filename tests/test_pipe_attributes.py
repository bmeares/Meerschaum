"""Metadata first reads must not poison a shared Pipe's target cache."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Lock
from types import SimpleNamespace
import time
import uuid
import json

import pytest
import meerschaum as mrsm


def test_import_uses_working_tree():
    assert Path(mrsm.__file__).resolve().parent == Path(__file__).resolve().parents[1] / 'meerschaum'


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


def test_sql_attribute_errors_are_not_unregistered(registered_pipe, monkeypatch):
    pipe = registered_pipe
    assert pipe.id is not None
    def fail(*args, **kwargs):
        raise RuntimeError('metadata read failed')
    monkeypatch.setattr(pipe.instance_connector, 'exec', fail)
    with pytest.raises(RuntimeError, match='metadata read failed'):
        pipe.instance_connector.get_pipe_attributes(pipe)


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


@pytest.mark.parametrize('failure', ['http', 'invalid_json', 'error_object'])
def test_api_metadata_failure_retries(monkeypatch, failure):
    conn = mrsm.get_connector('api', 'attrs_' + uuid.uuid4().hex, host='localhost',
                              port=8989, instance_keys='sql:server_default')
    pipe = mrsm.Pipe('test', 'api_attributes', instance=conn, cache=False,
                     parameters={'instance_keys': 'sql:explicit'})
    calls = []
    def get(*args, **kwargs):
        calls.append(kwargs['params'])
        def check_status():
            if len(calls) == 1 and failure == 'http':
                raise RuntimeError('HTTP 503')
        text = json.dumps({'parameters': {'target': 'api_target'}})
        if len(calls) == 1 and failure == 'invalid_json':
            text = 'not json'
        elif len(calls) == 1 and failure == 'error_object':
            text = json.dumps({'detail': 'unavailable'})
        return SimpleNamespace(text=text, raise_for_status=check_status)
    monkeypatch.setattr(conn, 'get', get)
    with pytest.warns(UserWarning, match='Failed to load attributes'):
        assert pipe.attributes['parameters'] == {'instance_keys': 'sql:explicit'}
    assert pipe.__dict__.get('_attributes_sync_time') is None
    assert pipe.target == 'api_target'
    assert calls == [{'instance': 'sql:explicit'}] * 2


def test_sql_no_row_is_still_empty(registered_pipe, monkeypatch):
    pipe = registered_pipe
    assert pipe.id is not None
    monkeypatch.setattr(pipe.instance_connector, 'exec', lambda *a, **k:
                        SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [])))
    assert pipe.instance_connector.get_pipe_attributes(pipe) == {}


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
