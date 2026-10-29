#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`from meerschaum.utils.cache import cache` (rc15 Q33): the decorator, `get`/`set`/`delete`/`clear`,
safe serialization (JSON plus dates, Decimal, UUID, bytes and DataFrames; never pickle), TTLs, a
bounded memory store, invalidation by a listed pipe's recorded syncs, Valkey when the configured
connector answers (and a one-time warning in the API host when it does not).
"""

from tests._ports import service_port

import json
import time
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

import meerschaum as mrsm
import meerschaum.utils.cache as cache_module
from meerschaum.utils.cache import Cache, decode_value, encode_value


def _memory_cache(**kwargs) -> Cache:
    """A cache that never asks Valkey."""
    cache = Cache(**kwargs)
    cache._connector_keys = lambda: None
    return cache


class FakePipe:
    """A pipe whose recorded sync the test moves forward."""

    def __init__(self, name='fake'):
        self.connector_keys = 'test'
        self.metric_key = name
        self.location_key = None
        self.instance_keys = 'sql:nowhere'
        self.last_success = '2026-01-01T00:00:00+00:00'

    def get_sync_status(self):
        return {'last_success': self.last_success}

    def sync(self):
        self.last_success = datetime.now(timezone.utc).isoformat()


def test_values_round_trip_without_pickle():
    values = [
        {'a': 1, 'b': [1.5, None, True], 'c': 'text'},
        [datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc), date(2026, 1, 2)],
        {'amount': Decimal('12.30'), 'id': uuid.UUID(int=7)},
        b'\x00\xffbytes',
        'plain',
        None,
    ]
    for value in values:
        kind, payload = encode_value(value)
        assert isinstance(payload, str)
        assert decode_value(kind, payload) == value
    ### Unknown types are refused rather than pickled.
    with pytest.raises(TypeError):
        encode_value({'object': object()})


def test_dataframes_round_trip():
    pd = pytest.importorskip('pandas')
    df = pd.DataFrame({
        'ts': pd.to_datetime(['2026-01-01', '2026-01-02'], utc=True),
        'value': pd.array([1, None], dtype='Int64'),
        'name': ['a', 'b'],
    })
    kind, payload = encode_value(df)
    assert kind in ('pandas-arrow', 'pandas-json')
    back = decode_value(kind, payload)
    pd.testing.assert_frame_equal(back, df, check_dtype=(kind == 'pandas-arrow'))
    pl = pytest.importorskip('polars')
    frame = pl.DataFrame({'x': [1, 2], 'y': ['a', 'b']})
    kind, payload = encode_value(frame)
    assert kind == 'polars-arrow'
    assert decode_value(kind, payload).equals(frame)


def test_get_set_delete_clear_and_ttl():
    cache = _memory_cache()
    assert cache.backend == 'memory'
    assert cache.get('missing', 'default') == 'default'
    assert cache.set('greeting', {'text': 'hello'}, ttl=60)
    value = cache.get('greeting')
    assert value == {'text': 'hello'}
    ### A copy comes back: changing it does not change the cache.
    value['text'] = 'changed'
    assert cache.get('greeting') == {'text': 'hello'}
    assert cache.delete('greeting') is True
    assert cache.delete('greeting') is False
    assert cache.get('greeting') is None

    cache.set('short', 1, ttl=0.2)
    assert cache.get('short') == 1
    time.sleep(0.3)
    assert cache.get('short') is None
    assert cache.set('never', 1, ttl=0) is False

    cache.set('one', 1)
    cache.set('two', 2)
    assert cache.clear() == 2
    assert cache.get('one') is None
    with pytest.raises(TypeError):
        cache.set('bad', object())


def test_memory_is_bounded_least_recently_used_first():
    cache = _memory_cache(max_entries=3)
    for key in ('a', 'b', 'c'):
        cache.set(key, key)
    assert cache.get('a') == 'a'
    cache.set('d', 'd')
    assert cache.get('b') is None
    assert [cache.get(key) for key in ('a', 'c', 'd')] == ['a', 'c', 'd']
    small = _memory_cache(max_bytes=400)
    small.set('big', 'x' * 1000)
    assert small.get('big') is None


def test_decorator_caches_by_arguments():
    cache = _memory_cache()
    calls = []

    @cache(ttl=60)
    def square(x, scale=1):
        calls.append((x, scale))
        return {'value': x * x * scale}

    assert square(3) == {'value': 9}
    assert square(3) == {'value': 9}
    assert square(3, scale=2) == {'value': 18}
    assert calls == [(3, 1), (3, 2)]
    assert square.cache_key(3) != square.cache_key(4)
    assert square.cache_clear() == 2
    square(3)
    assert calls == [(3, 1), (3, 2), (3, 1)]
    assert square.uncached(2) == {'value': 4}

    @cache
    def bare():
        calls.append('bare')
        return 1

    assert bare() == bare() == 1
    assert calls.count('bare') == 1


def test_an_uncacheable_result_is_returned_and_warned_once(capsys):
    cache = _memory_cache()
    calls = []

    @cache(ttl=60)
    def thing():
        calls.append(1)
        return object()

    first, second = thing(), thing()
    assert first is not second and len(calls) == 2
    assert capsys.readouterr().err.count('Not caching the results of') == 1


def test_a_listed_pipe_sync_invalidates():
    cache = _memory_cache()
    cache.version_seconds = 0
    pipe = FakePipe()
    calls = []
    original_current = cache._current_versions

    def current_versions(refs, deps):
        ### The fake pipe is not on a real instance: answer for it directly.
        return {ref: cache._pipe_version(pipe) for ref in refs}

    cache._current_versions = current_versions

    @cache(ttl=60, pipes=[pipe])
    def report():
        calls.append(1)
        return len(calls)

    assert report() == 1
    assert report() == 1
    pipe.sync()
    assert report() == 2
    assert report() == 2

    ### `pipes` may be a function of the arguments.
    other = FakePipe('other')

    @cache(ttl=60, pipes=lambda p: [p])
    def per_pipe(p):
        calls.append(p.metric_key)
        return p.metric_key

    cache._current_versions = lambda refs, deps: {ref: cache._pipe_version(other) for ref in refs}
    assert per_pipe(other) == per_pipe(other) == 'other'
    other.sync()
    per_pipe(other)
    assert calls.count('other') == 2

    ### Manual entries take `pipes` too.
    cache.set('manual', 'v', pipes=[other])
    assert cache.get('manual') == 'v'
    other.sync()
    assert cache.get('manual') is None
    cache._current_versions = original_current


def test_a_real_pipe_sync_invalidates(tmp_path):
    conn = mrsm.get_connector(
        'sql', f'cache_test_{uuid.uuid4().hex[:8]}', flavor='sqlite', database=str(tmp_path / 'c.db'),
    )
    pipe = mrsm.Pipe('test', 'cache_rows', instance=conn, columns={'datetime': 'ts'})
    if not hasattr(pipe, 'get_sync_status'):
        pytest.skip("Per-pipe sync statuses are not available.")
    success, message = pipe.sync([{'ts': '2026-01-01', 'value': 1}])
    assert success, message
    cache = _memory_cache()
    cache.version_seconds = 0

    @cache(ttl=60, pipes=[pipe])
    def total():
        return len(pipe.get_data())

    assert total() == 1
    ### Recorded syncs of one pipe a few seconds apart can be merged into one record.
    time.sleep(6)
    success, message = pipe.sync([{'ts': '2026-01-02', 'value': 2}])
    assert success, message
    assert total() == 2


def test_unreachable_valkey_falls_back_to_memory_and_warns_in_the_host_once(monkeypatch, capsys):
    label = f'cache_nope_{uuid.uuid4().hex[:6]}'
    mrsm.get_connector('valkey', label, host='127.0.0.1', port=1)
    cache = Cache()
    cache._connector_keys = lambda: f'valkey:{label}'
    ### Outside the API host, falling back to memory is silent.
    monkeypatch.delenv('MRSM_API_HOST', raising=False)
    assert cache.backend == 'memory'
    assert 'is unreachable' not in capsys.readouterr().err

    cache = Cache()
    cache._connector_keys = lambda: f'valkey:{label}'
    monkeypatch.setenv('MRSM_API_HOST', '1')
    assert cache.backend == 'memory'
    cache._valkey_until = 0
    assert cache.backend == 'memory'
    cache.set('x', 1)
    assert cache.get('x') == 1
    err = capsys.readouterr().err
    assert err.count('is unreachable') == 1 and f"'valkey:{label}'" in err
    assert 'connection refused' in err


def test_valkey_backend_is_shared(monkeypatch):
    conn = mrsm.get_connector('valkey', 'test_valkey_cache', host='localhost', port=service_port('valkey'))
    try:
        reachable = conn.test_connection()
    except Exception:
        reachable = False
    if not reachable:
        pytest.skip("The test Valkey (localhost:6399) is not running.")
    namespace = f'mrsm:cache:rc15test-{uuid.uuid4().hex[:8]}:'
    monkeypatch.setattr(cache_module, 'NAMESPACE', namespace)
    first, second = Cache(), Cache()
    for cache in (first, second):
        cache._connector_keys = lambda: 'valkey:test_valkey_cache'
    try:
        assert first.backend == 'valkey'
        first.set('shared', {'when': datetime(2026, 1, 1, tzinfo=timezone.utc)}, ttl=30)
        ### Another process (another cache object) reads it from Valkey.
        assert second.get('shared') == {'when': datetime(2026, 1, 1, tzinfo=timezone.utc)}
        raw = conn.get(namespace + 'shared')
        header, _, payload = raw.partition('\n')
        assert json.loads(header)['kind'] == 'json' and 'datetime' in payload

        calls = []

        @first(ttl=30)
        def compute(x):
            calls.append(x)
            return x + 1

        @second(ttl=30)
        def compute_elsewhere(x):
            calls.append(('elsewhere', x))
            return x + 1

        compute_elsewhere.__wrapped__.__qualname__ = compute.__qualname__
        assert compute(1) == 2 and compute(1) == 2
        assert calls == [1]
        assert first.delete('shared') is True
        assert second.get('shared') is None
    finally:
        first.clear()
    assert conn.client.keys(namespace + '*') == []


def test_asking_valkey_again_never_blocks_a_lookup():
    cache = Cache()
    cache._connector_keys = lambda: None
    assert cache.backend == 'memory'
    calls = []

    def slow_probe():
        calls.append(time.monotonic())
        time.sleep(1.5)
        with cache._lock:
            cache._probing = False
            cache._valkey_until = time.monotonic() + 60
        return None

    cache._probe_valkey = slow_probe
    cache._valkey_until = 0
    started = time.monotonic()
    assert cache.backend == 'memory'
    assert cache.set('quick', 1) and cache.get('quick') == 1
    assert time.monotonic() - started < 1.0
    ### One probe in the background at a time.
    assert cache.backend == 'memory'
    time.sleep(0.1)
    assert len(calls) == 1
