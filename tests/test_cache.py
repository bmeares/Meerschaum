#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Tests for Pipe._cache_value / _get_cached_value / _invalidate_cache.
Memory layer tested without DB; disk layer uses the SQLite test connector.
"""

import pytest

import meerschaum as mrsm
from tests.connectors import conns

_MEMORY_INSTANCE = 'sql:memory'
_SQLITE_CONN = conns['sqlite']


# ---------------------------------------------------------------------------
# Memory-layer tests (no DB required, always run)
# ---------------------------------------------------------------------------

def test_cache_value_stored_in_memory():
    pipe = mrsm.Pipe('test', 'cache', 'stored_in_memory', instance=_MEMORY_INSTANCE)
    pipe._cache_value('my_key', 42, memory_only=True)
    assert pipe.__dict__.get('_my_key') == 42


def test_cache_value_leading_underscore_stored_correctly():
    pipe = mrsm.Pipe('test', 'cache', 'leading_underscore', instance=_MEMORY_INSTANCE)
    # _get_in_memory_key leaves already-prefixed keys unchanged: '_internal' → '_internal'
    pipe._cache_value('_internal', 'hello', memory_only=True)
    assert pipe.__dict__.get('_internal') == 'hello'


def test_get_cached_value_memory_hit():
    pipe = mrsm.Pipe('test', 'cache', 'memory_hit', instance=_MEMORY_INSTANCE)
    pipe._cache_value('token', 'abc123', memory_only=True)
    assert pipe._get_cached_value('token') == 'abc123'


def test_get_cached_value_miss_returns_none():
    pipe = mrsm.Pipe('test', 'cache', 'miss_none', instance=_MEMORY_INSTANCE)
    assert pipe._get_cached_value('nonexistent_key') is None


def test_cache_value_none_is_skipped():
    pipe = mrsm.Pipe('test', 'cache', 'none_skipped', instance=_MEMORY_INSTANCE)
    pipe._cache_value('nothing', None, memory_only=True)
    assert '_nothing' not in pipe.__dict__
    assert pipe._get_cached_value('nothing') is None


def test_cache_value_overwrites_existing():
    pipe = mrsm.Pipe('test', 'cache', 'overwrites', instance=_MEMORY_INSTANCE)
    pipe._cache_value('count', 1, memory_only=True)
    pipe._cache_value('count', 2, memory_only=True)
    assert pipe._get_cached_value('count') == 2


def test_cache_value_complex_object():
    pipe = mrsm.Pipe('test', 'cache', 'complex_obj', instance=_MEMORY_INSTANCE)
    obj = {'nested': [1, 2, {'deep': True}]}
    pipe._cache_value('data', obj, memory_only=True)
    assert pipe._get_cached_value('data') == obj


def test_cache_value_separate_pipes_isolated():
    pipe_a = mrsm.Pipe('test', 'cache', 'isolated_a', instance=_MEMORY_INSTANCE)
    pipe_b = mrsm.Pipe('test', 'cache', 'isolated_b', instance=_MEMORY_INSTANCE)
    pipe_a._cache_value('x', 1, memory_only=True)
    pipe_b._cache_value('x', 2, memory_only=True)
    assert pipe_a._get_cached_value('x') == 1
    assert pipe_b._get_cached_value('x') == 2


def test_invalidate_cache_soft_clears_exists_and_sync_ts():
    pipe = mrsm.Pipe('test', 'cache', 'soft_invalidate', instance=_MEMORY_INSTANCE)
    pipe._cache_value('_exists', True, memory_only=True)
    pipe._cache_value('sync_ts', 'some_ts', memory_only=True)
    pipe._cache_value('attributes', {'connector_keys': 'test'}, memory_only=True)

    success, msg = pipe._invalidate_cache(hard=False)
    assert success, msg

    assert pipe._get_cached_value('_exists') is None
    assert pipe._get_cached_value('sync_ts') is None
    assert pipe._get_cached_value('attributes') is not None


def test_invalidate_cache_hard_clears_volatile_keys():
    pipe = mrsm.Pipe('test', 'cache', 'hard_invalidate', instance=_MEMORY_INSTANCE)
    pipe._cache_value('_exists', True, memory_only=True)
    pipe._cache_value('sync_ts', 'some_ts', memory_only=True)
    pipe._cache_value('attributes', {'connector_keys': 'test'}, memory_only=True)

    success, msg = pipe._invalidate_cache(hard=True)
    assert success, msg

    # _exists and sync_ts are always cleared (soft + hard).
    assert pipe._get_cached_value('_exists') is None
    assert pipe._get_cached_value('sync_ts') is None
    # 'attributes' is intentionally preserved across hard invalidation.
    assert pipe._get_cached_value('attributes') is not None


# ---------------------------------------------------------------------------
# Disk-layer tests (SQLite connector — no Docker, just a local file.
# sql:memory hard-disables cache in __init__ so we need a real connector.)
# ---------------------------------------------------------------------------

def test_disk_cache_write_and_read():
    pipe = mrsm.Pipe('test', 'diskcache', 'write_and_read', instance=_SQLITE_CONN, cache=True)

    pipe._cache_value('sentinel', {'answer': 42})

    fresh = mrsm.Pipe('test', 'diskcache', 'write_and_read', instance=_SQLITE_CONN, cache=True)
    assert fresh._get_cached_value('sentinel') == {'answer': 42}

    pipe._invalidate_cache(hard=True)


def test_disk_cache_memory_layer_prevents_redundant_reads():
    pipe = mrsm.Pipe('test', 'diskcache', 'memory_layer', instance=_SQLITE_CONN, cache=True)

    pipe._cache_value('flag', 'initial')
    assert pipe._get_cached_value('flag') == 'initial'

    pipe.__dict__['_flag'] = 'overridden_in_memory'
    assert pipe._get_cached_value('flag') == 'overridden_in_memory'

    pipe._invalidate_cache(hard=True)


def test_disk_cache_hard_invalidate_removes_files():
    pipe = mrsm.Pipe('test', 'diskcache', 'hard_invalidate', instance=_SQLITE_CONN, cache=True)

    pipe._cache_value('canary', 'tweet')
    cache_dir = pipe._get_cache_dir_path()
    assert (cache_dir / 'canary.pkl').exists()

    pipe._invalidate_cache(hard=True)
    assert not cache_dir.exists() or not (cache_dir / 'canary.pkl').exists()


def test_disk_cache_disabled_for_memory_instance():
    pipe = mrsm.Pipe('test', 'diskcache', 'disabled_memory', instance='sql:memory')
    assert pipe.cache is False

    pipe._cache_value('key', 'val')
    pkl_path = pipe._get_cache_dir_path() / 'key.pkl'
    assert not pkl_path.exists()
