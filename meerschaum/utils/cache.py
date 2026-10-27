#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A small cache for plugins: remember what was slow to compute (a query, a chart, an API call)
for a while, and forget it when the pipes it came from sync.

```python
from meerschaum.utils.cache import cache

pipe = mrsm.Pipe('sql:main', 'weather', instance='sql:main')

@cache(ttl=300, pipes=[pipe])
def daily_means(station: str):
    return pipe.get_data(params={'station': station}).groupby('day').mean()

cache.set('greeting', {'text': 'hello'}, ttl=60)
cache.get('greeting')            # {'text': 'hello'}
cache.delete('greeting')
cache.clear()
```

**Where values live.** In Valkey when the connector `api:cache:connector` (default `valkey:main`)
answers, so every process of a deployment (the web console, its API routes, jobs) shares them;
otherwise in this process's memory (thread-safe, bounded by `cache.max_entries` and
`cache.max_bytes`, least recently used first out). Valkey is probed once and again a minute after it
was unreachable; the web console warns once at startup when the configured connector does not
answer.

**What can be cached.** JSON values (`dict`, `list`, `str`, numbers, `bool`, `None`), plus
`datetime`, `date`, `time`, `Decimal`, `UUID` and `bytes`, and pandas and Polars DataFrames (as
Arrow IPC when `pyarrow` is installed). Nothing is pickled. A value comes back as a copy, from
memory or Valkey alike. A function whose result cannot be stored still returns it, uncached.

**When values expire.** After `ttl` seconds (`None`: never), or when a listed pipe syncs: an entry
remembers the latest recorded sync of each of its `pipes` (`pipe.get_sync_status()`, written by
every sync path: jobs, `mrsm sync pipes`, Pipelines, the API and `pipe.sync()`), and a newer sync
makes it stale. Syncs of one pipe a few seconds apart are recorded as one, so a burst of syncs may
be noticed at the next recorded one.
"""

from __future__ import annotations

import base64
import functools
import hashlib
import io
import json
import os
import threading
import time
from collections import OrderedDict
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from uuid import UUID

from meerschaum.utils.typing import Any, Callable, Dict, List, Optional, Tuple, Union

__all__ = ('cache', 'Cache')

#: Every key in Valkey starts with this.
NAMESPACE: str = 'mrsm:cache:'
FORMAT_VERSION: int = 1
_TYPE_KEY: str = '__mrsm_cache_type__'
_MISSING = object()


class CacheValueError(TypeError):
    """A value the cache cannot store."""


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return {_TYPE_KEY: 'datetime', 'value': value.isoformat()}
    if isinstance(value, date):
        return {_TYPE_KEY: 'date', 'value': value.isoformat()}
    if isinstance(value, dt_time):
        return {_TYPE_KEY: 'time', 'value': value.isoformat()}
    if isinstance(value, Decimal):
        return {_TYPE_KEY: 'decimal', 'value': str(value)}
    if isinstance(value, UUID):
        return {_TYPE_KEY: 'uuid', 'value': str(value)}
    if isinstance(value, (bytes, bytearray)):
        return {_TYPE_KEY: 'bytes', 'value': base64.b64encode(bytes(value)).decode('ascii')}
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=repr)
    raise CacheValueError(
        f"The cache cannot store a {type(value).__name__}: use JSON values, dates, Decimal, UUID, "
        "bytes or a pandas or Polars DataFrame."
    )


def _json_object_hook(obj: Dict[str, Any]) -> Any:
    kind = obj.get(_TYPE_KEY, None)
    if kind is None or len(obj) != 2:
        return obj
    value = obj.get('value', None)
    if kind == 'datetime':
        return datetime.fromisoformat(value)
    if kind == 'date':
        return date.fromisoformat(value)
    if kind == 'time':
        return dt_time.fromisoformat(value)
    if kind == 'decimal':
        return Decimal(value)
    if kind == 'uuid':
        return UUID(value)
    if kind == 'bytes':
        return base64.b64decode(value)
    return obj


def _is_frame(value: Any, package: str) -> bool:
    cls = type(value)
    return cls.__name__ == 'DataFrame' and str(cls.__module__).split('.')[0] == package


def encode_value(value: Any) -> Tuple[str, str]:
    """
    Return `(kind, payload)` for a value: text that is safe for Valkey and memory alike.
    """
    if _is_frame(value, 'pandas'):
        try:
            import pyarrow as pa
            table = pa.Table.from_pandas(value, preserve_index=True)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            return 'pandas-arrow', base64.b64encode(sink.getvalue().to_pybytes()).decode('ascii')
        except ImportError:
            return 'pandas-json', value.to_json(orient='table', date_format='iso', date_unit='ns')
    if _is_frame(value, 'polars'):
        buffer = io.BytesIO()
        value.write_ipc(buffer)
        return 'polars-arrow', base64.b64encode(buffer.getvalue()).decode('ascii')
    if isinstance(value, (bytes, bytearray)):
        return 'bytes', base64.b64encode(bytes(value)).decode('ascii')
    return 'json', json.dumps(value, default=_json_default, separators=(',', ':'), allow_nan=True)


def decode_value(kind: str, payload: str) -> Any:
    """
    Return the value `encode_value()` encoded.
    """
    if kind == 'json':
        return json.loads(payload, object_hook=_json_object_hook)
    if kind == 'bytes':
        return base64.b64decode(payload)
    if kind == 'pandas-arrow':
        import pyarrow as pa
        reader = pa.ipc.open_stream(pa.py_buffer(base64.b64decode(payload)))
        return reader.read_all().to_pandas()
    if kind == 'pandas-json':
        import pandas as pd
        return pd.read_json(io.StringIO(payload), orient='table')
    if kind == 'polars-arrow':
        import polars as pl
        return pl.read_ipc(io.BytesIO(base64.b64decode(payload)))
    raise ValueError(f"Unknown cached value kind '{kind}'.")


def _pipe_ref(pipe: Any) -> str:
    return '|'.join(
        str(getattr(pipe, attr, None))
        for attr in ('instance_keys', 'connector_keys', 'metric_key', 'location_key')
    )


def _key_default(value: Any) -> Any:
    """How a function argument becomes part of its cache key."""
    if all(hasattr(value, attr) for attr in ('connector_keys', 'metric_key', 'instance_keys')):
        return 'Pipe:' + _pipe_ref(value)
    if isinstance(value, (datetime, date, dt_time)):
        return value.isoformat()
    if isinstance(value, (set, frozenset)):
        return sorted((repr(item) for item in value))
    if isinstance(value, (bytes, bytearray)):
        return hashlib.sha256(bytes(value)).hexdigest()
    if _is_frame(value, 'pandas'):
        import pandas as pd
        return 'pandas:' + str(int(pd.util.hash_pandas_object(value, index=True).sum()))
    if _is_frame(value, 'polars'):
        return 'polars:' + str(int(value.hash_rows().sum()))
    return repr(value)


class _MemoryStore:
    """Entries of this process: `{key: (expires_at, text)}`, least recently used first out."""

    def __init__(self):
        self.entries: 'OrderedDict[str, Tuple[Optional[float], str]]' = OrderedDict()
        self.size = 0
        self.lock = threading.RLock()

    def get(self, key: str) -> Optional[str]:
        with self.lock:
            entry = self.entries.get(key, None)
            if entry is None:
                return None
            expires_at, text = entry
            if expires_at is not None and time.monotonic() >= expires_at:
                self._pop(key)
                return None
            self.entries.move_to_end(key)
            return text

    def set(self, key: str, text: str, ttl: Optional[float], max_entries: int, max_bytes: int):
        expires_at = (time.monotonic() + ttl) if ttl is not None else None
        with self.lock:
            self._pop(key)
            if len(text) > max_bytes:
                return False
            self.entries[key] = (expires_at, text)
            self.size += len(text)
            now = time.monotonic()
            for stale in [k for k, (until, _) in self.entries.items() if until is not None and until <= now]:
                self._pop(stale)
            while self.entries and (len(self.entries) > max_entries or self.size > max_bytes):
                self._pop(next(iter(self.entries)))
            return True

    def _pop(self, key: str) -> bool:
        entry = self.entries.pop(key, None)
        if entry is None:
            return False
        self.size -= len(entry[1])
        return True

    def delete(self, key: str) -> bool:
        with self.lock:
            return self._pop(key)

    def delete_prefix(self, prefix: str) -> int:
        with self.lock:
            keys = [key for key in self.entries if key.startswith(prefix)]
            for key in keys:
                self._pop(key)
            return len(keys)


class Cache:
    """
    The cache of `meerschaum.utils.cache` (use the module's `cache` object): a decorator
    (`@cache(ttl=300, pipes=[pipe])`) and `get`, `set`, `delete` and `clear`. See the module.
    """

    #: How long (seconds) a listed pipe's latest sync is trusted before it is read again.
    version_seconds: float = 1.0
    #: How long (seconds) an unreachable Valkey is not asked again.
    retry_seconds: float = 60.0

    def __init__(self, max_entries: int = 2048, max_bytes: int = 128 * 1024 * 1024):
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self._memory = _MemoryStore()
        self._lock = threading.RLock()
        self._valkey_conn: Any = None
        self._valkey_until: float = 0.0
        self._probed: bool = False
        self._probing: bool = False
        self._warned: Dict[str, bool] = {}
        self._versions: Dict[str, Tuple[float, str]] = {}

    ### Backends

    def _connector_keys(self) -> Optional[str]:
        from meerschaum.config import get_config
        keys = get_config('api', 'cache', 'connector', warn=False)
        return str(keys) if keys else None

    def _warn_unreachable(self, keys: str, reason: str = '') -> None:
        if self._warned.get(keys, False):
            return
        self._warned[keys] = True
        ### Only the web console says so (once): elsewhere memory is the expected fallback.
        if os.environ.get('MRSM_API_HOST', '').lower() in ('', '0', 'false'):
            return
        from meerschaum.utils.warnings import warn
        reason = reason[len(keys) + 1:].strip() if reason.startswith(keys + ':') else reason
        warn(
            f"The cache connector '{keys}' is unreachable, so plugin caches keep their values in "
            "each process's memory" + (f" ({reason.rstrip('.')})" if reason else '') + '.',
            stack=False,
        )

    def _valkey(self) -> Any:
        """
        Return the Valkey connector when it answers, else `None`. The first lookup asks Valkey
        (or, while another thread is asking, answers `None`); later ones answer from the last probe
        and ask again in the background once it is old, so a dead server never makes a cache
        lookup wait twice.
        """
        now = time.monotonic()
        with self._lock:
            if self._probing or (self._probed and now < self._valkey_until):
                return self._valkey_conn
            self._probing = True
            first = not self._probed
        if first:
            return self._probe_valkey()
        threading.Thread(target=self._probe_valkey, name='mrsm-cache-probe', daemon=True).start()
        with self._lock:
            return self._valkey_conn

    def _probe_valkey(self) -> Any:
        """Ask the configured connector (`api:cache:connector`) and remember the answer."""
        now = time.monotonic()
        try:
            keys = self._connector_keys()
        except Exception:
            keys = None
        conn, reason = None, ''
        try:
            if keys and keys.startswith('valkey'):
                try:
                    import meerschaum as mrsm
                    candidate = mrsm.get_connector(keys)
                    if candidate.test_connection():
                        conn = candidate
                    else:
                        reason = 'no answer'
                except Exception as e:
                    reason = str(e).splitlines()[0][:200] if str(e) else type(e).__name__
                if conn is None:
                    self._warn_unreachable(keys, reason)
        finally:
            with self._lock:
                self._valkey_conn = conn
                self._valkey_until = now + (self.retry_seconds * (5 if conn is not None else 1))
                self._probed = True
                self._probing = False
        return conn

    def _valkey_failed(self, keys: Optional[str], error: Exception) -> None:
        with self._lock:
            self._valkey_conn = None
            self._valkey_until = time.monotonic() + self.retry_seconds
            self._probed = True
        if keys:
            self._warn_unreachable(keys, str(error).splitlines()[0][:200] if str(error) else '')

    @property
    def backend(self) -> str:
        """`'valkey'` when values are shared through Valkey, else `'memory'`."""
        return 'valkey' if self._valkey() is not None else 'memory'

    ### Entries

    def _pipe_version(self, pipe: Any) -> str:
        """
        The latest recorded sync of a pipe (its sync status, else its sync time).
        """
        ref = _pipe_ref(pipe)
        now = time.monotonic()
        with self._lock:
            memo = self._versions.get(ref, None)
            if memo is not None and memo[0] > now:
                return memo[1]
        version = 'none'
        get_status = getattr(pipe, 'get_sync_status', None)
        status = None
        if callable(get_status):
            try:
                status = get_status()
            except Exception:
                status = None
        if isinstance(status, dict):
            version = 'status:' + str(status.get('last_success', None))
        else:
            try:
                sync_time = pipe.get_sync_time()
                version = 'time:' + (
                    sync_time.isoformat() if hasattr(sync_time, 'isoformat') else str(sync_time)
                )
            except Exception:
                version = 'unknown'
        with self._lock:
            self._versions[ref] = (now + self.version_seconds, version)
        return version

    def _versions_of(self, pipes: Optional[List[Any]]) -> Dict[str, str]:
        return {_pipe_ref(pipe): self._pipe_version(pipe) for pipe in (pipes or [])}

    def _encode_entry(
        self,
        value: Any,
        pipes: Optional[List[Any]],
        versions: Optional[Dict[str, str]] = None,
    ) -> str:
        kind, payload = encode_value(value)
        header = {
            'v': FORMAT_VERSION,
            'kind': kind,
            'deps': versions if versions is not None else self._versions_of(pipes),
            'pipes': [_pipe_ref(pipe) for pipe in (pipes or [])],
        }
        return json.dumps(header, separators=(',', ':')) + '\n' + payload

    def _read_text(self, full_key: str) -> Optional[str]:
        conn = self._valkey()
        if conn is not None:
            try:
                return conn.get(full_key)
            except Exception as e:
                self._valkey_failed(self._connector_keys(), e)
        return self._memory.get(full_key)

    def _write_text(self, full_key: str, text: str, ttl: Optional[float]) -> bool:
        conn = self._valkey()
        if conn is not None:
            try:
                if ttl is None:
                    conn.set(full_key, text)
                else:
                    conn.set(full_key, text, ex=max(1, int(-(-ttl // 1))))
                return True
            except Exception as e:
                self._valkey_failed(self._connector_keys(), e)
        return self._memory.set(full_key, text, ttl, self.max_entries, self.max_bytes)

    def _delete_full_key(self, full_key: str) -> bool:
        deleted = self._memory.delete(full_key)
        conn = self._valkey()
        if conn is not None:
            try:
                deleted = bool(conn.client.delete(full_key)) or deleted
            except Exception as e:
                self._valkey_failed(self._connector_keys(), e)
        return deleted

    def _delete_prefix(self, prefix: str) -> int:
        count = self._memory.delete_prefix(prefix)
        conn = self._valkey()
        if conn is not None:
            try:
                keys = [
                    key.decode('utf-8') if isinstance(key, bytes) else str(key)
                    for key in (conn.client.keys(prefix + '*') or [])
                ]
                if keys:
                    count += int(conn.client.delete(*keys) or 0)
            except Exception as e:
                self._valkey_failed(self._connector_keys(), e)
        return count

    def _lookup(self, full_key: str) -> Any:
        """Return the value stored under a full key, or `_MISSING` (absent, expired or stale)."""
        text = self._read_text(full_key)
        if not text:
            return _MISSING
        header_text, _, payload = text.partition('\n')
        try:
            header = json.loads(header_text)
        except ValueError:
            return _MISSING
        if not isinstance(header, dict) or header.get('v', None) != FORMAT_VERSION:
            return _MISSING
        deps = header.get('deps', None) or {}
        if deps:
            current = self._current_versions(header.get('pipes', None) or [], deps)
            if current != deps:
                self._delete_full_key(full_key)
                return _MISSING
        try:
            return decode_value(str(header.get('kind', '')), payload)
        except Exception:
            return _MISSING

    def _current_versions(self, refs: List[str], deps: Dict[str, str]) -> Dict[str, str]:
        """The current versions of the pipes an entry depends on (rebuilt from their refs)."""
        import meerschaum as mrsm
        out = {}
        for ref in refs:
            parts = ref.split('|')
            if len(parts) != 4:
                out[ref] = 'unknown'
                continue
            instance_keys, connector_keys, metric_key, location_key = parts
            pipe = mrsm.Pipe(
                connector_keys,
                metric_key,
                None if location_key == 'None' else location_key,
                instance=instance_keys,
            )
            out[ref] = self._pipe_version(pipe)
        return out

    ### Public API

    def get(self, key: str, default: Any = None) -> Any:
        """
        Return the value cached under `key`, or `default` when there is none (or it expired, or
        a pipe it depends on synced since).
        """
        value = self._lookup(NAMESPACE + str(key))
        return default if value is _MISSING else value

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        *,
        pipes: Optional[List[Any]] = None,
    ) -> bool:
        """
        Cache `value` under `key` for `ttl` seconds (`None`: until it is deleted or evicted), or
        until one of `pipes` syncs. Raise a `TypeError` for a value that cannot be stored.

        Returns
        -------
        Whether the value was stored.
        """
        return self._set(key, value, ttl=ttl, pipes=pipes)

    def _set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        pipes: Optional[List[Any]] = None,
        versions: Optional[Dict[str, str]] = None,
    ) -> bool:
        if ttl is not None and ttl <= 0:
            return False
        text = self._encode_entry(value, pipes, versions=versions)
        return self._write_text(NAMESPACE + str(key), text, ttl)

    def delete(self, key: str) -> bool:
        """Forget `key`. Return whether it was cached."""
        return self._delete_full_key(NAMESPACE + str(key))

    def clear(self) -> int:
        """Forget every cached value (in Valkey: every key under `mrsm:cache:`). Return how many."""
        with self._lock:
            self._versions.clear()
        return self._delete_prefix(NAMESPACE)

    def __call__(
        self,
        function: Optional[Callable[..., Any]] = None,
        *,
        ttl: Optional[float] = 300,
        pipes: Union[List[Any], Callable[..., List[Any]], None] = None,
    ) -> Callable[..., Any]:
        """
        Decorate a function to cache its results, by its arguments.

        Parameters
        ----------
        ttl: Optional[float], default 300
            Seconds a result is kept (`None`: until evicted or a listed pipe syncs).

        pipes: Union[List[Pipe], Callable[..., List[Pipe]], None], default None
            Pipes whose syncs make a result stale, or a function of the call's arguments that
            returns them (e.g. `pipes=lambda pipe, **kw: [pipe]`).

        The decorated function has `cache_clear()` (forget its results), `cache_key(*args,
        **kwargs)` and `uncached` (the function itself).
        """
        def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            prefix = f"fn:{getattr(func, '__module__', '')}.{getattr(func, '__qualname__', func)}:"
            warned = {'value': False}

            def cache_key(*args, **kwargs) -> str:
                text = json.dumps(
                    {'args': list(args), 'kwargs': kwargs},
                    default=_key_default,
                    sort_keys=True,
                    separators=(',', ':'),
                )
                return prefix + hashlib.sha256(text.encode('utf-8')).hexdigest()

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                key = cache_key(*args, **kwargs)
                value = self._lookup(NAMESPACE + key)
                if value is not _MISSING:
                    return value
                dependencies = list((pipes(*args, **kwargs) if callable(pipes) else pipes) or [])
                ### Read the pipes' syncs before computing: a sync meanwhile makes the result stale.
                versions = self._versions_of(dependencies)
                result = func(*args, **kwargs)
                try:
                    self._set(key, result, ttl=ttl, pipes=dependencies, versions=versions)
                except (CacheValueError, TypeError, ValueError) as e:
                    if not warned['value']:
                        warned['value'] = True
                        from meerschaum.utils.warnings import warn
                        warn(f"Not caching the results of {func.__qualname__}: {e}", stack=False)
                return result

            wrapper.cache_key = cache_key
            wrapper.cache_clear = lambda: self._delete_prefix(NAMESPACE + prefix)
            wrapper.uncached = func
            return wrapper

        if function is not None:
            if not callable(function):
                raise TypeError("`@cache` decorates a function (`@cache` or `@cache(ttl=...)`).")
            return _decorator(function)
        return _decorator


#: The cache plugins share (see the module's documentation).
cache = Cache()


def probe() -> str:
    """
    Ask the configured cache connector (in the API host a warning names it when it is
    unreachable, once) and return the backend in use (`'valkey'` or `'memory'`).
    """
    return cache.backend
