#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Worker test plugin for the `Pipe` methods (wave 15, task L2).

- `w15l2back:<label>`: an `IS_INSTANCE`, `IS_THREAD_SAFE` connector storing pipes as JSON files
  under the root directory that defines its own `get_backtrack_data()` (every call is recorded in
  a calls file) and sleeps `delay` seconds per `get_pipe_data()` (to observe concurrency).
- `w15l2src:<label>`: a source with `fetch()` (a generator of three chunks; with a `chunk_hook`,
  the list of the hook's results), `get_sync_time()` and `get_pipe_rowcount()` that answer
  `remote=True`.
- `w15l2nosrc:<label>`: a source whose `get_pipe_rowcount()` takes no `remote` and that has no
  `get_sync_time()`.
"""

__version__ = '0.1.0'

import contextlib
import fcntl
import json
import os
import pathlib
import time
from datetime import datetime, timezone, timedelta

import meerschaum as mrsm
from meerschaum.connectors import make_connector, Connector, InstanceConnector

BASE = datetime(2025, 3, 1, tzinfo=timezone.utc)


def _root() -> pathlib.Path:
    import meerschaum.config.paths as paths
    return pathlib.Path(paths.ROOT_DIR_PATH)


def _store_path(label: str) -> pathlib.Path:
    return _root() / f'w15l2back_{label}.json'


def _calls_path(label: str) -> pathlib.Path:
    return _root() / f'w15l2back_{label}_calls.json'


@contextlib.contextmanager
def _locked(label: str):
    """
    An exclusive cross-process lock around a read-modify-write of the store: the connector is
    `IS_THREAD_SAFE`, and concurrent chunks (threads or worker processes) must not lose each
    other's updates.
    """
    with open(_root() / f'w15l2back_{label}.lock', 'a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _inflight_path(label: str) -> pathlib.Path:
    return _root() / f'w15l2back_{label}_inflight.json'


def _load(label: str) -> dict:
    path = _store_path(label)
    if not path.exists():
        return {'pipes': {}, 'next_id': 1}
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _save(label: str, store: dict) -> None:
    path = _store_path(label)
    tmp = path.with_suffix('.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(store, f, default=str)
    os.replace(tmp, path)


def _key(pipe) -> str:
    return f"{pipe.connector_keys}|{pipe.metric_key}|{pipe.location_key}"


def _serialized(method):
    """Run a mutating method of the store under the store's lock."""
    import functools

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with _locked(self.label):
            return method(self, *args, **kwargs)

    return wrapper


@make_connector
class W15l2backConnector(InstanceConnector):
    """A file-backed instance connector with its own `get_backtrack_data()`."""

    IS_INSTANCE = True
    IS_THREAD_SAFE = True
    REQUIRED_ATTRIBUTES = []

    @_serialized
    def register_pipe(self, pipe, debug=False, **kwargs):
        store = _load(self.label)
        key = _key(pipe)
        if key in store['pipes']:
            return False, f"{pipe} is already registered."
        store['pipes'][key] = {
            'pipe_id': store['next_id'],
            'connector_keys': pipe.connector_keys,
            'metric_key': pipe.metric_key,
            'location_key': pipe.location_key,
            'parameters': pipe._attributes.get('parameters', {}),
            'rows': [],
        }
        store['next_id'] += 1
        _save(self.label, store)
        return True, "Success"

    def get_pipe_attributes(self, pipe, debug=False, **kwargs):
        doc = _load(self.label)['pipes'].get(_key(pipe), None)
        if doc is None:
            return {}
        return {k: v for k, v in doc.items() if k != 'rows'}

    def get_pipe_id(self, pipe, debug=False, **kwargs):
        doc = _load(self.label)['pipes'].get(_key(pipe), None)
        return doc['pipe_id'] if doc else None

    @_serialized
    def edit_pipe(self, pipe, debug=False, **kwargs):
        store = _load(self.label)
        doc = store['pipes'].get(_key(pipe), None)
        if doc is None:
            return False, f"{pipe} is not registered."
        doc['parameters'] = pipe._attributes.get('parameters', {})
        _save(self.label, store)
        return True, "Success"

    @_serialized
    def delete_pipe(self, pipe, debug=False, **kwargs):
        store = _load(self.label)
        store['pipes'].pop(_key(pipe), None)
        _save(self.label, store)
        return True, "Success"

    def fetch_pipes_keys(self, connector_keys=None, metric_keys=None, location_keys=None,
                         tags=None, params=None, debug=False, **kwargs):
        return [
            (doc['connector_keys'], doc['metric_key'], doc['location_key'])
            for doc in _load(self.label)['pipes'].values()
        ]

    def pipe_exists(self, pipe, debug=False, **kwargs):
        doc = _load(self.label)['pipes'].get(_key(pipe), None)
        return bool(doc and doc['rows'])

    @_serialized
    def drop_pipe(self, pipe, debug=False, **kwargs):
        store = _load(self.label)
        doc = store['pipes'].get(_key(pipe), None)
        if doc is not None:
            doc['rows'] = []
            _save(self.label, store)
        return True, "Success"

    @_serialized
    def sync_pipe(self, pipe, df=None, check_existing=True, debug=False, **kwargs):
        store = _load(self.label)
        doc = store['pipes'].setdefault(_key(pipe), {
            'pipe_id': store['next_id'],
            'connector_keys': pipe.connector_keys,
            'metric_key': pipe.metric_key,
            'location_key': pipe.location_key,
            'parameters': {},
            'rows': [],
        })
        records = json.loads(df.to_json(orient='records', date_format='iso', date_unit='us'))
        doc['rows'].extend(records)
        _save(self.label, store)
        return True, f"Inserted {len(records)} rows."

    @_serialized
    def clear_pipe(self, pipe, begin=None, end=None, params=None, debug=False, **kwargs):
        store = _load(self.label)
        doc = store['pipes'].get(_key(pipe), None)
        if doc is not None:
            dt_col = pipe.columns.get('datetime', None)
            keep = []
            for row in doc['rows']:
                value = row.get(dt_col) if dt_col else None
                if value is not None and (begin is not None or end is not None):
                    ts = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    if (begin is None or ts >= begin) and (end is None or ts < end):
                        continue
                    keep.append(row)
                elif begin is None and end is None:
                    continue
                else:
                    keep.append(row)
            doc['rows'] = keep
            _save(self.label, store)
        return True, "Success"

    def get_pipe_data(self, pipe, select_columns=None, omit_columns=None, begin=None, end=None,
                      params=None, debug=False, **kwargs):
        import pandas as pd
        self._record_marker(kwargs)
        delay = float(self.__dict__.get('delay', 0) or 0)
        ### Meerschaum's own pipes on this instance (the per-pipe sync status a sync records in
        ### the background, `mrsm:engine_sync_status`) are not the chunks being counted.
        if delay and pipe.connector_keys != 'mrsm':
            ### Record how many reads are in flight at once (the test reads the maximum).
            with _locked(self.label):
                path = _inflight_path(self.label)
                state = json.loads(path.read_text()) if path.exists() else {'now': 0, 'max': 0}
                state['now'] += 1
                state['max'] = max(state['max'], state['now'])
                path.write_text(json.dumps(state))
            try:
                time.sleep(delay)
            finally:
                with _locked(self.label):
                    state = json.loads(path.read_text())
                    state['now'] -= 1
                    path.write_text(json.dumps(state))
        doc = _load(self.label)['pipes'].get(_key(pipe), None)
        if doc is None:
            return None
        df = pd.DataFrame(doc['rows'])
        dt_col = pipe.columns.get('datetime', None)
        if dt_col and dt_col in df.columns:
            df[dt_col] = pd.to_datetime(df[dt_col], utc=True)
            if begin is not None:
                df = df[df[dt_col] >= begin]
            if end is not None:
                df = df[df[dt_col] < end]
        return df.reset_index(drop=True)

    def _record_marker(self, kwargs):
        marker = kwargs.get('w15l2_marker', None)
        if marker is None:
            return
        path = _root() / f'w15l2back_{self.label}_markers.json'
        with _locked(self.label):
            seen = json.loads(path.read_text()) if path.exists() else []
            seen.append(marker)
            path.write_text(json.dumps(seen))

    def get_backtrack_data(self, pipe, begin=None, backtrack_minutes=None, params=None,
                           limit=None, debug=False, **kwargs):
        self._record_marker(kwargs)
        path = _calls_path(self.label)
        with _locked(self.label):
            calls = json.loads(path.read_text()) if path.exists() else []
            calls.append({'backtrack_minutes': backtrack_minutes, 'limit': limit})
            path.write_text(json.dumps(calls))
        df = self.get_pipe_data(pipe)
        if df is None or df.empty:
            return df
        dt_col = pipe.columns['datetime']
        start = (begin if begin is not None else df[dt_col].max()) - timedelta(
            minutes=float(backtrack_minutes or 0)
        )
        out = df[df[dt_col] >= start].sort_values(dt_col, ascending=False)
        return out.head(limit).reset_index(drop=True) if limit else out.reset_index(drop=True)

    def get_sync_time(self, pipe, params=None, newest=True, debug=False, **kwargs):
        df = self.get_pipe_data(pipe)
        dt_col = pipe.columns.get('datetime', None)
        if df is None or df.empty or not dt_col:
            return None
        value = df[dt_col].max() if newest else df[dt_col].min()
        return value.to_pydatetime()

    def get_pipe_rowcount(self, pipe, begin=None, end=None, params=None, remote=False,
                          debug=False, **kwargs):
        df = self.get_pipe_data(pipe, begin=begin, end=end)
        return 0 if df is None else len(df)

    def get_pipe_columns_types(self, pipe, debug=False, **kwargs):
        return {}


def _chunks(label: str):
    return [
        [
            {
                'dt': BASE + timedelta(hours=(chunk * 4) + i),
                'id': i % 2,
                'val': float((chunk * 4) + i),
                'name': f'{label}{chunk}{i}',
            }
            for i in range(4)
        ]
        for chunk in range(3)
    ]


@make_connector
class W15l2srcConnector(Connector):
    """A source with a chunked `fetch()` and remote `get_sync_time()` / `get_pipe_rowcount()`."""

    REQUIRED_ATTRIBUTES = []

    def fetch(self, pipe, begin=None, end=None, chunk_hook=None, **kwargs):
        chunks = _chunks(self.label)
        if chunk_hook is not None:
            return [chunk_hook(chunk) for chunk in chunks]
        return (chunk for chunk in chunks)

    def get_sync_time(self, pipe, params=None, newest=True, remote=False, debug=False, **kwargs):
        return BASE + timedelta(hours=11 if newest else 0) + timedelta(days=1 if remote else 0)

    def get_pipe_rowcount(self, pipe, begin=None, end=None, params=None, remote=False,
                          debug=False, **kwargs):
        return 12 if remote else 0


@make_connector
class W15l2nosrcConnector(Connector):
    """A source whose rowcount takes no `remote` and that has no `get_sync_time()`."""

    REQUIRED_ATTRIBUTES = []

    def fetch(self, pipe, begin=None, end=None, **kwargs):
        return _chunks(self.label)[0]

    def get_pipe_rowcount(self, pipe, begin=None, end=None, params=None, debug=False):
        return 99
