#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Worker test plugin: custom connectors.

- `w15lfeed:<label>`: a plain connector with `fetch()`.
- `w15lmem:<label>`: an `IS_INSTANCE = True` connector storing pipes as JSON files (one per label) under the root
  directory, so every worker process sees the same data.
- `w15lstream:<label>`: an `IS_STREAMING = True` connector with `subscribe(pipe, sink)`.
"""

__version__ = '0.3.0'

import json
import os
import pathlib
import tempfile
from functools import wraps
from datetime import datetime, timezone, timedelta

import meerschaum as mrsm
from meerschaum.connectors import make_connector, Connector, InstanceConnector


@make_connector
class W15lfeedConnector(Connector):
    """A plain custom connector with `fetch()`."""

    REQUIRED_ATTRIBUTES = []

    def fetch(self, pipe, begin=None, end=None, **kwargs):
        base = datetime(2024, 4, 1, tzinfo=timezone.utc)
        return [
            {'ts': base + timedelta(hours=i), 'sensor': self.label, 'reading': i * 10}
            for i in range(4)
        ]


def _store_path(label: str) -> pathlib.Path:
    import meerschaum.config.paths as paths
    return pathlib.Path(paths.ROOT_DIR_PATH) / f'w15lmem_{label}.json'


def _load(label: str) -> dict:
    path = _store_path(label)
    if not path.exists():
        return {'pipes': {}, 'next_id': 1}
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _save(label: str, store: dict) -> None:
    path = _store_path(label)
    tmp = None
    try:
        with tempfile.NamedTemporaryFile(
            mode='w', encoding='utf-8', dir=path.parent,
            prefix=path.name + '.', suffix='.tmp', delete=False,
        ) as f:
            tmp = pathlib.Path(f.name)
            json.dump(store, f, default=str)
        os.replace(tmp, path)
    finally:
        if tmp is not None:
            tmp.unlink(missing_ok=True)


def _serialized(method):
    """Primary operations and background telemetry share one process-safe store lock."""
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        from meerschaum.utils.locks import InterProcessLock
        with InterProcessLock(_store_path(self.label).with_suffix('.lock')):
            return method(self, *args, **kwargs)
    return wrapped


def _key(pipe) -> str:
    return f"{pipe.connector_keys}|{pipe.metric_key}|{pipe.location_key}"


@make_connector
class W15lmemConnector(InstanceConnector):
    """A file-backed instance connector."""

    IS_INSTANCE = True
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
        keys = []
        for doc in _load(self.label)['pipes'].values():
            if connector_keys and doc['connector_keys'] not in connector_keys:
                continue
            if metric_keys and doc['metric_key'] not in metric_keys:
                continue
            keys.append((doc['connector_keys'], doc['metric_key'], doc['location_key']))
        return keys

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
        return True, f"Inserted {len(records)} rows (check_existing={check_existing})."

    @_serialized
    def clear_pipe(self, pipe, begin=None, end=None, params=None, debug=False, **kwargs):
        store = _load(self.label)
        doc = store['pipes'].get(_key(pipe), None)
        if doc is not None:
            doc['rows'] = []
            _save(self.label, store)
        return True, "Success"

    def get_pipe_data(self, pipe, select_columns=None, omit_columns=None, begin=None, end=None,
                      params=None, debug=False, **kwargs):
        import pandas as pd
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


@make_connector
class W15lstreamConnector(Connector):
    """A streaming connector (`IS_STREAMING = True`) pushing batches into the engine's sink."""

    IS_STREAMING = True
    REQUIRED_ATTRIBUTES = []

    def subscribe(self, pipe, sink, **kwargs):
        base = datetime(2024, 6, 1, tzinfo=timezone.utc)
        results = []
        for i in range(3):
            results.append(sink([{'dt': base + timedelta(minutes=i), 'id': 1, 'val': i}]))
        failed = [msg for success, msg in results if not success]
        if failed:
            return False, '\n'.join(failed)
        return True, f"pushed {len(results)} batches"
