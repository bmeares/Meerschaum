#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Wave 15 (L) test plugin: a deterministic `fetch()` source.

`parameters['fetch']['mode']` selects what `fetch()` returns: `frame` (a list of documents, the
default), `generator` (chunks of documents), `hook` (every chunk goes to `chunk_hook` and the
list of its results is returned), `none` (`None`) and `true` (`True`, "syncing in parallel").
Rows are 12 hourly documents from 2026-01-01 (UTC), cut by `begin` and `end`.
"""

__version__ = '0.1.0'

from datetime import datetime, timedelta, timezone

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def register(pipe):
    return {
        'columns': {'datetime': 'dt', 'id': 'id'},
        'dtypes': {'val': 'float'},
    }


def _rows(begin, end):
    rows = [
        {'dt': BASE + timedelta(hours=i), 'id': i % 3, 'val': i / 2}
        for i in range(12)
    ]
    if begin is not None:
        rows = [row for row in rows if row['dt'] >= begin]
    if end is not None:
        rows = [row for row in rows if row['dt'] < end]
    return rows


def fetch(pipe, begin=None, end=None, chunk_hook=None, sync_chunks=False, **kw):
    mode = (pipe.parameters.get('fetch', None) or {}).get('mode', 'frame')
    rows = _rows(begin, end)
    if mode == 'none':
        return None
    if mode == 'true':
        return True
    if mode == 'generator':
        return (rows[i:i + 5] for i in range(0, len(rows), 5))
    if mode == 'hook':
        return [chunk_hook(rows[i:i + 5]) for i in range(0, len(rows), 5)]
    return rows
