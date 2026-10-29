#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Worker test plugin: `fetch()` returns every shape v4 accepts, chosen by the pipe's metric key.
All data is synthetic and deterministic.
"""

__version__ = '1.0.0'

from datetime import datetime, timezone, timedelta

BASE = datetime(2024, 1, 1, tzinfo=timezone.utc)


def register(pipe):
    """Return the default parameters."""
    return {
        'columns': {'datetime': 'dt', 'id': 'id'},
        'fetch': {'backtrack_minutes': 30},
    }


def _docs(n=3, offset=0):
    return [
        {
            'dt': BASE + timedelta(minutes=i + offset),
            'id': (i % 2) + 1,
            'val': float(i) / 2,
            'label': f"row-{i + offset}",
        }
        for i in range(n)
    ]


def fetch(pipe, begin=None, end=None, chunksize=None, **kw):
    """Return the shape named by the metric key."""
    import meerschaum as mrsm
    shape = pipe.metric_key
    if shape == 'docs':
        return _docs()
    if shape == 'docs_strdates':
        return [
            {'dt': (BASE + timedelta(minutes=i)).isoformat(), 'id': 1, 'val': i}
            for i in range(3)
        ]
    if shape == 'columns':
        docs = _docs()
        return {key: [doc[key] for doc in docs] for key in docs[0]}
    if shape == 'df':
        pd = mrsm.attempt_import('pandas')
        return pd.DataFrame(_docs())
    if shape == 'polars':
        pl = mrsm.attempt_import('polars')
        return pl.DataFrame(_docs())
    if shape == 'json':
        import json
        return json.dumps([
            {'dt': doc['dt'].isoformat(), 'id': doc['id'], 'val': doc['val'], 'label': doc['label']}
            for doc in _docs()
        ])
    if shape == 'lines':
        return "dt:2024-01-01T00:00:00+00:00,id:1,val:1\ndt:2024-01-01T00:01:00+00:00,id:2,val:2"
    if shape == 'generator':
        def gen():
            pd = mrsm.attempt_import('pandas')
            yield _docs(2, 0)
            yield pd.DataFrame(_docs(2, 10))
            yield {'dt': [BASE + timedelta(minutes=20)], 'id': [1], 'val': [9.5], 'label': ['row-20']}
        return gen()
    if shape == 'special':
        from decimal import Decimal
        from uuid import UUID
        pd = mrsm.attempt_import('pandas')
        return pd.DataFrame([
            {
                'dt': BASE,
                'id': 1,
                'meta': {'a': 1, 'b': [1, 2]},
                'amount': Decimal('1.10'),
                'uid': UUID('12345678-1234-5678-1234-567812345678'),
                'raw': b'\x00\x01',
            },
        ])
    if shape == 'bounds':
        return [{'dt': BASE, 'id': 1, 'begin': str(begin), 'end': str(end)}]
    if shape == 'hooked':
        ### Sync chunks through v4's `chunk_hook` and return the hook results.
        chunk_hook = kw.get('chunk_hook', None)
        chunks = [_docs(2, 0), _docs(2, 30)]
        if chunk_hook is None:
            return chunks[0] + chunks[1]
        return [chunk_hook(chunk) for chunk in chunks]
    if shape == 'none':
        return None
    if shape == 'empty':
        return []
    if shape == 'raise':
        raise ValueError("synthetic fetch failure")
    raise ValueError(f"Unknown shape '{shape}'.")
