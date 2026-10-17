#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""Wave 15 (L) test plugin: a `sync()` that calls `pipe.sync()` itself."""

__version__ = '0.1.0'

from datetime import datetime, timedelta, timezone

BASE = datetime(2026, 2, 1, tzinfo=timezone.utc)


def register(pipe):
    return {'columns': {'datetime': 'dt', 'id': 'id'}}


def sync(pipe, debug=False, flag=None, **kw):
    """Sync two batches through `pipe.sync()` and report both results."""
    first = pipe.sync([{'dt': BASE, 'id': 1, 'val': 1}])
    second = pipe.sync([{'dt': BASE + timedelta(minutes=1), 'id': 1, 'val': 2 if flag is None else flag}])
    if not first[0] or not second[0]:
        return False, f"{first[1]} | {second[1]}"
    return True, f"synced twice: {first[1]} | {second[1]}"
