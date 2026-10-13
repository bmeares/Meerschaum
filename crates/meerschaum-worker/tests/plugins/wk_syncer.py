#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""Worker test plugin: a `sync()` that calls `pipe.sync()` itself."""

__version__ = '1.0.0'

from datetime import datetime, timezone, timedelta

BASE = datetime(2024, 2, 1, tzinfo=timezone.utc)


def register(pipe):
    return {'columns': {'datetime': 'dt', 'id': 'id'}}


def sync(pipe, debug=False, **kw):
    """Sync two batches through `pipe.sync()` and report both results."""
    first = pipe.sync([{'dt': BASE, 'id': 1, 'val': 1}])
    second = pipe.sync([{'dt': BASE + timedelta(minutes=1), 'id': 1, 'val': 2}])
    if not first[0] or not second[0]:
        return False, f"{first[1]} | {second[1]}"
    return True, f"synced twice: {first[1]} | {second[1]}"
