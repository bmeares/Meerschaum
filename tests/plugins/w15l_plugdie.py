#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""Wave 15 (L) test plugin: `fetch()` kills the worker process (a crash, not an exception)."""

__version__ = '0.1.0'


def register(pipe):
    return {'columns': {'datetime': 'dt'}}


def fetch(pipe, **kw):
    import os
    ### Talk to the host first (a callback into the engine), then crash mid-call.
    pipe.get_sync_time()
    pipe.get_rowcount()
    os._exit(7)
