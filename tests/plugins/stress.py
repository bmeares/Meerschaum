#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Stress test for pipes.
"""

__version__ = '0.3.7'

from datetime import datetime, timezone, timedelta
import random
import math

import meerschaum as mrsm
from meerschaum.utils.misc import iterate_chunks
from meerschaum.utils.typing import Optional

def register(pipe):
    """
    Return the default parameters.
    """
    return {
        'columns': {
            'datetime': 'datetime',
            'id': 'id',
        },
        'fetch': {
            'rows': 1440,
            'ids': 3,
            'backtrack_minutes': 30,
        },
        'upsert': True,
    }

def fetch(
    pipe: mrsm.Pipe,
    chunksize: Optional[int] = None,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    **kw
):
    """
    Yield random chunks of data.
    """
    _edit_pipe = False

    sync_time = pipe.get_sync_time(round_down=False)
    now = (
        begin
        if begin is not None
        else (
            sync_time
            if sync_time is not None
            else datetime.now(timezone.utc).replace(tzinfo=None)
        )
    )
    if chunksize is None or chunksize < 1:
        chunksize = 1440

    dt_col, id_col, val_col = 'datetime', 'id', 'val'

    fetch_params = pipe.parameters.get('fetch', {})
    row_limit = fetch_params.get('rows', None) or 1440
    num_ids = fetch_params.get('ids', None) or 3

    def stop_generating(dt_val, num_yielded_rows):
        if end is not None:
            return dt_val >= end
        return num_yielded_rows >= row_limit

    yielded_rows = 0
    while True:

        chunk = []
        for _ in range(chunksize):
            if stop_generating(now, yielded_rows):
                break

            chunk.append({
                dt_col: now,
                id_col: random.randint(1, num_ids),
                val_col: random.randint(1, 100),
            })
            now += timedelta(minutes=1)
            yielded_rows += 1

        if chunk:
            yield chunk
        if stop_generating(now, yielded_rows):
            break
