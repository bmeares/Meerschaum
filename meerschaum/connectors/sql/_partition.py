#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Native range partitioning for non-TimescaleDB SQL flavors.

TimescaleDB auto-creates chunks on insert; other flavors do not. These methods, mixed into
`SQLConnector`, declare a partitioned parent table (`PARTITION BY RANGE` on the datetime column)
and pre-create the interval-aligned child partitions a dataframe needs before it is inserted.

Partitioning is on by default for datetime-axis pipes on these flavors (`hypertable` defaults to
`True`, the same flag TimescaleDB uses); set `pipe.parameters['hypertable'] = False` to opt out.
The partition width reuses the pipe's chunk interval (`pipe.parameters['verify']['chunk_minutes']`,
default 43200 — 30 days).
"""

from __future__ import annotations


import meerschaum as mrsm
from meerschaum.utils.typing import Any, Optional, SuccessTuple

def get_partition_info(self, pipe: mrsm.Pipe, debug: bool = False) -> dict:
    """
    Return a summary of a pipe's target table partitioning for `show partitions`.

    Keys:
    - `flavor`: the connector flavor.
    - `partitioned`: whether the table is range-partitioned (native) or a TimescaleDB hypertable.
    - `count`: the number of partitions / chunks (`None` if unknown).
    - `interval`: the physical partition width (`timedelta`, epoch-`int`, or `None`).
    """
    return self._rust_method('get_partition_info')(pipe)


def partition_pipe(
    self,
    pipe: mrsm.Pipe,
    chunk_minutes: Optional[int] = None,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Rebuild a pipe's target table to a new partition (chunk) width.

    The width is taken from `chunk_minutes` if provided, else the pipe's configured
    `verify.chunk_minutes`. The new width is persisted to `verify.chunk_minutes`, which is the
    authoritative partition width (see `Pipe.get_chunk_interval`).

    Strategy by flavor:

    - **TimescaleDB**: call `set_chunk_time_interval()`. This changes the width of FUTURE chunks
      only; existing chunks are not rewritten.
    - **PostgreSQL / PostGIS, MySQL / MariaDB, MSSQL**: rebuild the table by reading its data,
      dropping it, and re-syncing at the new width. This reuses the tested `create_pipe_table_from_df`
      and `_create_missing_partitions` paths, and (for MSSQL) frees the partition function/scheme
      names so they can be recreated. The whole table is read into memory; for very large tables
      consider a manual chunked rebuild.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The partitioned pipe whose target table to repartition.

    chunk_minutes: Optional[int], default None
        The new partition width in minutes. Defaults to the pipe's `verify.chunk_minutes`.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    result = self._rust_method('partition_pipe')(pipe, chunk_minutes=chunk_minutes)
    if result[0] and not pipe.temporary:
        ### v4 sets `verify.chunk_minutes` on the Python pipe (`update_parameters`);
        ### Rust persisted it, so mirror the registered value in memory.
        verify = (
            (self.get_pipe_attributes(pipe, debug=debug) or {})
            .get('parameters', {})
            .get('verify', None)
        )
        if isinstance(verify, dict) and 'chunk_minutes' in verify:
            pipe.update_parameters(
                {'verify': {'chunk_minutes': verify['chunk_minutes']}},
                persist=False,
                debug=debug,
            )
    return result
