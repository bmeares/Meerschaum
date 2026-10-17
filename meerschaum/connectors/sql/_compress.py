#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Measure and reduce the disk usage of pipes' target tables.

These methods are mixed into `SQLConnector` (see `meerschaum/connectors/sql/_SQLConnector.py`).
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import Union, Any, SuccessTuple

def get_pipe_size(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> Union[int, None]:
    """
    Return the on-disk size of a pipe's target table in bytes.

    For TimescaleDB hypertables, the total hypertable size (including chunks and indexes)
    is returned. Other flavors use their native size functions where available.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table size to measure.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    An `int` of the number of bytes occupied by the target table,
    or `None` if the size could not be determined.
    """
    return self._rust_method('get_pipe_size')(pipe)


def set_integer_now_func(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> bool:
    """
    Register an `integer_now` function if the pipe's datetime axis is an integer.
    Returns `True` when nothing needed doing or the function was registered.
    """
    return self._rust_method('set_integer_now_func')(pipe)


def apply_compression_policy(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Idempotently enable compression and install a compression policy for a pipe.

    Intended to be called automatically (e.g. after a sync) when `pipe.compress` is set.
    Only TimescaleDB hypertables are affected; all other flavors are a no-op success.
    Failures are non-fatal and never raise.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table should have a compression policy.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    return self._rust_method('apply_compression_policy')(pipe)


def compress_pipe(
    self,
    pipe: mrsm.Pipe,
    no_policy: bool = False,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Compress a pipe's target table to reduce disk usage.

    For TimescaleDB, enables the Hypercore columnstore, installs a columnstore (compression)
    policy (so future synced chunks are converted automatically), and converts any existing
    uncompressed chunks now. For MySQL/MariaDB and MSSQL, applies the flavor's native table
    compression. Other flavors are unsupported.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table to compress.

    no_policy: bool, default False
        If `True` (TimescaleDB only), compress existing chunks now without installing an ongoing
        columnstore (compression) policy. Any pre-existing policy is left untouched.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` indicating success, including the amount of disk reclaimed.
    """
    return self._rust_method('compress_pipe')(pipe, no_policy=no_policy)


def decompress_pipe(
    self,
    pipe: mrsm.Pipe,
    no_policy: bool = False,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Decompress a pipe's target table, the inverse of `compress_pipe()`.

    For TimescaleDB, removes the columnstore (compression) policy, converts every compressed
    chunk back to row-store, and disables the columnstore so future synced chunks stay
    uncompressed. For MySQL/MariaDB and MSSQL, reverts the flavor's native table compression.
    Other flavors are unsupported.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table to decompress.

    no_policy: bool, default False
        If `True` (TimescaleDB only), decompress existing chunks now but leave the columnstore
        (compression) policy in place — chunks will be recompressed on the policy's schedule.
        Useful to temporarily decompress for a bulk backfill without disabling compression.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` indicating success, including the change in disk size.
    """
    return self._rust_method('decompress_pipe')(pipe, no_policy=no_policy)
