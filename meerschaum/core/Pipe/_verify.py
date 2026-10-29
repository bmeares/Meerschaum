#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Verify the contents of a pipe by resyncing its interval.
"""

from datetime import datetime, timedelta

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Any, Optional, Union, Tuple, Dict
from meerschaum._internal.static import STATIC_CONFIG

### `Pipe.sync()` keywords `Pipe.verify()` passes as explicit arguments to `_core.verify_pipe`
### (every other keyword is forwarded to each chunk's sync: `force`, `retries`, plugin arguments).
_VERIFY_SYNC_KWARGS = frozenset({'chunksize', 'check_existing'})


def _rust_verify_context(
    pipe: 'mrsm.Pipe',
    debug: bool = False,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Return `(context, '')` with the connectors `_core.verify_pipe` needs (the pipe's instance
    and source, as the Rust SQL connectors this process holds, and the attributes of the
    connectors it defined ad hoc), or `(None, reason)` when a connector has no native driver.
    """
    from meerschaum.core.Pipe._sync import _native_context
    context, reason = _native_context(pipe, debug=debug)
    if context is None:
        return None, reason
    connectors = dict(context['connectors'])
    if context['connector'] is not None:
        connectors[str(pipe.instance_keys)] = context['connector']
    return {'connectors': connectors, 'attributes': context['attributes']}, ''


def verify(
    self,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    chunk_interval: Union[timedelta, int, None] = None,
    bounded: Optional[bool] = None,
    deduplicate: bool = False,
    workers: Optional[int] = None,
    batchsize: Optional[int] = None,
    skip_chunks_with_greater_rowcounts: bool = False,
    check_rowcounts_only: bool = False,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Verify the contents of the pipe by resyncing its interval.

    Parameters
    ----------
    begin: Union[datetime, int, None], default None
        If specified, only verify rows greater than or equal to this value.

    end: Union[datetime, int, None], default None
        If specified, only verify rows less than this value.

    chunk_interval: Union[timedelta, int, None], default None
        If provided, use this as the size of the chunk boundaries.
        Default to the value set in `pipe.parameters['verify']['chunk_minutes']` (43200 — 30 days).

    bounded: Optional[bool], default None
        If `True`, do not verify older than the oldest sync time or newer than the newest.
        If `False`, verify unbounded syncs outside of the new and old sync times.
        The default behavior (`None`) is to bound only if a bound interval is set
        (e.g. `pipe.parameters['verify']['bound_days']`).

    deduplicate: bool, default False
        If `True`, deduplicate the pipe's table after the verification syncs.

    workers: Optional[int], default None
        If provided, limit the verification to this many threads.
        Use a value of `1` to sync chunks in series.

    batchsize: Optional[int], default None
        If provided, sync this many chunks in parallel.
        Defaults to `Pipe.get_num_workers()`.

    skip_chunks_with_greater_rowcounts: bool, default False
        If `True`, compare the rowcounts for a chunk and skip syncing if the pipe's
        chunk rowcount equals or exceeds the remote's rowcount.

    check_rowcounts_only: bool, default False
        If `True`, only compare rowcounts and print chunks which are out-of-sync.

    debug: bool, default False
        Verbosity toggle.

    kwargs: Any
        All keyword arguments are passed to `pipe.sync()`.

    Returns
    -------
    A SuccessTuple indicating whether the pipe was successfully resynced.
    """
    from meerschaum import _core
    ### Verifying re-syncs its chunks: a `sync` lock refuses it before any is read.
    if not check_rowcounts_only:
        refusal = self._lock_refusal('sync', always=True, debug=debug)
        if refusal:
            return False, refusal
    context, reason = _rust_verify_context(self, debug=debug)
    if context is None:
        return False, f"Cannot verify {self}: {reason}."
    result = _core.verify_pipe(
        self,
        context['connectors'],
        begin=begin,
        end=end,
        params=params,
        chunk_interval=chunk_interval,
        bounded=bounded,
        deduplicate=deduplicate,
        workers=workers,
        batchsize=batchsize,
        skip_chunks_with_greater_rowcounts=skip_chunks_with_greater_rowcounts,
        check_rowcounts_only=check_rowcounts_only,
        chunksize=kwargs.get('chunksize', -1),
        check_existing=kwargs.get('check_existing', True),
        kwargs={
            key: value
            for key, value in kwargs.items()
            if key not in _VERIFY_SYNC_KWARGS
        },
        attributes=context['attributes'],
        debug=debug,
    )
    ### The chunk syncs ran in Rust: drop what this object cached about the table.
    self._invalidate_cache(hard=True, debug=debug)
    self._clear_cache_key('_attributes_sync_time', debug=debug)
    return result



def get_bound_interval(self, debug: bool = False) -> Union[timedelta, int, None]:
    """
    Return the interval used to determine the bound time (limit for verification syncs).
    If the datetime axis is an integer, just return its value.

    Below are the supported keys for the bound interval:

        - `pipe.parameters['verify']['bound_minutes']`
        - `pipe.parameters['verify']['bound_hours']`
        - `pipe.parameters['verify']['bound_days']`
        - `pipe.parameters['verify']['bound_weeks']`
        - `pipe.parameters['verify']['bound_years']`
        - `pipe.parameters['verify']['bound_seconds']`

    If multiple keys are present, the first on this priority list will be used.

    Returns
    -------
    A `timedelta` or `int` value to be used to determine the bound time.
    """
    verify_params = self.parameters.get('verify', {})
    prefix = 'bound_'
    suffixes_to_check = ('minutes', 'hours', 'days', 'weeks', 'years', 'seconds')
    keys_to_search = {
        key: val
        for key, val in verify_params.items()
        if key.startswith(prefix)
    }
    bound_time_key, bound_time_value = None, None
    for key, value in keys_to_search.items():
        for suffix in suffixes_to_check:
            if key == prefix + suffix:
                bound_time_key = key
                bound_time_value = value
                break
        if bound_time_key is not None:
            break

    if bound_time_value is None:
        return bound_time_value

    dt_col = self.columns.get('datetime', None)
    if not dt_col:
        return bound_time_value

    dt_typ = self.dtypes.get(dt_col, 'datetime')
    if 'int' in dt_typ.lower():
        return int(bound_time_value)

    interval_type = bound_time_key.replace(prefix, '')
    return timedelta(**{interval_type: bound_time_value})


def get_bound_time(self, debug: bool = False) -> Union[datetime, int, None]:
    """
    The bound time is the limit at which long-running verification syncs should stop.
    A value of `None` means verification syncs should be unbounded.

    Like deriving a backtrack time from `pipe.get_sync_time()`,
    the bound time is the sync time minus a large window (e.g. 366 days).

    Unbound verification syncs (i.e. `bound_time is None`)
    if the oldest sync time is less than the bound interval.

    Returns
    -------
    A `datetime` or `int` corresponding to the
    `begin` bound for verification and deduplication syncs.
    """
    bound_interval = self.get_bound_interval(debug=debug)
    if bound_interval is None:
        return None

    sync_time = self.get_sync_time(debug=debug)
    if sync_time is None:
        return None

    bound_time = sync_time - bound_interval
    oldest_sync_time = self.get_sync_time(newest=False, debug=debug)
    max_bound_time_days = STATIC_CONFIG['pipes']['max_bound_time_days']

    extreme_sync_times_delta = (
        hasattr(oldest_sync_time, 'tzinfo')
        and (sync_time - oldest_sync_time) >= timedelta(days=max_bound_time_days)
    )

    return (
        bound_time
        if bound_time > oldest_sync_time or extreme_sync_times_delta
        else None
    )


def verify_schema(
    self,
    apply: bool = False,
    dry_run: bool = False,
    debug: bool = False,
) -> SuccessTuple:
    """
    Compare the pipe's table with its declared dtypes (`dtypes`, the `columns` roles, geometry
    notations) and, with `apply=True`, bring the table in line.

    Added columns and safe widenings (`int` to `bigint`, `int` to `float`, `float` to `numeric`,
    `varchar(n)` to `text`, `text` to `json`) keep every value; other type changes may fail on
    existing values or change them. Columns are never dropped, and static pipes are included.
    The command line equivalent is `mrsm verify schema`.

    Parameters
    ----------
    apply: bool, default False
        If `True`, apply every change the plan lists. Otherwise nothing is written.

    dry_run: bool, default False
        If `True`, return the SQL the plan would run (and write nothing).

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple`: the message is the plan (a table of the columns that change), the SQL
    with `dry_run=True`, or the plan and what changed with `apply=True`. A protected instance
    refuses `apply=True` unless writes are allowed (`meerschaum.connectors.allow_protected()`).

    Examples
    --------
    >>> pipe = mrsm.Pipe('plugin:noaa', 'weather', dtypes={'temperature': 'float'})
    >>> success, plan = pipe.verify_schema()
    >>> print(plan)
    >>> success, msg = pipe.verify_schema(apply=True)
    """
    from meerschaum import _core
    from meerschaum.core.Pipe._data import _rust_context
    context = _rust_context(self, debug=debug)
    success, message = _core.pipe_verify_schema(
        self,
        context,
        apply=(apply and not dry_run),
        dry_run=dry_run,
        debug=debug,
    )
    if apply and not dry_run:
        ### The table's columns changed: forget the cached columns types.
        self._clear_cache_key('_columns_types', debug=debug)
        self._clear_cache_key('_columns_types_timestamp', debug=debug)
    return success, message
