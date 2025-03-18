#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize a pipe's data with its source via its connector.
"""

from __future__ import annotations

import json
import time
import threading
import multiprocessing
import functools
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import meerschaum as mrsm
from meerschaum.utils.typing import (
    Union,
    Optional,
    Callable,
    Any,
    Tuple,
    SuccessTuple,
    Dict,
    List,
)
from meerschaum.utils.warnings import warn, error

if TYPE_CHECKING:
    pd = mrsm.attempt_import('pandas')

class InferFetch:
    MRSM_INFER_FETCH: bool = True


def sync(
    self,
    df: Union[
        pd.DataFrame,
        Dict[str, List[Any]],
        List[Dict[str, Any]],
        InferFetch
    ] = InferFetch,
    begin: Union[datetime, int, str, None] = '',
    end: Union[datetime, int, None] = None,
    force: bool = False,
    retries: int = 10,
    min_seconds: int = 1,
    check_existing: bool = True,
    enforce_dtypes: bool = True,
    blocking: bool = True,
    workers: Optional[int] = None,
    callback: Optional[Callable[[Tuple[bool, str]], Any]] = None,
    error_callback: Optional[Callable[[Exception], Any]] = None,
    chunksize: Optional[int] = -1,
    sync_chunks: bool = True,
    debug: bool = False,
    _inplace: bool = True,
    **kw: Any
) -> SuccessTuple:
    """
    Fetch new data from the source and update the pipe's table with new data.
    
    Get new remote data via fetch, get existing data in the same time period,
    and merge the two, only keeping the unseen data.

    Parameters
    ----------
    df: Union[None, pd.DataFrame, Dict[str, List[Any]]], default None
        An optional DataFrame to sync into the pipe. Defaults to `None`.

    begin: Union[datetime, int, str, None], default ''
        Optionally specify the earliest datetime to search for data.

    end: Union[datetime, int, str, None], default None
        Optionally specify the latest datetime to search for data.

    force: bool, default False
        If `True`, keep trying to sync untul `retries` attempts.

    retries: int, default 10
        If `force`, how many attempts to try syncing before declaring failure.

    min_seconds: Union[int, float], default 1
        If `force`, how many seconds to sleep between retries. Defaults to `1`.

    check_existing: bool, default True
        If `True`, pull and diff with existing data from the pipe.

    enforce_dtypes: bool, default True
        If `True`, enforce dtypes on incoming data.
        Set this to `False` if the incoming rows are expected to be of the correct dtypes.

    blocking: bool, default True
        If `True`, wait for sync to finish and return its result, otherwise
        asyncronously sync (oxymoron?) and return success. Defaults to `True`.
        Only intended for specific scenarios.

    workers: Optional[int], default None
        If provided and the instance connector is thread-safe
        (`pipe.instance_connector.IS_THREAD_SAFE is True`),
        limit concurrent sync to this many threads.

    callback: Optional[Callable[[Tuple[bool, str]], Any]], default None
        Callback function which expects a SuccessTuple as input.
        Only applies when `blocking=False`.

    error_callback: Optional[Callable[[Exception], Any]], default None
        Callback function which expects an Exception as input.
        Only applies when `blocking=False`.

    chunksize: int, default -1
        Specify the number of rows to sync per chunk.
        If `-1`, resort to system configuration (default is `900`).
        A `chunksize` of `None` will sync all rows in one transaction.

    sync_chunks: bool, default True
        If possible, sync chunks while fetching them into memory.

    debug: bool, default False
        Verbosity toggle. Defaults to False.

    Returns
    -------
    A `SuccessTuple` of success (`bool`) and message (`str`).
    """
    from meerschaum.utils.debug import dprint, _checkpoint
    from meerschaum.utils.formatting import get_console
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.misc import df_is_chunk_generator, filter_keywords, filter_arguments
    from meerschaum.utils.pool import get_pool
    from meerschaum.config import get_config

    if (callback is not None or error_callback is not None) and blocking:
        warn("Callback functions are only executed when blocking = False. Ignoring...")

    _checkpoint(_total=2, **kw)

    if chunksize == 0:
        chunksize = None
        sync_chunks = False

    begin, end = self.parse_date_bounds(begin, end)
    kw.update({
        'begin': begin,
        'end': end,
        'force': force,
        'retries': retries,
        'min_seconds': min_seconds,
        'check_existing': check_existing,
        'blocking': blocking,
        'workers': workers,
        'callback': callback,
        'error_callback': error_callback,
        'sync_chunks': sync_chunks,
        'chunksize': chunksize,
        'safe_copy': True,
    })

    ### NOTE: Invalidate `_exists` cache before and after syncing.
    self._exists = None

    def _sync(
        p: mrsm.Pipe,
        df: Union[
            'pd.DataFrame',
            Dict[str, List[Any]],
            List[Dict[str, Any]],
            InferFetch
        ] = InferFetch,
    ) -> SuccessTuple:
        if df is None:
            p._exists = None
            return (
                False,
                f"You passed `None` instead of data into `sync()` for {p}.\n"
                + "Omit the DataFrame to infer fetching.",
            )
        ### Ensure that Pipe is registered.
        if not p.temporary and p.get_id(debug=debug) is None:
            ### NOTE: This may trigger an interactive session for plugins!
            register_success, register_msg = p.register(debug=debug)
            if not register_success:
                if 'already' not in register_msg:
                    p._exists = None
                    return register_success, register_msg

        ### If connector is a plugin with a `sync()` method, return that instead.
        ### If the plugin does not have a `sync()` method but does have a `fetch()` method,
        ### use that instead.
        ### NOTE: The DataFrame must be omitted for the plugin sync method to apply.
        ### If a DataFrame is provided, continue as expected.
        if hasattr(df, 'MRSM_INFER_FETCH'):
            try:
                if p.connector is None:
                    if ':' not in p.connector_keys:
                        return True, f"{p} does not support fetching; nothing to do."

                    msg = f"{p} does not have a valid connector."
                    if p.connector_keys.startswith('plugin:'):
                        msg += f"\n    Perhaps {p.connector_keys} has a syntax error?"
                    p._exists = None
                    return False, msg
            except Exception:
                p._exists = None
                return False, f"Unable to create the connector for {p}."

            ### Sync in place if this is a SQL pipe.
            if (
                str(self.connector) == str(self.instance_connector)
                and 
                hasattr(self.instance_connector, 'sync_pipe_inplace')
                and
                _inplace
                and
                get_config('system', 'experimental', 'inplace_sync')
            ):
                with Venv(get_connector_plugin(self.instance_connector)):
                    p._exists = None
                    _args, _kwargs = filter_arguments(
                        p.instance_connector.sync_pipe_inplace,
                        p,
                        debug=debug,
                        **kw
                    )
                    return self.instance_connector.sync_pipe_inplace(
                        *_args,
                        **_kwargs
                    )

            ### Activate and invoke `sync(pipe)` for plugin connectors with `sync` methods.
            try:
                if getattr(p.connector, 'sync', None) is not None:
                    with Venv(get_connector_plugin(p.connector), debug=debug):
                        _args, _kwargs = filter_arguments(
                            p.connector.sync,
                            p,
                            debug=debug,
                            **kw
                        )
                        return_tuple = p.connector.sync(*_args, **_kwargs)
                    p._exists = None
                    if not isinstance(return_tuple, tuple):
                        return_tuple = (
                            False,
                            f"Plugin '{p.connector.label}' returned non-tuple value: {return_tuple}"
                        )
                    return return_tuple

            except Exception as e:
                get_console().print_exception()
                msg = f"Failed to sync {p} with exception: '" + str(e) + "'"
                if debug:
                    error(msg, silent=False)
                p._exists = None
                return False, msg

            ### Fetch the dataframe from the connector's `fetch()` method.
            try:
                with Venv(get_connector_plugin(p.connector), debug=debug):
                    df = p.fetch(
                        **filter_keywords(
                            p.fetch,
                            debug=debug,
                            **kw
                        )
                    )
                    kw['safe_copy'] = False
            except Exception as e:
                get_console().print_exception(
                    suppress=[
                        'meerschaum/core/Pipe/_sync.py',
                        'meerschaum/core/Pipe/_fetch.py',
                    ]
                )
                msg = f"Failed to fetch data from {p.connector}:\n    {e}"
                df = None

            if df is None:
                p._exists = None
                return False, f"No data were fetched for {p}."

            if isinstance(df, list):
                if len(df) == 0:
                    return True, f"No new rows were returned for {p}."

                ### May be a chunk hook results list.
                if isinstance(df[0], tuple):
                    success = all([_success for _success, _ in df])
                    message = '\n'.join([_message for _, _message in df])
                    return success, message

            if df is True:
                p._exists = None
                return True, f"{p} is being synced in parallel."

        ### CHECKPOINT: Retrieved the DataFrame.
        _checkpoint(**kw)

        ### Allow for dataframe generators or iterables.
        if df_is_chunk_generator(df):
            kw['workers'] = p.get_num_workers(kw.get('workers', None))
            dt_col = p.columns.get('datetime', None)
            pool = get_pool(workers=kw.get('workers', 1))
            if debug:
                dprint(f"Received {type(df)}. Attempting to sync first chunk...")

            try:
                chunk = next(df)
            except StopIteration:
                return True, "Received an empty generator; nothing to do."

            chunk_success, chunk_msg = _sync(p, chunk)
            chunk_msg = '\n' + self._get_chunk_label(chunk, dt_col) + '\n' + chunk_msg
            if not chunk_success:
                return chunk_success, f"Unable to sync initial chunk for {p}:\n{chunk_msg}"
            if debug:
                dprint("Successfully synced the first chunk, attemping the rest...")

            def _process_chunk(_chunk):
                _chunk_attempts = 0
                _max_chunk_attempts = 3
                while _chunk_attempts < _max_chunk_attempts:
                    try:
                        _chunk_success, _chunk_msg = _sync(p, _chunk)
                    except Exception as e:
                        _chunk_success, _chunk_msg = False, str(e)
                    if _chunk_success:
                        break
                    _chunk_attempts += 1
                    _sleep_seconds = _chunk_attempts ** 2
                    warn(
                        (
                            f"Failed to sync chunk to {self} "
                            + f"(attempt {_chunk_attempts} / {_max_chunk_attempts}).\n"
                            + f"Sleeping for {_sleep_seconds} second"
                            + ('s' if _sleep_seconds != 1 else '')
                            + ":\n{_chunk_msg}"
                        ),
                        stack=False,
                    )
                    time.sleep(_sleep_seconds)

                num_rows_str = (
                    f"{num_rows:,} rows"
                    if (num_rows := len(_chunk)) != 1
                    else f"{num_rows} row"
                )
                _chunk_msg = (
                    (
                        "Synced"
                        if _chunk_success
                        else "Failed to sync"
                    ) + f" a chunk ({num_rows_str}) to {p}:\n"
                    + self._get_chunk_label(_chunk, dt_col)
                    + '\n'
                    + _chunk_msg
                )

                mrsm.pprint((_chunk_success, _chunk_msg), calm=True)
                return _chunk_success, _chunk_msg

            results = sorted(
                [(chunk_success, chunk_msg)] + (
                    list(pool.imap(_process_chunk, df))
                    if (
                        not df_is_chunk_generator(chunk)  # Handle nested generators.
                        and kw.get('workers', 1) != 1
                    )
                    else list(
                        _process_chunk(_child_chunks)
                        for _child_chunks in df
                    )
                )
            )
            chunk_messages = [chunk_msg for _, chunk_msg in results]
            success_bools = [chunk_success for chunk_success, _ in results]
            num_successes = len([chunk_success for chunk_success, _ in results if chunk_success])
            num_failures = len([chunk_success for chunk_success, _ in results if not chunk_success])
            success = all(success_bools)
            msg = (
                'Synced '
                + f'{len(chunk_messages):,} chunk'
                + ('s' if len(chunk_messages) != 1 else '')
                + f' to {p}\n({num_successes} succeeded, {num_failures} failed):\n\n'
                + '\n\n'.join(chunk_messages).lstrip().rstrip()
            ).lstrip().rstrip()
            return success, msg

        ### Cast to a dataframe and ensure datatypes are what we expect.
        df = self.enforce_dtypes(
            df,
            chunksize=chunksize,
            enforce=enforce_dtypes,
            debug=debug,
        )

        ### Capture `numeric`, `uuid`, `json`, and `bytes` columns.
        self._persist_new_json_columns(df, debug=debug)
        self._persist_new_numeric_columns(df, debug=debug)
        self._persist_new_uuid_columns(df, debug=debug)
        self._persist_new_bytes_columns(df, debug=debug)
        self._persist_new_geometry_columns(df, debug=debug)

        if debug:
            dprint(
                "DataFrame to sync:\n"
                + (
                    str(df)[:255]
                    + '...'
                    if len(str(df)) >= 256
                    else str(df)
                ),
                **kw
            )

        ### if force, continue to sync until success
        return_tuple = False, f"Did not sync {p}."
        run = True
        _retries = 1
        while run:
            with Venv(get_connector_plugin(self.instance_connector)):
                return_tuple = p.instance_connector.sync_pipe(
                    pipe=p,
                    df=df,
                    debug=debug,
                    **kw
                )
            _retries += 1
            run = (not return_tuple[0]) and force and _retries <= retries
            if run and debug:
                dprint(f"Syncing failed for {p}. Attempt ( {_retries} / {retries} )", **kw)
                dprint(f"Sleeping for {min_seconds} seconds...", **kw)
                time.sleep(min_seconds)
            if _retries > retries:
                warn(
                    f"Unable to sync {p} within {retries} attempt" +
                        ("s" if retries != 1 else "") + "!"
                )

        ### CHECKPOINT: Finished syncing. Handle caching.
        _checkpoint(**kw)
        if self.cache_pipe is not None:
            if debug:
                dprint("Caching retrieved dataframe.", **kw)
                _sync_cache_tuple = self.cache_pipe.sync(df, debug=debug, **kw)
                if not _sync_cache_tuple[0]:
                    warn(f"Failed to sync local cache for {self}.")

        self._exists = None
        return return_tuple

    if blocking:
        self._exists = None
        return _sync(self, df=df)

    from meerschaum.utils.threading import Thread
    def default_callback(result_tuple: SuccessTuple):
        dprint(f"Asynchronous result from {self}: {result_tuple}", **kw)

    def default_error_callback(x: Exception):
        dprint(f"Error received for {self}: {x}", **kw)

    if callback is None and debug:
        callback = default_callback
    if error_callback is None and debug:
        error_callback = default_error_callback
    try:
        thread = Thread(
            target=_sync,
            args=(self,),
            kwargs={'df': df},
            daemon=False,
            callback=callback,
            error_callback=error_callback,
        )
        thread.start()
    except Exception as e:
        self._exists = None
        return False, str(e)

    self._exists = None
    return True, f"Spawned asyncronous sync for {self}."


def get_sync_time(
    self,
    params: Optional[Dict[str, Any]] = None,
    newest: bool = True,
    apply_backtrack_interval: bool = False,
    remote: bool = False,
    round_down: bool = False,
    debug: bool = False
) -> Union['datetime', int, None]:
    """
    Get the most recent datetime value for a Pipe.

    Parameters
    ----------
    params: Optional[Dict[str, Any]], default None
        Dictionary to build a WHERE clause for a specific column.
        See `meerschaum.utils.sql.build_where`.

    newest: bool, default True
        If `True`, get the most recent datetime (honoring `params`).
        If `False`, get the oldest datetime (`ASC` instead of `DESC`).

    apply_backtrack_interval: bool, default False
        If `True`, subtract the backtrack interval from the sync time.

    remote: bool, default False
        If `True` and the instance connector supports it, return the sync time
        for the remote table definition.

    round_down: bool, default False
        If `True`, round down the datetime value to the nearest minute.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `datetime` or int, if the pipe exists, otherwise `None`.

    """
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.misc import round_time, filter_keywords
    from meerschaum.utils.warnings import warn

    if not self.columns.get('datetime', None):
        return None

    connector = self.instance_connector if not remote else self.connector
    with Venv(get_connector_plugin(connector)):
        if not hasattr(connector, 'get_sync_time'):
            warn(
                f"Connectors of type '{connector.type}' "
                "do not implement `get_sync_time().",
                stack=False,
            )
            return None
        sync_time = connector.get_sync_time(
            self,
            **filter_keywords(
                connector.get_sync_time,
                params=params,
                newest=newest,
                remote=remote,
                debug=debug,
            )
        )

    if round_down and isinstance(sync_time, datetime):
        sync_time = round_time(sync_time, timedelta(minutes=1))

    if apply_backtrack_interval and sync_time is not None:
        backtrack_interval = self.get_backtrack_interval(debug=debug)
        try:
            sync_time -= backtrack_interval
        except Exception as e:
            warn(f"Failed to apply backtrack interval:\n{e}")

    return self.parse_date_bounds(sync_time)


def exists(
    self,
    debug: bool = False
) -> bool:
    """
    See if a Pipe's table exists.

    Parameters
    ----------
    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `bool` corresponding to whether a pipe's underlying table exists.

    """
    import time
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.config import STATIC_CONFIG
    from meerschaum.utils.debug import dprint
    now = time.perf_counter()
    exists_timeout_seconds = STATIC_CONFIG['pipes']['exists_timeout_seconds']

    _exists = self.__dict__.get('_exists', None)
    if _exists:
        exists_timestamp = self.__dict__.get('_exists_timestamp', None)
        if exists_timestamp is not None:
            delta = now - exists_timestamp
            if delta < exists_timeout_seconds:
                if debug:
                    dprint(f"Returning cached `exists` for {self} ({round(delta, 2)} seconds old).")
                return _exists

    with Venv(get_connector_plugin(self.instance_connector)):
        _exists = (
            self.instance_connector.pipe_exists(pipe=self, debug=debug)
            if hasattr(self.instance_connector, 'pipe_exists')
            else False
        )

    self.__dict__['_exists'] = _exists
    self.__dict__['_exists_timestamp'] = now
    return _exists


def filter_existing(
    self,
    df: 'pd.DataFrame',
    safe_copy: bool = True,
    date_bound_only: bool = False,
    include_unchanged_columns: bool = False,
    enforce_dtypes: bool = False,
    chunksize: Optional[int] = -1,
    debug: bool = False,
    **kw
) -> Tuple['pd.DataFrame', 'pd.DataFrame', 'pd.DataFrame']:
    """
    Inspect a dataframe and filter out rows which already exist in the pipe.

    Parameters
    ----------
    df: 'pd.DataFrame'
        The dataframe to inspect and filter.

    safe_copy: bool, default True
        If `True`, create a copy before comparing and modifying the dataframes.
        Setting to `False` may mutate the DataFrames.
        See `meerschaum.utils.dataframe.filter_unseen_df`.

    date_bound_only: bool, default False
        If `True`, only use the datetime index to fetch the sample dataframe.

    include_unchanged_columns: bool, default False
        If `True`, include the backtrack columns which haven't changed in the update dataframe.
        This is useful if you can't update individual keys.

    enforce_dtypes: bool, default False
        If `True`, ensure the given and intermediate dataframes are enforced to the correct dtypes.
        Setting `enforce_dtypes=True` may impact performance.

    chunksize: Optional[int], default -1
        The `chunksize` used when fetching existing data.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A tuple of three pandas DataFrames: unseen, update, and delta.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import, import_pandas
    from meerschaum.utils.misc import round_time
    from meerschaum.utils.dataframe import (
        filter_unseen_df,
        add_missing_cols_to_df,
        get_unhashable_cols,
    )
    from meerschaum.utils.dtypes import (
        to_pandas_dtype,
        none_if_null,
        to_datetime,
        are_dtypes_equal,
        value_is_null,
    )
    from meerschaum.config import get_config
    pd = import_pandas()
    pandas = attempt_import('pandas')
    if enforce_dtypes or 'dataframe' not in str(type(df)).lower():
        df = self.enforce_dtypes(df, chunksize=chunksize, debug=debug)
    is_dask = hasattr('df', '__module__') and 'dask' in df.__module__
    if is_dask:
        dd = attempt_import('dask.dataframe')
        merge = dd.merge
        NA = pandas.NA
    else:
        merge = pd.merge
        NA = pd.NA

    primary_key = self.columns.get('primary', None)
    autoincrement = self.parameters.get('autoincrement', False)
    pipe_columns = self.columns.copy()

    if primary_key and autoincrement and df is not None and primary_key in df.columns:
        if safe_copy:
            df = df.copy()
            safe_copy = False
        if df[primary_key].isnull().all():
            del df[primary_key]
            _ = self.columns.pop(primary_key, None)

    def get_empty_df():
        empty_df = pd.DataFrame([])
        dtypes = dict(df.dtypes) if df is not None else {}
        dtypes.update(self.dtypes)
        pd_dtypes = {
            col: to_pandas_dtype(str(typ))
            for col, typ in dtypes.items()
        }
        return add_missing_cols_to_df(empty_df, pd_dtypes)

    if df is None:
        empty_df = get_empty_df()
        return empty_df, empty_df, empty_df

    if (df.empty if not is_dask else len(df) == 0):
        return df, df, df

    ### begin is the oldest data in the new dataframe
    begin, end = None, None
    dt_col = pipe_columns.get('datetime', None)
    primary_key = pipe_columns.get('primary', None)
    dt_type = self.dtypes.get(dt_col, 'datetime64[ns, UTC]') if dt_col else None

    if autoincrement and primary_key == dt_col and dt_col not in df.columns:
        if enforce_dtypes:
            df = self.enforce_dtypes(df, chunksize=chunksize, debug=debug)
        return df, get_empty_df(), df

    try:
        min_dt_val = df[dt_col].min(skipna=True) if dt_col and dt_col in df.columns else None
        if is_dask and min_dt_val is not None:
            min_dt_val = min_dt_val.compute()
        min_dt = (
            to_datetime(min_dt_val, as_pydatetime=True)
            if min_dt_val is not None and are_dtypes_equal(dt_type, 'datetime')
            else min_dt_val
        )
    except Exception:
        min_dt = None

    if not are_dtypes_equal('datetime', str(type(min_dt))) or value_is_null(min_dt):
        if not are_dtypes_equal('int', str(type(min_dt))):
            min_dt = None

    if isinstance(min_dt, datetime):
        rounded_min_dt = round_time(min_dt, to='down')
        try:
            begin = rounded_min_dt - timedelta(minutes=1)
        except OverflowError:
            begin = rounded_min_dt
    elif dt_type and 'int' in dt_type.lower():
        begin = min_dt
    elif dt_col is None:
        begin = None

    ### end is the newest data in the new dataframe
    try:
        max_dt_val = df[dt_col].max(skipna=True) if dt_col and dt_col in df.columns else None
        if is_dask and max_dt_val is not None:
            max_dt_val = max_dt_val.compute()
        max_dt = (
            to_datetime(max_dt_val, as_pydatetime=True)
            if max_dt_val is not None and 'datetime' in str(dt_type)
            else max_dt_val
        )
    except Exception:
        import traceback
        traceback.print_exc()
        max_dt = None

    if not are_dtypes_equal('datetime', str(type(max_dt))) or value_is_null(max_dt):
        if not are_dtypes_equal('int', str(type(max_dt))):
            max_dt = None

    if isinstance(max_dt, datetime):
        end = (
            round_time(
                max_dt,
                to='down'
            ) + timedelta(minutes=1)
        )
    elif dt_type and 'int' in dt_type.lower() and max_dt is not None:
        end = max_dt + 1

    if max_dt is not None and min_dt is not None and min_dt > max_dt:
        warn("Detected minimum datetime greater than maximum datetime.")

    if begin is not None and end is not None and begin > end:
        if isinstance(begin, datetime):
            begin = end - timedelta(minutes=1)
        ### We might be using integers for the datetime axis.
        else:
            begin = end - 1

    unique_index_vals = {
        col: df[col].unique()
        for col in (pipe_columns if not primary_key else [primary_key])
        if col in df.columns and col != dt_col
    } if not date_bound_only else {}
    filter_params_index_limit = get_config('pipes', 'sync', 'filter_params_index_limit')
    _ = kw.pop('params', None)
    params = {
        col: [
            none_if_null(val)
            for val in unique_vals
        ]
        for col, unique_vals in unique_index_vals.items()
        if len(unique_vals) <= filter_params_index_limit
    } if not date_bound_only else {}

    if debug:
        dprint(f"Looking at data between '{begin}' and '{end}':", **kw)

    backtrack_df = self.get_data(
        begin=begin,
        end=end,
        chunksize=chunksize,
        params=params,
        debug=debug,
        **kw
    )
    if backtrack_df is None:
        if debug:
            dprint(f"No backtrack data was found for {self}.")
        return df, get_empty_df(), df

    if enforce_dtypes:
        backtrack_df = self.enforce_dtypes(backtrack_df, chunksize=chunksize, debug=debug)

    if debug:
        dprint(f"Existing data for {self}:\n" + str(backtrack_df), **kw)
        dprint(f"Existing dtypes for {self}:\n" + str(backtrack_df.dtypes))

    ### Separate new rows from changed ones.
    on_cols = [
        col
        for col_key, col in pipe_columns.items()
        if (
            col
            and
            col_key != 'value'
            and col in backtrack_df.columns
        )
    ] if not primary_key else [primary_key]
    self_dtypes = self.dtypes
    on_cols_dtypes = {
        col: to_pandas_dtype(typ)
        for col, typ in self_dtypes.items()
        if col in on_cols
    }

    ### Detect changes between the old target and new source dataframes.
    delta_df = add_missing_cols_to_df(
        filter_unseen_df(
            backtrack_df,
            df,
            dtypes={
                col: to_pandas_dtype(typ)
                for col, typ in self_dtypes.items()
            },
            safe_copy=safe_copy,
            coerce_mixed_numerics=(not self.static),
            debug=debug
        ),
        on_cols_dtypes,
    )
    if enforce_dtypes:
        delta_df = self.enforce_dtypes(delta_df, chunksize=chunksize, debug=debug)

    ### Cast dicts or lists to strings so we can merge.
    serializer = functools.partial(json.dumps, sort_keys=True, separators=(',', ':'), default=str)

    def deserializer(x):
        return json.loads(x) if isinstance(x, str) else x

    unhashable_delta_cols = get_unhashable_cols(delta_df)
    unhashable_backtrack_cols = get_unhashable_cols(backtrack_df)
    for col in unhashable_delta_cols:
        delta_df[col] = delta_df[col].apply(serializer)
    for col in unhashable_backtrack_cols:
        backtrack_df[col] = backtrack_df[col].apply(serializer)
    casted_cols = set(unhashable_delta_cols + unhashable_backtrack_cols)

    joined_df = merge(
        delta_df.infer_objects(copy=False).fillna(NA),
        backtrack_df.infer_objects(copy=False).fillna(NA),
        how='left',
        on=on_cols,
        indicator=True,
        suffixes=('', '_old'),
    ) if on_cols else delta_df
    for col in casted_cols:
        if col in joined_df.columns:
            joined_df[col] = joined_df[col].apply(deserializer)
        if col in delta_df.columns:
            delta_df[col] = delta_df[col].apply(deserializer)

    ### Determine which rows are completely new.
    new_rows_mask = (joined_df['_merge'] == 'left_only') if on_cols else None
    cols = list(delta_df.columns)

    unseen_df = (
        joined_df
        .where(new_rows_mask)
        .dropna(how='all')[cols]
        .reset_index(drop=True)
    ) if on_cols else delta_df

    ### Rows that have already been inserted but values have changed.
    update_df = (
        joined_df
        .where(~new_rows_mask)
        .dropna(how='all')[cols]
        .reset_index(drop=True)
    ) if on_cols else get_empty_df()

    if include_unchanged_columns and on_cols:
        unchanged_backtrack_cols = [
            col
            for col in backtrack_df.columns
            if col in on_cols or col not in update_df.columns
        ]
        if enforce_dtypes:
            update_df = self.enforce_dtypes(update_df, chunksize=chunksize, debug=debug)
        update_df = merge(
            backtrack_df[unchanged_backtrack_cols],
            update_df,
            how='inner',
            on=on_cols,
        )

    return unseen_df, update_df, delta_df


@staticmethod
def _get_chunk_label(
    chunk: Union[
        'pd.DataFrame',
        List[Dict[str, Any]],
        Dict[str, List[Any]]
    ],
    dt_col: str,
) -> str:
    """
    Return the min - max label for the chunk.
    """
    from meerschaum.utils.dataframe import get_datetime_bound_from_df
    min_dt = get_datetime_bound_from_df(chunk, dt_col)
    max_dt = get_datetime_bound_from_df(chunk, dt_col, minimum=False)
    return (
        f"{min_dt} - {max_dt}"
        if min_dt is not None and max_dt is not None
        else ''
    )


def get_num_workers(self, workers: Optional[int] = None) -> int:
    """
    Get the number of workers to use for concurrent syncs.

    Parameters
    ----------
    The number of workers passed via `--workers`.

    Returns
    -------
    The number of workers, capped for safety.
    """
    is_thread_safe = getattr(self.instance_connector, 'IS_THREAD_SAFE', False)
    if not is_thread_safe:
        return 1

    engine_pool_size = (
        self.instance_connector.engine.pool.size()
        if self.instance_connector.type == 'sql'
        else None
    )
    current_num_threads = threading.active_count()
    current_num_connections = (
        self.instance_connector.engine.pool.checkedout()
        if engine_pool_size is not None
        else current_num_threads
    )
    desired_workers = (
        min(workers or engine_pool_size, engine_pool_size)
        if engine_pool_size is not None
        else workers
    )
    if desired_workers is None:
        desired_workers = (multiprocessing.cpu_count() if is_thread_safe else 1)

    return max(
        (desired_workers - current_num_connections),
        1,
    )


def _persist_new_numeric_columns(self, df, debug: bool = False) -> SuccessTuple:
    """
    Check for new numeric columns and update the parameters.
    """
    from meerschaum.utils.dataframe import get_numeric_cols
    numeric_cols = get_numeric_cols(df)
    existing_numeric_cols = [col for col, typ in self.dtypes.items() if typ.startswith('numeric')]
    new_numeric_cols = [col for col in numeric_cols if col not in existing_numeric_cols]
    if not new_numeric_cols:
        return True, "Success"

    self._attributes_sync_time = None
    dtypes = self.parameters.get('dtypes', {})
    dtypes.update({col: 'numeric' for col in new_numeric_cols})
    self.parameters['dtypes'] = dtypes
    if not self.temporary:
        edit_success, edit_msg = self.edit(interactive=False, debug=debug)
        if not edit_success:
            warn(f"Unable to update NUMERIC dtypes for {self}:\n{edit_msg}")

        return edit_success, edit_msg

    return True, "Success"


def _persist_new_uuid_columns(self, df, debug: bool = False) -> SuccessTuple:
    """
    Check for new numeric columns and update the parameters.
    """
    from meerschaum.utils.dataframe import get_uuid_cols
    uuid_cols = get_uuid_cols(df)
    existing_uuid_cols = [col for col, typ in self.dtypes.items() if typ == 'uuid']
    new_uuid_cols = [col for col in uuid_cols if col not in existing_uuid_cols]
    if not new_uuid_cols:
        return True, "Success"

    self._attributes_sync_time = None
    dtypes = self.parameters.get('dtypes', {})
    dtypes.update({col: 'uuid' for col in new_uuid_cols})
    self.parameters['dtypes'] = dtypes
    if not self.temporary:
        edit_success, edit_msg = self.edit(interactive=False, debug=debug)
        if not edit_success:
            warn(f"Unable to update UUID dtypes for {self}:\n{edit_msg}")

        return edit_success, edit_msg

    return True, "Success"


def _persist_new_json_columns(self, df, debug: bool = False) -> SuccessTuple:
    """
    Check for new JSON columns and update the parameters.
    """
    from meerschaum.utils.dataframe import get_json_cols
    json_cols = get_json_cols(df)
    existing_json_cols = [col for col, typ in self.dtypes.items() if typ == 'json']
    new_json_cols = [col for col in json_cols if col not in existing_json_cols]
    if not new_json_cols:
        return True, "Success"

    self._attributes_sync_time = None
    dtypes = self.parameters.get('dtypes', {})
    dtypes.update({col: 'json' for col in new_json_cols})
    self.parameters['dtypes'] = dtypes

    if not self.temporary:
        edit_success, edit_msg = self.edit(interactive=False, debug=debug)
        if not edit_success:
            warn(f"Unable to update JSON dtypes for {self}:\n{edit_msg}")

        return edit_success, edit_msg

    return True, "Success"


def _persist_new_bytes_columns(self, df, debug: bool = False) -> SuccessTuple:
    """
    Check for new `bytes` columns and update the parameters.
    """
    from meerschaum.utils.dataframe import get_bytes_cols
    bytes_cols = get_bytes_cols(df)
    existing_bytes_cols = [col for col, typ in self.dtypes.items() if typ == 'bytes']
    new_bytes_cols = [col for col in bytes_cols if col not in existing_bytes_cols]
    if not new_bytes_cols:
        return True, "Success"

    self._attributes_sync_time = None
    dtypes = self.parameters.get('dtypes', {})
    dtypes.update({col: 'bytes' for col in new_bytes_cols})
    self.parameters['dtypes'] = dtypes

    if not self.temporary:
        edit_success, edit_msg = self.edit(interactive=False, debug=debug)
        if not edit_success:
            warn(f"Unable to update bytes dtypes for {self}:\n{edit_msg}")

        return edit_success, edit_msg

    return True, "Success"


def _persist_new_geometry_columns(self, df, debug: bool = False) -> SuccessTuple:
    """
    Check for new `geometry` columns and update the parameters.
    """
    from meerschaum.utils.dataframe import get_geometry_cols
    geometry_cols_types_srids = get_geometry_cols(df, with_types_srids=True)
    existing_geometry_cols = [
        col
        for col, typ in self.dtypes.items()
        if typ.startswith('geometry') or typ.startswith('geography')
    ]
    new_geometry_cols = [
        col
        for col in geometry_cols_types_srids
        if col not in existing_geometry_cols
    ]
    if not new_geometry_cols:
        return True, "Success"

    self._attributes_sync_time = None
    dtypes = self.parameters.get('dtypes', {})

    new_cols_types = {}
    for col, (geometry_type, srid) in geometry_cols_types_srids.items():
        if col not in new_geometry_cols:
            continue

        new_dtype = "geometry"
        modifier = ""
        if not srid and geometry_type.lower() == 'geometry':
            new_cols_types[col] = new_dtype
            continue

        modifier = "["
        if geometry_type.lower() != 'geometry':
            modifier += f"{geometry_type}"

        if srid:
            if modifier != '[':
                modifier += ", "
            modifier += f"{srid}"
        modifier += "]"
        new_cols_types[col] = f"{new_dtype}{modifier}"

    dtypes.update(new_cols_types)
    self.parameters['dtypes'] = dtypes

    if not self.temporary:
        edit_success, edit_msg = self.edit(interactive=False, debug=debug)
        if not edit_success:
            warn(f"Unable to update bytes dtypes for {self}:\n{edit_msg}")

        return edit_success, edit_msg

    return True, "Success"
