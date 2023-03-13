#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize a pipe's data with its source via its connector
"""

from __future__ import annotations

import json
from meerschaum.utils.typing import (
    Union, Optional, Callable, Any, Tuple, SuccessTuple, Mapping, Dict, List
)

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
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        force: bool = False,
        retries: int = 10,
        min_seconds: int = 1,
        check_existing: bool = True,
        blocking: bool = True,
        workers: Optional[int] = None,
        callback: Optional[Callable[[Tuple[bool, str]], Any]] = None,
        error_callback: Optional[Callable[[Exception], Any]] = None,
        chunksize: Optional[int] = -1,
        sync_chunks: bool = False,
        debug: bool = False,
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

    begin: Optional[datetime.datetime], default None
        Optionally specify the earliest datetime to search for data.
        Defaults to `None`.

    end: Optional[datetime.datetime], default None
        Optionally specify the latest datetime to search for data.
        Defaults to `None`.

    force: bool, default False
        If `True`, keep trying to sync untul `retries` attempts.
        Defaults to `False`.

    retries: int, default 10
        If `force`, how many attempts to try syncing before declaring failure.
        Defaults to `10`.

    min_seconds: Union[int, float], default 1
        If `force`, how many seconds to sleep between retries. Defaults to `1`.

    check_existing: bool, default True
        If `True`, pull and diff with existing data from the pipe.
        Defaults to `True`.

    blocking: bool, default True
        If `True`, wait for sync to finish and return its result, otherwise
        asyncronously sync (oxymoron?) and return success. Defaults to `True`.
        Only intended for specific scenarios.

    workers: Optional[int], default None
        No use directly within `Pipe.sync()`. Instead is passed on to
        instance connectors' `sync_pipe()` methods
        (e.g. `meerschaum.connectors.plugin.PluginConnector`).
        Defaults to `None`.

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
        Defaults to `-1`.

    sync_chunks: bool, default False
        If possible, sync chunks while fetching them into memory.
        Defaults to `False`.

    debug: bool, default False
        Verbosity toggle. Defaults to False.

    Returns
    -------
    A `SuccessTuple` of success (`bool`) and message (`str`).

    """
    from meerschaum.utils.debug import dprint, _checkpoint
    from meerschaum.utils.warnings import warn, error
    from meerschaum.connectors import custom_types
    from meerschaum.plugins import Plugin
    from meerschaum.utils.formatting import get_console
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    from meerschaum.config import get_config
    import datetime
    import time
    if (callback is not None or error_callback is not None) and blocking:
        warn("Callback functions are only executed when blocking = False. Ignoring...")

    _checkpoint(_total=2, **kw)

    ### NOTE: Setting begin to the sync time for Simple Sync.
    ### TODO: Add flag for specifying syncing method.
    begin = _determine_begin(self, begin, debug=debug)
    kw.update({
        'begin': begin, 'end': end, 'force': force, 'retries': retries, 'min_seconds': min_seconds,
        'check_existing': check_existing, 'blocking': blocking, 'workers': workers,
        'callback': callback, 'error_callback': error_callback, 'sync_chunks': sync_chunks,
        'chunksize': chunksize,
    })

    ### NOTE: Invalidate `_exists` cache before and after syncing.
    self._exists = None

    def _sync(
        p: 'meerschaum.Pipe',
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
            register_tuple = p.register(debug=debug)
            if not register_tuple[0]:
                p._exists = None
                return register_tuple

        ### If connector is a plugin with a `sync()` method, return that instead.
        ### If the plugin does not have a `sync()` method but does have a `fetch()` method,
        ### use that instead.
        ### NOTE: The DataFrame must be omitted for the plugin sync method to apply.
        ### If a DataFrame is provided, continue as expected.
        if hasattr(df, 'MRSM_INFER_FETCH'):
            try:
                if p.connector is None:
                    msg = f"{p} does not have a valid connector."
                    if p.connector_keys.startswith('plugin:'):
                        msg += f"\n    Perhaps {p.connector_keys} has a syntax error?"
                    p._exists = None
                    return False, msg
            except Exception as e:
                p._exists = None
                return False, f"Unable to create the connector for {p}."

            ### Sync in place if this is a SQL pipe.
            if (
                str(self.connector) == str(self.instance_connector)
                and 
                hasattr(self.instance_connector, 'sync_pipe_inplace')
                and
                get_config('system', 'experimental', 'inplace_sync')
            ):
                with Venv(get_connector_plugin(self.instance_connector)):
                    p._exists = None
                    return self.instance_connector.sync_pipe_inplace(p, debug=debug, **kw)


            ### Activate and invoke `sync(pipe)` for plugin connectors with `sync` methods.
            try:
                if p.connector.type == 'plugin' and p.connector.sync is not None:
                    connector_plugin = Plugin(p.connector.label)
                    with Venv(connector_plugin, debug=debug):
                        return_tuple = p.connector.sync(p, debug=debug, **kw)
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
                ### If was added by `make_connector`, activate the plugin's virtual environment.
                is_custom = p.connector.type in custom_types
                plugin = (
                    Plugin(p.connector.__module__.replace('plugins.', '').split('.')[0])
                    if is_custom else (
                        Plugin(p.connector.label) if p.connector.type == 'plugin'
                        else None
                    )
                )
                with Venv(plugin, debug=debug):
                    df = p.fetch(debug=debug, **kw)

            except Exception as e:
                get_console().print_exception(
                    suppress = [
                        'meerschaum/core/Pipe/_sync.py', 
                        'meerschaum/core/Pipe/_fetch.py', 
                    ]
                )
                msg = f"Failed to fetch data from {p.connector}:\n    {e}"
                df = None

            if df is None:
                p._exists = None
                return False, f"No data were fetched for {p}."

            ### TODO: Depreciate async?
            if df is True:
                p._exists = None
                return True, f"{p} is being synced in parallel."

        ### CHECKPOINT: Retrieved the DataFrame.
        _checkpoint(**kw)
        
        ### Cast to a dataframe and ensure datatypes are what we expect.
        df = self.enforce_dtypes(df, debug=debug)
        if debug:
            dprint(
                "DataFrame to sync:\n"
                + (str(df)[:255] + '...' if len(str(df)) >= 256 else str(df)),
                **kw
            )

        ### if force, continue to sync until success
        return_tuple = False, f"Did not sync {p}."
        run = True
        _retries = 1
        while run:
            with Venv(get_connector_plugin(self.instance_connector)):
                return_tuple = p.instance_connector.sync_pipe(
                    pipe = p,
                    df = df,
                    debug = debug,
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
                dprint(f"Caching retrieved dataframe.", **kw)
                _sync_cache_tuple = self.cache_pipe.sync(df, debug=debug, **kw)
                if not _sync_cache_tuple[0]:
                    warn(f"Failed to sync local cache for {self}.")

        self._exists = None
        return return_tuple

    if blocking:
        self._exists = None
        return _sync(self, df = df)

    ### TODO implement concurrent syncing (split DataFrame? mimic the functionality of modin?)
    from meerschaum.utils.threading import Thread
    def default_callback(result_tuple : SuccessTuple):
        dprint(f"Asynchronous result from {self}: {result_tuple}", **kw)
    def default_error_callback(x : Exception):
        dprint(f"Error received for {self}: {x}", **kw)
    if callback is None and debug:
        callback = default_callback
    if error_callback is None and debug:
        error_callback = default_error_callback
    try:
        thread = Thread(
            target = _sync,
            args = (self,),
            kwargs = {'df' : df},
            daemon = False,
            callback = callback,
            error_callback = error_callback
        )
        thread.start()
    except Exception as e:
        self._exists = None
        return False, str(e)
    self._exists = None
    return True, f"Spawned asyncronous sync for {self}."


def _determine_begin(
        pipe: meerschaum.Pipe,
        begin: Optional[datetime.datetime] = None,
        debug: bool = False,
    ) -> Union[datetime.datetime, None]:
    ### Datetime has already been provided.
    if begin is not None:
        return begin
    return pipe.get_sync_time(debug=debug)


def get_sync_time(
        self,
        params: Optional[Dict[str, Any]] = None,
        newest: bool = True,
        round_down: bool = True,
        debug: bool = False
    ) -> Union['datetime.datetime', None]:
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

    round_down: bool, default True
        If `True`, round down the sync time to the nearest minute.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `datetime.datetime` object if the pipe exists, otherwise `None`.

    """
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    with Venv(get_connector_plugin(self.instance_connector)):
        return self.instance_connector.get_sync_time(
            self,
            params = params,
            newest = newest,
            round_down = round_down,
            debug = debug,
        )


def exists(
        self,
        debug : bool = False
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
        _exists = self.instance_connector.pipe_exists(pipe=self, debug=debug)

    self.__dict__['_exists'] = _exists
    self.__dict__['_exists_timestamp'] = now
    return _exists


def filter_existing(
        self,
        df: 'pd.DataFrame',
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        chunksize: Optional[int] = -1,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False,
        **kw
    ) -> Tuple['pd.DataFrame', 'pd.DataFrame', 'pd.DataFrame']:
    """
    Inspect a dataframe and filter out rows which already exist in the pipe.

    Parameters
    ----------
    df: 'pd.DataFrame'
        The dataframe to inspect and filter.
        
    begin: Optional[datetime.datetime], default None
        NOTE: This is not used! The minimum datetime value is used.

    end: Optional[datetime.datetime], default
        If provided, use this boundary when searching for existing data.
        Else use the maximum datetime value (+1 for integer datetimes).

    chunksize: Optional[int], default -1
        The `chunksize` used when fetching existing data.

    params: Optional[Dict[str, Any]], default None
        If provided, use this filter when searching for existing data. 

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A tuple of three pandas DataFrames: unseen, update, and delta.
    """
    import datetime
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import, import_pandas
    from meerschaum.utils.misc import (
        round_time,
        filter_unseen_df,
        add_missing_cols_to_df,
        to_pandas_dtype,
        get_unhashable_cols,
    )

    pd = import_pandas()
    if not isinstance(df, pd.DataFrame):
        df = self.enforce_dtypes(df, debug=debug)

    if df.empty:
        return df, df, df

    ### begin is the oldest data in the new dataframe
    dt_col = self.columns.get('datetime', None)
    dt_type = self.dtypes.get(dt_col, 'datetime64[ns]') if dt_col else None
    try:
        min_dt_val = df[dt_col].min(skipna=True) if dt_col else None
        min_dt = (
            pd.to_datetime(min_dt_val).to_pydatetime()
            if 'datetime' in dt_type
            else min_dt_val
        )
    except Exception as e:
        min_dt = None
    if not ('datetime' in str(type(min_dt))) or str(min_dt) == 'NaT':
        if 'int' not in str(type(min_dt)).lower():
            min_dt = None

    if isinstance(min_dt, datetime.datetime):
        begin = (
            round_time(
                min_dt,
                to = 'down'
            ) - datetime.timedelta(minutes=1)
        )
    elif dt_type and 'int' in dt_type.lower():
        begin = min_dt

    ### end is the newest data in the new dataframe
    try:
        max_dt_val = df[dt_col].max(skipna=True) if dt_col else None
        max_dt = (
            pd.to_datetime(max_dt_val).to_pydatetime()
            if 'datetime' in dt_type
            else max_dt_val
        )
    except Exception as e:
        max_dt = None

    if not ('datetime' in str(type(max_dt))) or str(min_dt) == 'NaT':
        if 'int' not in str(type(max_dt)).lower():
            max_dt = None

    if end is None:
        if isinstance(max_dt, datetime.datetime):
            end = (
                round_time(
                    max_dt,
                    to = 'down'
                ) + datetime.timedelta(minutes=1)
            )
        elif dt_type and 'int' in dt_type.lower():
            end = max_dt + 1

    if max_dt is not None and min_dt is not None and min_dt > max_dt:
        warn(f"Detected minimum datetime greater than maximum datetime.")

    if begin is not None and end is not None and begin > end:
        if isinstance(begin, datetime.datetime):
            begin = end - datetime.timedelta(minutes=1)
        ### We might be using integers for out datetime axis.
        else:
            begin = end - 1

    if debug:
        dprint(f"Looking at data between '{begin}' and '{end}'.", **kw)

    backtrack_df = self.get_data(
        begin = begin,
        end = end,
        chunksize = chunksize,
        params = params,
        debug = debug,
        **kw
    )
    if debug:
        dprint("Existing data:\n" + str(backtrack_df), **kw)
        dprint("Existing dtypes:\n" + str(backtrack_df.dtypes))

    ### Separate new rows from changed ones.
    on_cols = [
        col for col_key, col in self.columns.items()
        if (
            col
            and
            col_key != 'value'
            and col in backtrack_df.columns
        )
    ]
    on_cols_dtypes = {
        col: to_pandas_dtype(typ)
        for col, typ in self.dtypes.items()
        if col in on_cols
    }

    ### Detect changes between the old target and new source dataframes.
    delta_df = add_missing_cols_to_df(
        filter_unseen_df(
            backtrack_df,
            df,
            dtypes = {
                col: to_pandas_dtype(typ)
                for col, typ in self.dtypes.items()
            },
            debug = debug
        ),
        on_cols_dtypes,
    )

    ### Cast dicts or lists to strings so we can merge.
    unhashable_delta_cols = get_unhashable_cols(delta_df)
    unhashable_backtrack_cols = get_unhashable_cols(backtrack_df)
    for col in unhashable_delta_cols:
        delta_df[col] = delta_df[col].apply(json.dumps)
    for col in unhashable_backtrack_cols:
        backtrack_df[col] = backtrack_df[col].apply(json.dumps)
    casted_cols = set(unhashable_delta_cols + unhashable_backtrack_cols)

    joined_df = pd.merge(
        delta_df,
        backtrack_df,
        how = 'left',
        on = on_cols,
        indicator = True,
        suffixes = ('', '_old'),
    ) if on_cols else delta_df
    for col in casted_cols:
        if col in joined_df.columns:
            joined_df[col] = joined_df[col].apply(
                lambda x: (
                    json.loads(x)
                    if isinstance(x, str)
                    else x
                )
            )

    ### Determine which rows are completely new.
    new_rows_mask = (joined_df['_merge'] == 'left_only') if on_cols else None
    cols = list(backtrack_df.columns)

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
    ) if on_cols else None

    return unseen_df, update_df, delta_df
