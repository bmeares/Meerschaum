#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize a pipe's data with its source via its connector
"""

from __future__ import annotations

from meerschaum.utils.typing import (
    Union, Optional, Callable, Any, Tuple, SuccessTuple, Mapping, Dict, List
)

class InferFetch:
    pass

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
        deactivate_plugin_venv: bool = True,
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

    deactivate_plugin_venv: bool, default True
        If `True`, deactivate a plugin's virtual environment after syncing.
        Defaults to `True`.

    debug: bool, default False
        Verbosity toggle. Defaults to False.

    Returns
    -------
    A `SuccessTuple` of success (`bool`) and message (`str`).

    """
    from meerschaum.utils.debug import dprint, _checkpoint
    from meerschaum.utils.warnings import warn, error
    import datetime
    import time
    if (callback is not None or error_callback is not None) and blocking:
        warn("Callback functions are only executed when blocking = False. Ignoring...")

    _checkpoint(_total=2, **kw)

    if (
          not self.connector_keys.startswith('plugin:')
          and not self.get_columns('datetime', error=False)
    ):
        return False, f"Cannot sync {self} without a datetime column."

    ### NOTE: Setting begin to the sync time for Simple Sync.
    ### TODO: Add flag for specifying syncing method.
    begin = _determine_begin(self, begin, debug=debug)
    kw.update({
        'begin': begin, 'end': end, 'force': force, 'retries': retries, 'min_seconds': min_seconds,
        'check_existing': check_existing, 'blocking': blocking, 'workers': workers,
        'callback': callback, 'error_callback': error_callback, 'sync_chunks': sync_chunks,
        'chunksize': chunksize,
    })


    def _sync(
        p: 'meerschaum.Pipe',
        df: Union['pd.DataFrame', Dict[str, List[Any]], InferFetch] = InferFetch,
    ) -> SuccessTuple:
        if df is None:
            return (
                False,
                f"You passed `None` instead of data into `sync()` for {p}.\n"
                + "Omit the DataFrame to infer fetching.",
            )
        ### Ensure that Pipe is registered.
        if p.get_id(debug=debug) is None:
            ### NOTE: This may trigger an interactive session for plugins!
            register_tuple = p.register(debug=debug)
            if not register_tuple[0]:
                return register_tuple

        ### If connector is a plugin with a `sync()` method, return that instead.
        ### If the plugin does not have a `sync()` method but does have a `fetch()` method,
        ### use that instead.
        ### NOTE: The DataFrame must be omitted for the plugin sync method to apply.
        ### If a DataFrame is provided, continue as expected.
        if df is InferFetch:
            try:
                if p.connector is None:
                    msg = f"{p} does not have a valid connector."
                    if p.connector_keys.startswith('plugin:'):
                        msg += f"\n    Perhaps {p.connector_keys} has a syntax error?"
                    return False, msg
            except Exception as e:
                return False, f"Unable to create the connector for {p}."

            try:
                if p.connector.type == 'plugin' and p.connector.sync is not None:
                    from meerschaum.plugins import Plugin
                    connector_plugin = Plugin(p.connector.label)
                    connector_plugin.activate_venv(debug=debug)
                    return_tuple = p.connector.sync(p, debug=debug, **kw)
                    if deactivate_plugin_venv:
                        connector_plugin.deactivate_venv(debug=debug)
                    if not isinstance(return_tuple, tuple):
                        return_tuple = (
                            False,
                            f"Plugin '{p.connector.label}' returned non-tuple value: {return_tuple}"
                        )
                    return return_tuple

            except Exception as e:
                msg = f"Failed to sync {p} with exception: '" + str(e) + "'"
                if debug:
                    error(msg, silent=False)
                return False, msg

        ### default: fetch new data via the connector.
        ### If new data is provided, skip fetching.
        #  if df is None:
            if p.connector is None:
                return False, f"Cannot fetch data for {p} without a connector."
            df = p.fetch(debug=debug, **kw)
            if df is None:
                return False, f"Unable to fetch data for {p}."
            if df is True:
                return True, f"{p} is being synced in parallel."

        ### CHECKPOINT: Retrieved the DataFrame.
        _checkpoint(**kw)
        if debug:
            df = self.enforce_dtypes(df, debug=debug)
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

        return return_tuple

    if blocking:
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
        return False, str(e)
    return True, f"Spawned asyncronous sync for {self}."


def _determine_begin(
        pipe: meerschaum.Pipe,
        begin: Optional[datetime.datetime] = None,
        debug: bool = False,
    ) -> Union[datetime.datetime, None]:
    ### Datetime has already been provided.
    if begin is not None:
        return begin
    ### Only manipulate the datetime for SQL or API pipes.
    if not pipe.instance_connector.type in ('sql', 'api'):
        return begin

    return pipe.get_sync_time(debug=debug)


def get_sync_time(
        self,
        params : Optional[Dict[str, Any]] = None,
        newest: bool = True,
        round_down: bool = True,
        debug : bool = False
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
    from meerschaum.utils.warnings import error, warn
    if self.columns is None:
        warn(
            f"No columns found for {self}. " +
            "Pipe might not be registered or is missing columns in parameters."
        )
        return None

    if 'datetime' not in self.columns:
        warn(
            f"'datetime' must be declared in parameters:columns for {self}.\n\n" +
            f"You can add parameters for this pipe with the following command:\n\n" +
            f"mrsm edit pipes -c {self.connector_keys} -m " +
            f"{self.metric_key} -l " +
            (f"[None]" if self.location_key is None else f"{self.location_key}")
        )
        return None

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
    return self.instance_connector.pipe_exists(pipe=self, debug=debug)


def filter_existing(
        self,
        df: 'pd.DataFrame',
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        chunksize: Optional[int] = -1,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False,
        **kw
    ) -> Tuple['pd.DataFrame', Union['pd.DataFrame', None]]:
    """
    Inspect a dataframe and filter out rows which already exist in the pipe.

    Parameters
    ----------
    df: 'pd.DataFrame'
        The dataframe to inspect and filter.
        
    begin: Optional[datetime.datetime], default None
        If provided, use this boundary when searching for existing data.

    end: Optional[datetime.datetime], default
        If provided, use this boundary when searching for existing data.

    chunksize: Optional[int], default -1
        The `chunksize` used when fetching existing data.

    params: Optional[Dict[str, Any]], default None
        If provided, use this filter when searching for existing data. 

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `pd.DataFrame` with existing rows removed.

    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import round_time
    from meerschaum.utils.packages import attempt_import, import_pandas
    import datetime
    pd = import_pandas()
    if not isinstance(df, pd.DataFrame):
        df = self.enforce_dtypes(df, debug=debug)
    ### begin is the oldest data in the new dataframe
    try:
        min_dt = pd.to_datetime(df[self.get_columns('datetime')].min(skipna=True)).to_pydatetime()
    except Exception as e:
        ### NOTE: This will fetch the entire pipe!
        min_dt = self.get_sync_time(newest=False, debug=debug)
    if not isinstance(min_dt, datetime.datetime) or str(min_dt) == 'NaT':
        ### min_dt might be None, a user-supplied value, or the sync time.
        min_dt = begin
    ### If `min_dt` is None, use `datetime.utcnow()`.
    begin = round_time(
        min_dt,
        to = 'down'
    ) - datetime.timedelta(minutes=1)

    ### end is the newest data in the new dataframe
    try:
        max_dt = pd.to_datetime(df[self.get_columns('datetime')].max(skipna=True)).to_pydatetime()
    except Exception as e:
        max_dt = end
    if not isinstance(max_dt, datetime.datetime) or str(max_dt) == 'NaT':
        max_dt = None

    if max_dt is not None and min_dt > max_dt:
        warn(f"Detected minimum datetime greater than maximum datetime.")

    ### If `max_dt` is `None`, unbound the search.
    end = (
        round_time(
            max_dt,
            to = 'down'
        ) + datetime.timedelta(minutes=1)
    ) if max_dt is not None else end
    if begin is not None and end is not None and begin > end:
        begin = end - datetime.timedelta(minutes=1)

    if debug:
        dprint(f"Looking at data between '{begin}' and '{end}'.", **kw)

    ### backtrack_df is existing Pipe data that overlaps with the fetched df
    try:
        backtrack_minutes = self.parameters['fetch']['backtrack_minutes']
    except Exception as e:
        backtrack_minutes = 0

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

    ### Detect changes between the old target and new source dataframes.
    from meerschaum.utils.misc import filter_unseen_df
    delta_df = filter_unseen_df(backtrack_df, df, dtypes=self.dtypes, debug=debug)

    ### Separate new rows from changed ones.
    dt_col = self.columns['datetime']
    id_col = self.columns.get('id', None)
    on_cols = [dt_col] + ([id_col] if id_col is not None else [])

    joined_df = pd.merge(
        delta_df,
        backtrack_df,
        how='left',
        on=on_cols,
        indicator=True,
        suffixes=('', '_old'),
    )

    ### Determine which rows are completely new.
    new_rows_mask = (joined_df['_merge'] == 'left_only')
    cols = list(backtrack_df.columns)

    unseen_df = (
        joined_df
        .where(new_rows_mask)
        .dropna(how='all')[cols]
        .reset_index(drop=True)
    )

    ### Rows that have already been inserted but values have changed.
    update_df = (
        joined_df
        .where(~new_rows_mask)
        .dropna(how='all')[cols]
        .reset_index(drop=True)
    )

    return unseen_df, update_df, delta_df
