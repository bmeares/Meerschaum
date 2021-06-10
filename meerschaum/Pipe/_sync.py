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

def sync(
        self,

        df : Optional[
            Union[
                pandas.DataFrame,
                Dict[str, List[Any]]
            ]
        ] = None,

        begin : Optional[
            datetime.datetime
        ] = None,

        end : Optional[
            datetime.datetime
        ] = None,

        force : bool = False,

        retries : int = 10,

        min_seconds : int = 1,

        check_existing : bool = True,

        blocking : bool = True,

        workers : Optional[int] = None,

        callback : Optional[
            Callable[[Tuple[bool, str]], Any]
        ] = None,

        error_callback : Optional[
            Callable[[Exception], Any]
        ] = None,

        chunksize : Optional[int] = -1,

        sync_chunks : bool = False,

        debug : bool = False,

        **kw : Any

    ) -> SuccessTuple:
    """
    Fetch new data from the source and update the pipe's table with new data.

    Get new remote data via fetch, get existing data in the same time period,
    and merge the two, only keeping the unseen data.

    :param df:
        An optional DataFrame to sync into the pipe. Defaults to None.

    :param begin:
        Optionally specify the earliest datetime to search for data.
        Defaults to None.

    :param end:
        Optionally specify the latest datetime to search for data.
        Defaults to None.

    :param force:
        If True, keep trying to sync untul `retries` attempts.
        Defaults to False.

    :param retries:
        If force, how many attempts to try syncing before declaring failure.
        Defaults to 10.

    :param min_seconds:
        If force, how many seconds to sleep between retries. Defaults to 1.

    :param check_existing:
        If True, pull and diff with existing data from the pipe.
        Defaults to True.

    :param blocking:
        If True, wait for sync to finish and return its result, otherwise
        asyncronously sync (oxymoron?) and return success. Defaults to True.

    :param workers:
        No use directly within `Pipe.sync()`. Instead is passed on to
        instance connectors' `sync_pipe()` methods (e.g. PluginConnector).
        Defaults to None.

    :param callback:
        Callback function which expects a SuccessTuple as input.
        Only applies when blocking = False.

    :param error_callback:
        Callback function which expects an Exception as input.
        Only applies when blocking = False.

    :param chunksize:
        Specify the number of rows to sync per chunk.
        If -1, resort to system configuration (default is 900).
        A `chunksize` of `None` will sync all rows in one transaction.
        Defaults to -1.

    :param sync_chunks:
        If possible, sync chunks in parallel.
        Defaults to False.

    :param debug:
        Verbosity toggle. Defaults to False.

    :param kw:
        Catch-all for keyword arguments.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error
    import time
    if (callback is not None or error_callback is not None) and blocking:
        warn("Callback functions are only executed when blocking = False. Ignoring...")

    if (
          not self.connector_keys.startswith('plugin:')
          and not self.get_columns('datetime', error=False)
    ):
        return False, f"Cannot sync pipe '{self}' without a datetime column."

    ### add the stated arguments back into kw
    ### NOTE: Setting begin to the sync time; might need to tweak how this works for multiplexing.
    kw.update({
        'begin' : (begin if begin is not None else self.sync_time), 'end' : end, 'force' : force,
        'retries' : retries, 'min_seconds' : min_seconds, 'check_existing' : check_existing,
        'blocking' : blocking, 'workers' : workers, 'callback' : callback,
        'error_callback' : error_callback, 'sync_chunks' : (sync_chunks),
        'chunksize' : chunksize,
    })

    def _sync(
        p : 'meerschaum.Pipe',
        df : Optional['pandas.DataFrame'] = None
    ) -> SuccessTuple:
        ### ensure that Pipe is registered
        if not p.id:
            register_tuple = p.register(debug=debug)
            if not register_tuple[0]:
                return register_tuple

        ### If connector is a plugin with a `sync()` method, return that instead.
        ### If the plugin does not have a `sync()` method but does have a `fetch()` method,
        ### use that instead.
        ### NOTE: The DataFrame must be None for the plugin sync method to apply.
        ### If a DataFrame is provided, continue as expected.
        if df is None:
            try:
                if p.connector.type == 'plugin' and p.connector.sync is not None:
                    from meerschaum.utils.packages import activate_venv, deactivate_venv
                    activate_venv(p.connector.label, debug=debug)
                    return_tuple = p.connector.sync(p, debug=debug, **kw)
                    deactivate_venv(p.connector.label, debug=debug)
                    if not isinstance(return_tuple, tuple):
                        return_tuple = (
                            False,
                            f"Plugin '{p.connector.label}' returned non-tuple value: {return_tuple}"
                        )
                    return return_tuple

            except Exception as e:
                msg = f"Failed to sync pipe '{p}' with exception: '" + str(e) + "'"
                if debug:
                    error(msg, silent=False)
                return False, msg

        ### default: fetch new data via the connector.
        ### If new data is provided, skip fetching
        if df is None:
            if p.connector is None:
                return False, f"Cannot fetch data for pipe '{p}' without a connector."
            df = p.fetch(debug=debug, **kw)
            if df is None:
                return False, f"Unable to fetch data for pipe '{p}'."
            if df is True:
                return True, f"Pipe '{p}' was synced in parallel."

        if debug:
            dprint(
                "DataFrame to sync:\n"
                + (str(df)[:255] + '...' if len(str(df)) >= 256 else str(df))
            )

        ### if force, continue to sync until success
        return_tuple = False, f"Did not sync pipe '{p}'."
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
                dprint(f"Syncing failed for pipe '{p}'. Attempt ( {_retries} / {retries} )")
                dprint(f"Sleeping for {min_seconds} seconds...")
                time.sleep(min_seconds)
            if _retries > retries:
                warn(
                    f"Unable to sync pipe '{p}' within {retries} attempt" +
                        ("s" if retries != 1 else "") + "!"
                )
        ### Finished syncing. Handle caching.
        if self.cache_pipe is not None:
            if debug:
                dprint(f"Caching retrieved dataframe.")
                _sync_cache_tuple = self.cache_pipe.sync(df, debug=debug, **kw)
                if not _sync_cache_tuple[0]:
                    warn(f"Failed to sync local cache for pipe '{self}'.")

        return return_tuple

    if blocking:
        return _sync(self, df = df)

    ### TODO implement concurrent syncing (split DataFrame? mimic the functionality of modin?)
    from meerschaum.utils.threading import Thread
    def default_callback(result_tuple : SuccessTuple):
        dprint(f"Asynchronous result from Pipe '{self}': {result_tuple}")
    def default_error_callback(x : Exception):
        dprint(f"Error received for Pipe '{self}': {x}")
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
    return True, f"Spawned asyncronous sync for pipe '{self}'."

def get_sync_time(
        self,
        params : Optional[Mapping[str, Any]] = None,
        debug : bool = False
    ) -> Optional['datetime.datetime']:
    """
    Get the most recent datetime value for a Pipe.

    :param params:
        Dictionary to build a WHERE clause for a specific column.
        E.g. params = None returns the latest possible datetime,
        but params = { 'a' : 1 } returns the latest datetime 'WHERE a = 1'.

    :param debug: Verbosity toggle.
    """
    from meerschaum.utils.warnings import error, warn
    if self.columns is None:
        warn(
            f"No columns found for Pipe '{self}'. " +
            "Pipe might not be registered or is missing columns in parameters."
        )
        return None

    if 'datetime' not in self.columns:
        warn(
            f"'datetime' must be declared in parameters:columns for Pipe '{self}'.\n\n" +
            f"You can add parameters for this Pipe with the following command:\n\n" +
            f"mrsm edit pipes -C {self.connector_keys} -M " +
            f"{self.metric_key} -L " +
            (f"[None]" if self.location_key is None else f"{self.location_key}")
        )
        return None

    return self.instance_connector.get_sync_time(
        self, params = params, debug = debug
    )

def exists(
        self,
        debug : bool = False
    ) -> bool:
    """
    See if a Pipe's table or view exists.
    """
    ### TODO test against views
    return self.instance_connector.pipe_exists(pipe=self, debug=debug)
