#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize a Pipe's data with its source via its connector
"""

from meerschaum.utils.debug import dprint
from meerschaum.utils.warnings import warn, error

def sync(
        self,
        df : 'pd.DataFrame' = None,
        force : bool = False,
        retries : bool = 10,
        min_seconds : int = 1,
        check_existing = True,
        blocking : bool = True,
        workers : int = None,
        callback : 'function' = None,
        error_callback : 'function' = None,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Fetch new data from the source and update the Pipe's table with new data.

    Get new remote data via fetch, get existing data in the same time period,
        and merge the two, only keeping the unseen data.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.misc import attempt_import
    import time
    if (callback is not None or error_callback is not None) and blocking:
        warn("Callback functions are only executed when blocking = False. Ignoring...")
    def _sync(p, df = None):
        ### ensure that Pipe is registered
        if not p.id:
            register_tuple = p.register(debug=debug)
            if not register_tuple[0]: return register_tuple

        ### If connector is a plugin with a `sync()` method, return that instead.
        ### If the plugin does not have a `sync()` method but does have a `fetch()` method,
        ### use that instead.
        ### NOTE: The DataFrame must be None for the plugin sync method to apply.
        ### If a DataFrame is provided, continue as expected.
        if df is None:
            try:
                if p.connector.type == 'plugin' and p.connector.sync is not None:
                    return p.connector.sync(p, debug=debug, **kw)
            except:
                pass

        ### default: fetch new data via the connector.
        ### If new data is provided, skip fetching
        if df is None:
            if p.connector is None:
                return False, "Cannot fetch without a connector"
            df = p.fetch(debug=debug)

        if debug: dprint("DataFrame to sync:\n" + f"{df}")

        ### if force, continue to sync until success
        return_tuple = False, f"Did not sync Pipe '{p}'"
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
                dprint(f"Syncing failed for Pipe '{p}'. Attempt ( {_retries} / {retries} )")
                dprint(f"Sleeping for {min_seconds} seconds...")
                time.sleep(min_seconds)
            if _retries > retries: warn(f"Unable to sync Pipe '{p}' within {retries} attempts!")
        return return_tuple

    if blocking: return _sync(self, df = df)

    ### TODO implement concurrent syncing (split DataFrame? mimic the functionality of modin?)
    from meerschaum.utils.threading import Thread
    def default_callback(result_tuple : tuple): dprint(f"Asynchronous result from Pipe '{self}': {result_tuple}")
    def default_error_callback(x): dprint(f"Error received for Pipe '{self}': {x}")
    if callback is None and debug: callback = default_callback
    if error_callback is None and debug: error_callback = default_error_callback
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
    return True, f"Spawned asyncronous sync for pipe '{self}'"

def get_sync_time(
        self,
        params : dict = None,
        debug : bool = False
    ) -> 'datetime.datetime':
    """
    Get the most recent datetime value for a Pipe
    """
    from meerschaum.utils.warnings import error, warn
    if self.columns is None:
        warn(f"No columns found for Pipe '{self}'. Pipe might not be registered or is missing columns in parameters.")
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

    return self.instance_connector.get_sync_time(self, params=params, debug=debug)

def exists(
        self,
        debug : bool = False
    ) -> bool:
    """
    See if a Pipe's table or view exists
    """
    ### TODO test against views
    return self.instance_connector.pipe_exists(pipe=self, debug=debug)

