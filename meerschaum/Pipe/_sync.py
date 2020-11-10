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
        check_existing = True,
        blocking : bool = True,
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
    if (callback is not None or error_callback is not None) and blocking:
        from meerschaum.utils.warnings import warn
        warn("Callback functions are only executed when blocking = False. Ignoring...")

    def do_sync(p, df=None):
        ### default: fetch new data via the connector.
        ### If new data is provided, skip fetching
        if df is None:
            if p.connector is None:
                return False, "Cannot fetch without a connector"
            df = p.fetch(debug=debug)

        if debug: dprint("DataFrame to sync:\n" + f"{df}")

        return p.instance_connector.sync_pipe(
            pipe = p,
            df = df,
            check_existing = check_existing,
            blocking = blocking,
            callback = callback,
            error_callback = error_callback,
            debug = debug,
            **kw
        )
    if blocking: return do_sync(self, df=df)
    from multiprocessing import cpu_count
    from multiprocessing.pool import ThreadPool as Pool
    pool = Pool(cpu_count())
    try:
        pool.apply_async(do_sync, (self,), kwds={'df' : df}, callback=callback, error_callback=error_callback)
    except Exception as e:
        return False, str(e)
    return True, f"Spawned asyncronous sync for pipe '{self}'"

def get_sync_time(
        self,
        debug : bool = False
    ) -> 'datetime.datetime':
    """
    Get the most recent datetime value for a Pipe
    """
    from meerschaum.utils.warnings import error, warn
    if self.columns is None:
        warn(f"No columns found for pipe '{self}'. Is pipe registered?")
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

    return self.instance_connector.get_sync_time(self, debug=debug)

def exists(
        self,
        debug : bool = False
    ) -> bool:
    """
    See if a Pipe's table or view exists
    """
    ### TODO test against views
    return self.instance_connector.pipe_exists(pipe=self, debug=debug)

