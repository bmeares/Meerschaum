#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize a Pipe's data with its source via its connector
"""

from meerschaum.utils.debug import dprint

def sync(
        self,
        debug : bool = False,
        **kw
    ) -> bool:
    from meerschaum import get_connector
    sql_connector = get_connector(type='sql')
    df = self.fetch(debug=debug)
    if debug: dprint("Fetched data:\n" + str(df))
    if df is not None: sql_connector.to_sql(df, name=str(self), debug=debug)

def get_sync_time(
        self,
        debug : bool = False
    ) -> 'datetime.datetime':
    """
    Get the most recent datetime value for a Pipe
    """
    
    if 'datetime' not in self.columns:
        from meerschaum.utils.warnings import error
        error(
            f"'datetime' must be declared in parameters:columns for Pipe '{self}'.\n\n" +
            f"You can add parameters for this Pipe with the following command:\n\n" +
            f"mrsm edit pipes -C {self.connector_keys} -M " +
            f"{self.metric_key} -L " +
            (f"[None]" if self.location_key is None else f"{self.location_key}")
        )

    datetime = self.columns['datetime']

    q = f"SELECT {datetime} FROM {self} ORDER BY {datetime} DESC LIMIT 1"
    from meerschaum import get_connector
    main_connector = get_connector(type='sql', label='main')
    try:
        sync_time = main_connector.value(q).date()
    except:
        sync_time = None

    return sync_time
