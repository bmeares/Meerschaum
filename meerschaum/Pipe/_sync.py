#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize a Pipe's data with its source via its connector
"""

from meerschaum.utils.debug import dprint
from meerschaum.utils.warnings import warn

def sync(
        self,
        debug : bool = False,
        **kw
    ) -> bool:
    """
    Fetch new data from the source and update the Pipe's table with new data.

    Get new remote data via fetch, get existing data in the same time period,
        and merge the two, only keeping the unseen data.
    """
    from meerschaum import get_connector
    from meerschaum.config import get_config
    from meerschaum.utils.misc import attempt_import, round_time
    pd = attempt_import(get_config('system', 'connectors', 'all', 'pandas'))

    ### fetch_df is the dataframe returned from the remote source via the connector
    fetch_df = self.fetch(debug=debug)
    if fetch_df is None:
        warn(f"Was not able to sync '{self}'")
        return None
    if debug: dprint("Fetched data:\n" + str(fetch_df))

    sql_connector = get_connector(type='sql', label='main', debug=debug)

    ### if table does not exist, create it with indices
    if not self.exists(debug=debug):
        ### create empty table
        sql_connector.to_sql(
            fetch_df.head(0),
            if_exists = 'append',
            name = str(self)
        )
        ### build indices on Pipe's root table
        sql_connector.create_indices(self, debug=debug)

    ### begin is the oldest data in the new dataframe
    begin = round_time(
        fetch_df[
            self.columns['datetime']
        ].min().to_pydatetime(),
        to = 'down'
    )
    if debug: dprint(f"Looking at data newer than {begin}")

    ### backtrack_df is existing Pipe data that overlaps with the fetch_df
    backtrack_df = self.get_backtrack_data(begin=begin, debug=debug)
    if debug: dprint("Existing data:\n" + str(backtrack_df))

    ### merge the dataframes and drop duplicate data
    new_data_df = pd.concat([
        backtrack_df,
        fetch_df
    ]).drop_duplicates(keep=False)
    if debug: dprint(f"New unseen data:\n" + str(new_data_df))

    ### append new data to Pipe's table
    sql_connector.to_sql(
        new_data_df,
        name = str(self),
        if_exists = 'append',
        debug = debug
    )
    return True

def get_backtrack_data(
        self,
        backtrack_minutes : int = 0,
        begin : 'datetime.datetime' = None,
        debug : bool = False
    )-> 'pd.DataFrame':
    """
    Get the last few `backtrack_minutes`' worth of data from a Pipe, or specify a begin datetime.
    """
    ### DEFAULT : 0
    try:
        backtrack_minutes = self.parameters['fetch']['backtrack_minutes']
    except KeyError:
        pass

    if begin is None: begin = self.sync_time

    from meerschaum.connectors.sql._fetch import dateadd_str
    da = dateadd_str(
        datepart = 'minute',
        number = (-1 * backtrack_minutes),
        begin = begin
    )

    query = f"SELECT * FROM {self}" + (f" WHERE {self.columns['datetime']} > {da}" if da else "")
    if debug: dprint(query)

    from meerschaum import get_connector
    main_connector = get_connector(type='sql')
    return main_connector.read(query)

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
        from meerschaum.utils.misc import round_time
        import datetime
        sync_time = round_time(
            main_connector.value(q).to_pydatetime(),
            date_delta = datetime.timedelta(minutes=1),
            to='down'
        )
    except:
        sync_time = None

    return sync_time

def exists(
        self,
        debug : bool = False
    ) -> bool:
    """
    See if a Pipe's table or view exists
    """
    ### TODO test against views

    from meerschaum import get_connector
    conn = get_connector('sql', 'main')
    if conn.flavor in ('timescaledb', 'postgresql'):
        q = f"SELECT to_regclass('{self}')"
    elif conn.flavor == 'mssql':
        q = f"SELECT OBJECT_ID('{self}')"
    elif conn.flavor in ('mysql', 'mariadb'):
        q = f"SHOW TABLES LIKE '{self}'"
    return conn.value(q) is not None
