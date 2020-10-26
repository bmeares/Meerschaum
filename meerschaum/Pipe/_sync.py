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
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Fetch new data from the source and update the Pipe's table with new data.

    Get new remote data via fetch, get existing data in the same time period,
        and merge the two, only keeping the unseen data.
    """
    from meerschaum import get_connector
    from meerschaum.config import get_config
    from meerschaum.utils.misc import attempt_import, round_time
    import datetime as datetime_pkg
    pd = attempt_import(get_config('system', 'connectors', 'all', 'pandas'))

    if df is None:
        df = self.fetch(debug = debug)
    else:
        if self.source_connector.type == 'api':
            return self.source_connector.sync_pipe(self, df, debug=debug)

    try:
        datetime = self.columns['datetime']
    except:
        warn(
            f"Columns not defined for {self}.\n" +
            f"Please set at a minimum pipe.columns = " + "{'datetime' : 'column_name'} " +
            f"or register Pipe '{self}'"
        )

    ### if Pipe is not registered
    if not self.id:
        self.register(debug=debug)

    ### quit here if implicitly syncing MQTT pipes.
    ### (pipe.sync() is called in the callback of the MQTTConnector.fetch() method)
    if df is None and self.connector.type == 'mqtt':
        return True, "Success"

    ### fetched df is the dataframe returned from the remote source
    ### via the connector
    if df is None:
        warn(f"Was not able to sync '{self}'")
        return None
    if debug: dprint("Fetched data:\n" + str(df))

    ### TODO use source connector, check for api instead of only SQL
    sql_connector = get_connector(type='sql', label='main', debug=debug)
    #  source_connector = self.source_connector

    ### if table does not exist, create it with indices
    if not self.exists(debug=debug):
        ### create empty table
        sql_connector.to_sql(
            df.head(0),
            if_exists = 'append',
            name = str(self)
        )
        ### build indices on Pipe's root table
        sql_connector.create_indices(self, debug=debug)

    ### begin is the oldest data in the new dataframe
    begin = round_time(
        df[
            self.columns['datetime']
        ].min().to_pydatetime(),
        to = 'down'
    ) - datetime_pkg.timedelta(minutes=1)
    if debug: dprint(f"Looking at data newer than {begin}")

    ### backtrack_df is existing Pipe data that overlaps with the fetched df
    backtrack_df = self.get_backtrack_data(begin=begin, debug=debug)
    if debug: dprint("Existing data:\n" + str(backtrack_df))

    ### merge the dataframes and drop duplicate data
    new_data_df = pd.concat([
        backtrack_df,
        df
    ]).drop_duplicates(keep=False, ignore_index=True)
    if debug: dprint(f"New unseen data:\n" + str(new_data_df))

    ### append new data to Pipe's table
    sql_connector.to_sql(
        new_data_df,
        name = str(self),
        if_exists = 'append',
        debug = debug
    )
    return True, "Success"

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
    except:
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
