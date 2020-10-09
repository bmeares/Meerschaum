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
    from meerschaum import get_connector
    from meerschaum.config import get_config
    from meerschaum.utils.misc import attempt_import
    pd = attempt_import(get_config('system', 'connectors', 'all', 'pandas'))

    sql_connector = get_connector(type='sql')
    fetch_df = self.fetch(debug=debug)
    if fetch_df is None:
        warn(f"Was not able to sync '{self}'")
        return None
    if debug: dprint("Fetched data:\n" + str(fetch_df))

    if (not self.exists(debug=debug)):
        ### create empty table
        sql_connector.to_sql(
            fetch_df.head(0),
            if_exists = 'append',
            name = str(self)
        )
        sql_connector.create_indices(self, debug=debug)

    if fetch_df is not None:
        pass

        #  sql_connector.to_sql(df, name=str(self), if_exists='append', debug=debug)

def get_backtrack_data(
        self,
        backtrack_minutes = 1440,
        debug : bool = False
    )-> 'pd.DataFrame':
    """
    Get the last few `backtrack_minutes`' worth of data from a Pipe
    """
    ### DEFAULT : 24 hours
    try:
        backtrack_minutes = self.parameters['fetch']['backtrack_minutes']
    except KeyError:
        pass

    from meerschaum.connectors.sql._fetch import dateadd_str
    da = dateadd_str(
        datepart = 'minute',
        number = (-1 * backtrack_minutes),
        begin = self.sync_time
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
