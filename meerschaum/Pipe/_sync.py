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
    from meerschaum.utils.misc import attempt_import, round_time, import_pandas
    import datetime as datetime_pkg
    pd = import_pandas()
    np = attempt_import('numpy')

    ### default: fetch new data via the connector.
    ### If new data is provided, skip fetching
    if df is None:
        if self.connector is None:
            return False, "Cannot fetch without a connector"
        df = self.fetch(debug=debug)

    if debug: dprint("DataFrame to sync:\n" + f"{df}")

    ### if the instance connector is API, use its method. Otherwise do SQL things below
    if self.instance_connector.type == 'api':
        return self.instance_connector.sync_pipe(
            pipe = self,
            df = df,
            debug = debug,
            check_existing = check_existing,
            **kw
        )

    datetime = self.get_columns('datetime')

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
        warning_msg = f"Was not able to sync Pipe '{self}'"
        warn(warning_msg)
        return False, warning_msg
    if debug: dprint("Fetched data:\n" + str(df))

    ### NOTE: this SHOULD only be executed if the instance connector is SQL
    sql_connector = self.instance_connector

    ### if table does not exist, create it with indices
    if not self.exists(debug=debug):
        if debug: dprint(f"Creating empty table for Pipe '{self}'...")
        if debug: dprint("New table data types:\n" + f"{df.head(0).dtypes}")
        ### create empty table
        sql_connector.to_sql(
            df.head(0),
            if_exists = 'append',
            name = str(self),
            debug = debug
        )
        ### build indices on Pipe's root table
        sql_connector.create_indices(self, debug=debug)

    def filter_existing():
        ### begin is the oldest data in the new dataframe
        try:
            min_dt = df[self.get_columns('datetime')].min().to_pydatetime()
        except:
            min_dt = None
        if min_dt in (np.nan, None):
            min_dt = self.sync_time
        begin = round_time(
            min_dt,
            to = 'down'
        ) - datetime_pkg.timedelta(minutes=1)
        if debug: dprint(f"Looking at data newer than '{begin}'")

        ### backtrack_df is existing Pipe data that overlaps with the fetched df
        try:
            backtrack_minutes = self.parameters['fetch']['backtrack_minutes']
        except:
            backtrack_minutes = 0

        backtrack_df = self.get_backtrack_data(begin=begin, backtrack_minutes=backtrack_minutes, debug=debug)
        if debug: dprint("Existing data:\n" + str(backtrack_df))

        ### remove data we've already seen before
        from meerschaum.utils.misc import filter_unseen_df
        return filter_unseen_df(backtrack_df, df, debug=debug)

    new_data_df = filter_existing() if check_existing else df
    if debug: dprint(f"New unseen data:\n" + str(new_data_df))

    if_exists = kw.get('if_exists', 'append')

    ### append new data to Pipe's table
    sql_connector.to_sql(
        new_data_df,
        name = str(self),
        if_exists = if_exists,
        debug = debug,
        **kw
    )
    return True, "Success"

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

