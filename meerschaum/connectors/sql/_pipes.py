#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with Pipes metadata via SQLConnector.
"""
from __future__ import annotations
from meerschaum.utils.typing import (
    Union, Any, Sequence, SuccessTuple, Mapping, Tuple
)

def register_pipe(
        self,
        pipe : meerschaum.Pipe,
        debug : bool = False,
    ) -> SuccessTuple:
    """
    Register a new pipe.
    A pipe's attributes must be set before registering.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    pipes = get_tables(mrsm_instance=self, debug=debug)['pipes']

    if pipe.id is not None:
        return False, f"Pipe '{pipe}' is already registered"

    ### NOTE: if `parameters` is supplied in the Pipe constructor,
    ###       then `pipe.parameters` will exist and not be fetched from the database.

    ### 1. Prioritize the Pipe object's `parameters` first.
    ###    E.g. if the user manually sets the `parameters` property
    ###    or if the Pipe already exists
    ###    (which shouldn't be able to be registered anyway but that's an issue for later).
    parameters = None
    try:
        parameters = pipe.parameters
    except Exception as e:
        if debug: dprint(str(e))
        parameters = None

    ### ensure `parameters` is a dictionary
    if parameters is None:
        parameters = dict()

    json, sqlalchemy = attempt_import('json', 'sqlalchemy')
    values = {
        'connector_keys' : pipe.connector_keys,
        'metric_key'     : pipe.metric_key,
        'location_key'   : pipe.location_key,
        'parameters'     : json.dumps(pipe.parameters),
    }
    query = sqlalchemy.insert(pipes).values(**values)
    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to register pipe '{pipe}'"
    return True, f"Successfully registered pipe '{pipe}'"

def edit_pipe(
        self,
        pipe : meerschaum.Pipe = None,
        patch : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Edit a Pipe's parameters.

    :param pipe:
        The pipe to be edited.

    :param patch:
        If patch is True, update the existing parameters by cascading.
        Otherwise overwrite the parameters (default).

    :param debug: Verbosity toggle.
    """

    if pipe.id is None:
        return False, f"pipe '{pipe}' is not registered and cannot be edited."

    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    if not patch:
        parameters = pipe.parameters
    else:
        from meerschaum import Pipe
        from meerschaum.config._patch import apply_patch_to_config
        original_parameters = Pipe(pipe.connector_keys, pipe.metric_key, pipe.location_key).parameters
        parameters = apply_patch_to_config(
            original_parameters,
            pipe.parameters
        )

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    pipes = get_tables(mrsm_instance=self, debug=debug)['pipes']

    json, sqlalchemy = attempt_import('json', 'sqlalchemy')

    values = {
        'parameters' : json.dumps(pipe.parameters),
    }
    q = sqlalchemy.update(pipes).values(**values).where(
        pipes.c.pipe_id == pipe.id
    )

    result = self.exec(q, debug=debug)
    message = f"Successfully edited pipe '{pipe}'"
    if result is None:
        message = f"Failed to edit pipe '{pipe}'"
    return (result is not None), message

def fetch_pipes_keys(
        self,
        connector_keys : Sequence[str] = [],
        metric_keys : Sequence[str] = [],
        location_keys : Sequence[str] = [],
        params : Mapping[str, Any] = {},
        debug : bool = False
    ) -> Optional[Sequence[Tuple[str, str, Optional[str]]]]:
    """
    Return a list of tuples corresponding to the parameters provided.

    :param connector_keys:
        List of connector_keys to search by.

    :param metric_keys:
        List of metric_keys to search by.

    :param location_keys:
        List of location_keys to search by.

    :param params:
        Dictionary of additional parameters to search by.
        E.g. --params pipe_id:1

    :param debug: Verbosity toggle.
    """
    from meerschaum.utils.warnings import error
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    json, sqlalchemy = attempt_import('json', 'sqlalchemy')

    ### Add three primary keys to params dictionary
    ###   (separated for convenience of arguments)
    cols = {
        'connector_keys' : connector_keys,
        'metric_key' : metric_keys,
        'location_key' : location_keys,
    }

    ### make deep copy because something weird is happening with pointers
    from copy import deepcopy
    parameters = deepcopy(params)
    for col, vals in cols.items():
        ### allow for IS NULL to be declared as a single-item list ([None])
        if vals == [None]: vals = None
        if vals not in [[], ['*']]:
            parameters[col] = vals
    cols = {k : v for k, v in cols.items() if v != [None]}

    from meerschaum.connectors.sql.tables import get_tables
    pipes = get_tables(mrsm_instance=self, debug=debug)['pipes']

    _params = {}
    for k, v in parameters.items():
        _v = json.dumps(v) if isinstance(v, dict) else v
        _params[k] = _v

    ### parse regular params
    _where = [
        pipes.c[key] == val
        for key, val in _params.items() if not isinstance(val, list)
    ]
    q = sqlalchemy.select(
        [pipes.c.connector_keys, pipes.c.metric_key, pipes.c.location_key]
    ).where(sqlalchemy.and_(*_where))

    ### parse IN params
    for c, vals in cols.items():
        if vals:
            q = q.where(pipes.c[c].in_(vals))

    ### execute the query and return a list of tuples
    try:
        if debug: dprint(q)
        return self.engine.execute(q).fetchall()
    except Exception as e:
        error(str(e))

    return None

def create_indices(
        self,
        pipe : meerschaum.Pipe,
        debug : bool = False
    ) -> bool:
    """
    Create indices for a Pipe's datetime and ID columns.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import sql_item_name
    from meerschaum.utils.warnings import warn
    index_queries = dict()

    if debug: dprint(f"Creating indices for Pipe '{pipe}'...")

    ### create datetime index
    if 'datetime' in pipe.columns and pipe.get_columns('datetime'):
        if self.flavor == 'timescaledb':
            ## create hypertable
            dt_query = (
                f"SELECT create_hypertable('{sql_item_name(str(pipe), self.flavor)}', " +
                f"'{pipe.get_columns('datetime')}', migrate_data => true);"
            )
        elif self.flavor == 'postgresql':
            dt_query = f"CREATE INDEX ON {sql_item_name(str(pipe), self.flavor)} ({sql_item_name(pipe.get_columns('datetime'), self.flavor)})"
        elif self.flavor in ('mysql', 'mariadb'):
            dt_query = f"CREATE INDEX ON {sql_item_name(str(pipe), self.flavor)} ({sql_item_name(pipe.get_columns('datetime'), self.flavor)})"
        else: ### mssql, sqlite, etc.
            dt_query = f"CREATE INDEX {pipe.get_columns('datetime').lower()}_index ON {pipe} ({sql_item_name(pipe.get_columns('datetime'), self.flavor)})"

        index_queries[pipe.get_columns('datetime')] = dt_query

    ### create id index
    if 'id' in pipe.columns and pipe.get_columns('id', error=False):
        if self.flavor in ('timescaledb', 'postgresql'):
            id_query = f"CREATE INDEX ON {sql_item_name(str(pipe), self.flavor)} ({sql_item_name(pipe.get_columns('id'), self.flavor)})"
        elif self.flavor in ('mysql', 'mariadb'):
            id_query = f"CREATE INDEX ON {sql_item_name(str(pipe), self.flavor)} ({sql_item_name(pipe.get_columns('id'), self.flavor)})"
        else: ### mssql, sqlite, etc.
            id_query = f"CREATE INDEX {pipe.get_columns('id').lower()}_index ON {sql_item_name(str(pipe), self.flavor)} ({sql_item_name(pipe.get_columns('id'), self.flavor)})"

        index_queries[pipe.get_columns('id')] = id_query

    failures = 0
    for col, q in index_queries.items():
        if debug: dprint(f"Creating index on column '{col}' for Pipe '{pipe}'")
        if not self.exec(q, silent=True, debug=debug):
            warn(f"Failed to create index on column '{col}' for Pipe '{pipe}'")
            failures += 1
    return failures == 0

def delete_pipe(
        self,
        pipe : meerschaum.Pipe,
        debug : bool = False,
    ) -> SuccessTuple:
    """
    Delete a Pipe's registration and drop its table.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.misc import sql_item_name
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    pipe_name = sql_item_name(str(pipe), self.flavor)
    if not pipe.id:
        return False, f"Pipe '{pipe}' is not registered"

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    pipes = get_tables(mrsm_instance=self, debug=debug)['pipes']

    q = sqlalchemy.delete(pipes).where(pipes.c.pipe_id == pipe.id)
    # q = f"DELETE FROM pipes WHERE pipe_id = {pipe.id}"
    if not self.exec(q, debug=debug):
        return False, f"Failed to delete registration for '{pipe}'"

    q = f"DROP TABLE {pipe_name}"
    if self.exec(q, silent=True, debug=debug) is None:
        q = f"DROP VIEW {pipe_name}"
    if self.exec(q, silent=True, debug=debug) is None:
        if debug: dprint(f"Failed to drop '{pipe}'. Ignoring...")

    return True, "Success"

def get_backtrack_data(
        self,
        pipe : Optional[meerschaum.Pipe] = None,
        backtrack_minutes : int = 0,
        begin : Optional[datetime.datetime] = None,
        debug : bool = False
    ) -> Optional[pandas.DataFrame]:
    """
    Get the most recent backtrack_minutes' worth of data from a Pipe
    """
    from meerschaum.utils.warnings import error, warn
    if pipe is None: error(f"Pipe must be provided")
    from meerschaum.utils.debug import dprint
    from meerschaum.connectors.sql._fetch import dateadd_str
    if begin is None: begin = pipe.sync_time
    da = dateadd_str(
        flavor = self.flavor,
        datepart = 'minute',
        number = (-1 * backtrack_minutes),
        begin = begin
    )

    ### check for capitals
    from meerschaum.utils.misc import sql_item_name
    table = sql_item_name(str(pipe), self.flavor)
    dt = sql_item_name(pipe.get_columns('datetime'), self.flavor)

    query = f"SELECT * FROM {table}" + (f" WHERE {dt} > {da}" if da else "")

    df = self.read(query, debug=debug)

    if self.flavor == 'sqlite':
        from meerschaum.utils.misc import parse_df_datetimes
        df = parse_df_datetimes(df, debug=debug)

    return df

def get_pipe_data(
        self,
        pipe : Optional[meerschaum.Pipe] = None,
        begin : Union[datetime.datetime, str, None] = None,
        end : Union[datetime.datetime, str, None] = None,
        debug : bool = False
    ) -> Optional[pandas.DataFrame]:
    """
    Fetch data from a Pipe.

    :param begin:
        Lower bound for the query (inclusive)

    :param end:
        Upper bound for the query (inclusive)
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import sql_item_name
    from meerschaum.connectors.sql._fetch import dateadd_str
    query = f"SELECT * FROM {pipe}"
    where = ""

    dt = sql_item_name(pipe.get_columns('datetime'), self.flavor)

    if begin is not None:
        begin_da = dateadd_str(
            flavor = self.flavor,
            datepart = 'minute',
            number = 0,
            begin = begin
        )
        where += f"{dt} >= {begin_da}" + (" AND " if end is not None else "")

    if end is not None:
        end_da = dateadd_str(
            flavor = self.flavor,
            datepart = 'minute',
            number = 0,
            begin = end
        )
        where += f"{dt} <= {end_da}"

    if len(where) > 0:
        query += "\nWHERE " + where

    query += "\nORDER BY " + dt + " DESC"

    if debug: dprint(f"Getting Pipe data with begin = '{begin}' and end = '{end}'")
    df = self.read(query, debug=debug)
    if self.flavor == 'sqlite':
        from meerschaum.utils.misc import parse_df_datetimes
        return parse_df_datetimes(df, debug=debug)
    return df

def get_pipe_id(
        self,
        pipe : meerschaum.Pipe,
        debug : bool = False,
    ) -> int:
    """
    Get a Pipe's ID from the pipes table.
    """
    from meerschaum.utils.packages import attempt_import
    json, sqlalchemy = attempt_import('json', 'sqlalchemy')
    from meerschaum.connectors.sql.tables import get_tables
    pipes = get_tables()['pipes']

    query = sqlalchemy.select([pipes.c.pipe_id]).where(
        pipes.c.connector_keys == pipe.connector_keys and
        pipes.c.metric_key == pipe.metric_key and
        pipes.c.location_key == pipe.location_key
    )
    _id = self.value(query, debug=debug)
    if _id is not None:
        _id = int(_id)
    return _id

def get_pipe_attributes(
        self,
        pipe : meerschaum.Pipe,
        debug : bool = False
    ) -> Optional[Mapping[Any, Any]]:
    """
    Get a Pipe's attributes dictionary
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.connectors.sql.tables import get_tables
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    pipes = get_tables()['pipes']
    try:
        q = sqlalchemy.select([pipes]).where(pipes.c.pipe_id == pipe.id)
        attributes = self.read(q, debug=debug).to_dict('records')[0]
    except Exception as e:
        warn(e)
        return None

    ### handle non-PostgreSQL databases (text vs JSON)
    if not isinstance(attributes['parameters'], dict):
        try:
            import json
            attributes['parameters'] = json.loads(attributes['parameters'])
        except:
            attributes['parameters'] = dict()

    return attributes

def sync_pipe(
        self,
        pipe : 'meerschaum.Pipe',
        df : 'pd.DataFrame' = None,
        begin : 'datetime.datetime' = None,
        end : 'datetime.datetime' = None,
        check_existing : bool = True,
        blocking : bool = True,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Sync a pipe using a SQL Connection.

    :param pipe: The Meerschaum Pipe instance into which to sync the data.
    :type pipe: meerschaum.Pipe
    :param df: An optional DataFrame to sync into the pipe, defaults to None.
    :type df: pandas.DataFrame
    :param begin: Optionally specify the earliest datetime to search for data, defaults to None.
    :type begin: datetime.datetime
    :param end: Optionally specify the latelst datetime to search for data, defaults to None.
    :type end: datetime.datetime
    :param check_existing: If True, pull and diff with existing data from the pipe, defaults to True.
    :type check_existing: bool
    :param blocking: If True, wait for sync to finish and return its result, otherwise asyncronously sync. Defaults to True.
    :type blocking: bool
    :param debug: Verbosity toggle. Defaults to False.
    :type debug: bool
    :param kw: Catch-all for keyword arguments.
    :type kw: dict
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import import_pandas
    if df is None:
        msg = f"DataFrame is None. Cannot sync Pipe '{pipe}'"
        warn(msg)
        return False, msg

    ## allow syncing for JSON or dict as well as DataFrame
    pd = import_pandas()
    if isinstance(df, dict): df = pd.DataFrame(df)
    elif isinstance(df, str): df = pd.read_json(df)

    ### if Pipe is not registered
    if not pipe.id:
        register_tuple = pipe.register(debug=debug)
        if not register_tuple[0]:
            return register_tuple

    ### quit here if implicitly syncing MQTT pipes.
    ### (pipe.sync() is called in the callback of the MQTTConnector.fetch() method)
    if df is None and pipe.connector.type == 'mqtt':
        return True, "Success"

    ### df is the dataframe returned from the remote source
    ### via the connector
    if debug: dprint("Fetched data:\n" + str(df))

    ### if table does not exist, create it with indices
    if not pipe.exists(debug=debug):
        if debug: dprint(f"Creating empty table for Pipe '{pipe}'...")
        if debug: dprint("New table data types:\n" + f"{df.head(0).dtypes}")
        ### create empty table
        success = self.to_sql(
            df.head(0),
            if_exists = 'append',
            name = str(pipe),
            debug = debug
        )
        if success and debug: dprint(f"Successfully created table for Pipe '{pipe}'. Creating indices...")
        elif not success:
            msg = f"Failed to create table for Pipe '{pipe}'."
            if debug: dprint(msg + " Exiting...")
            return False, msg
        ### build indices on Pipe's root table
        if not self.create_indices(pipe, debug=debug):
            if debug: dprint(f"Failed to create indices for Pipe '{pipe}'. Continuing...")

    new_data_df = filter_existing(pipe, df, debug=debug) if check_existing else df
    if debug: dprint(f"New unseen data:\n" + str(new_data_df))

    if_exists = kw.get('if_exists', 'append')
    if 'if_exists' in kw: kw.pop('if_exists')

    ### append new data to Pipe's table
    return self.to_sql(
        new_data_df,
        name = str(pipe),
        if_exists = if_exists,
        debug = debug,
        as_tuple = True,
        **kw
    )

def filter_existing(pipe, df, debug : bool = False):
    """
    Remove duplicate data from backtrack_data and a new dataframe
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import round_time
    from meerschaum.utils.packages import attempt_import
    np = attempt_import('numpy')
    import datetime as datetime_pkg
    ### begin is the oldest data in the new dataframe
    try:
        min_dt = df[pipe.get_columns('datetime')].min().to_pydatetime()
    except:
        min_dt = pipe.get_sync_time(debug=debug)
    if min_dt in (np.nan, None):
        min_dt = None
    begin = round_time(
        min_dt,
        to = 'down'
    ) - datetime_pkg.timedelta(minutes=1)
    if debug: dprint(f"Looking at data newer than '{begin}'")

    ### backtrack_df is existing Pipe data that overlaps with the fetched df
    try:
        backtrack_minutes = pipe.parameters['fetch']['backtrack_minutes']
    except:
        backtrack_minutes = 0

    backtrack_df = pipe.get_backtrack_data(begin=begin, backtrack_minutes=backtrack_minutes, debug=debug)
    if debug: dprint("Existing data:\n" + str(backtrack_df))

    ### remove data we've already seen before
    from meerschaum.utils.misc import filter_unseen_df
    return filter_unseen_df(backtrack_df, df, debug=debug)

def get_sync_time(
        self,
        pipe : 'meerschaum.Pipe',
        params : dict = None,
        debug : bool = False,
    ) -> 'datetime.datetime':
    """
    Get a Pipe's most recent datetime
    """
    from meerschaum.utils.misc import sql_item_name, build_where
    from meerschaum.utils.warnings import warn
    table = sql_item_name(str(pipe), self.flavor)
    dt = sql_item_name(pipe.get_columns('datetime'), self.flavor)

    where = "" if params is None else build_where(params)
    q = f"SELECT {dt}\nFROM {table}{where}\nORDER BY {dt} DESC\nLIMIT 1"
    if self.flavor == 'mssql':
        q = f"SELECT TOP 1 {dt}\nFROM {table}{where}\nORDER BY {dt} DESC"
    try:
        from meerschaum.utils.misc import round_time
        import datetime
        db_time = self.value(q, silent=True, debug=debug)

        ### sqlite returns str
        if db_time is None: return None
        elif isinstance(db_time, str):
            from meerschaum.utils.packages import attempt_import
            dateutil_parser = attempt_import('dateutil.parser')
            st = dateutil_parser.parse(db_time)
        else:
            st = db_time.to_pydatetime()

        ### round down to smooth timestamp
        sync_time = round_time(st, date_delta=datetime.timedelta(minutes=1), to='down')

    except Exception as e:
        sync_time = None
        warn(str(e))

    return sync_time

def pipe_exists(
        self,
        pipe : 'meerschaum.Pipe',
        debug : bool = False
    ) -> bool:
    """
    Check that a Pipe's table exists
    """
    from meerschaum.utils.misc import sql_item_name
    from meerschaum.utils.debug import dprint
    ### default: select no rows. NOTE: this might not work for Oracle
    q = f"SELECT COUNT(*) FROM {pipe}"
    if self.flavor in ('timescaledb', 'postgresql'):
        q = f"SELECT to_regclass('{sql_item_name(str(pipe), self.flavor)}')"
    elif self.flavor == 'mssql':
        q = f"SELECT OBJECT_ID('{pipe}')"
    elif self.flavor in ('mysql', 'mariadb'):
        q = f"SHOW TABLES LIKE '{pipe}'"
    elif self.flavor == 'sqlite':
        q = f"SELECT name FROM sqlite_master WHERE name='{pipe}'"
    exists = self.value(q, debug=debug) is not None
    if debug: dprint(f"Pipe '{pipe}' " + ('exists.' if exists else 'does not exist.'))
    return exists

def get_pipe_rowcount(
        self,
        pipe : 'meerschaum.Pipe',
        begin : 'datetime.datetime' = None,
        end : 'datetime.datetime' = None,
        remote : bool = False,
        debug : bool = False
    ) -> int:
    """
    Return the number of rows between datetimes for a Pipe's instance cache or remote source
    """
    from meerschaum.utils.warnings import error, warn
    if remote:
        msg = f"'fetch:definition' must be an attribute of pipe '{pipe}' to get a remote rowcount"
        if 'fetch' not in pipe.parameters:
            error(msg)
            return None
        if 'definition' not in pipe.parameters['fetch']:
            error(msg)
            return None
    if 'datetime' not in pipe.columns: error(f"Pipe '{pipe}' must have a 'datetime' column declared (columns:datetime)")
    src = str(pipe) if not remote else pipe.parameters['fetch']['definition']
    query = f"""
    WITH src AS ({src})
    SELECT COUNT({pipe.columns['datetime']})
    FROM src
    """
    if begin is not None or end is not None: query += "WHERE"
    if begin is not None:
        query += f"""
        {pipe.columns['datetime']} > {dateadd_str(flavor=self.flavor, datepart='minute', number=(0), begin=begin)}
        """
    if end is not None and begin is not None: query += "AND"
    if end is not None:
        query += f"""
        {pipe.columns['datetime']} <= {dateadd_str(flavor=self.flavor, datepart='minute', number=(0), begin=end)}
        """
    return self.value(query, debug=debug)
