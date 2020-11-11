#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with Pipes metadata via SQLConnector

NOTE: These methods will only work for connectors which correspond to an
      existing Meerschaum database. Use with caution!
"""

def register_pipe(
        self,
        pipe : 'meerschaum.Pipe',
        debug : bool = False,
    ) -> dict:
    """
    Register a new Pipe
    """
    from meerschaum.utils.debug import dprint

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

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

    ### 2. If the parent pipe does not have `parameters` either manually set
    ###    or within the database, check the `meta.parameters` value (likely None as well)
    #  if parameters is None:
        #  try:
            #  parameters = pipe.meta.parameters
        #  except Exception as e:
            #  if debug: dprint(str(e))
            #  parameters = None

    ### ensure `parameters` is a dictionary
    if parameters is None:
        parameters = dict()

    ### override `meta.parameters` with parameters found from the above process
    #  pipe.meta.parameters = parameters

    ### NOTE: I know it seems strange that I'm reverting from a perfectly
    ### working async ORM query to a hand-written synchronous query.
    ### But I value the design change more than miniscule performanc gain,
    ### and I know that this method may be refactored later if necessary.

    ### generate the INSERT statement
    #  query = get_tables()['pipes'].insert().values(
        #  connector_keys = pipe.connector_keys,
        #  metric_key = pipe.metric_key,
        #  location_key = pipe.location_key,
        #  parameters = pipe.parameters,
    #  )
    #  asyncio.run(retry_connect(connector=self, debug=debug))
    #  last_record_id = asyncio.run(self.db.execute(query))
    #  return {**pipe.meta.dict(), "pipe_id": last_record_id}
    import json
    location_key = pipe.location_key
    if location_key is None:
        location_key = 'NULL'
    else:
        location_key = "'" + location_key + "'"
    query = f"""
    INSERT INTO pipes (
        connector_keys,
        metric_key,
        location_key,
        parameters
    ) VALUES (
        '{pipe.connector_keys}',
        '{pipe.metric_key}',
        {location_key},
        '{json.dumps(pipe.parameters)}'
    );
    """

    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to register pipe '{pipe}'"
    return True, f"Successfully registered pipe '{pipe}'"

def edit_pipe(
        self,
        pipe : 'meerschaum.Pipe' = None,
        patch : bool = False,
        debug : bool = False
    ) -> tuple:
    """
    Edit a Pipe's parameters.
    patch : bool : False
        If patch is True, update the existing parameters by cascading.
        Otherwise overwrite the parameters (default)
    """

    from meerschaum.utils.debug import dprint
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
    tables = get_tables(mrsm_instance=self, debug=debug)

    import json
    q = f"""
    UPDATE pipes
    SET parameters = '{json.dumps(pipe.parameters)}'
    WHERE connector_keys = '{pipe.connector_keys}'
        AND metric_key = '{pipe.metric_key}'
        AND location_key """ + ("IS NULL" if pipe.location_key is None else f"= '{pipe.location_key}'")
    result = self.exec(q, debug=debug)
    message = f"Successfully edited pipe '{pipe}'"
    if result is None:
        message = f"Failed to edit pipe '{pipe}'"
    return (result is not None), message


def fetch_pipes_keys(
        self,
        connector_keys : list = [],
        metric_keys : list = [],
        location_keys : list = [],
        params : dict = dict(),
        debug : bool = False
    ) -> list:
    """
    Build a query to return a list of tuples corresponding to the parameters provided.
    """
    def build_where(parameters : dict):
        """
        Build the WHERE clause based on the input criteria
        """
        where = ""
        leading_and = "\n    AND "
        for key, value in parameters.items():
            ### search across a list (i.e. IN syntax)
            if isinstance(value, list):
                where += f"{leading_and}{key} IN ("
                for item in value:
                    where += f"'{item}', "
                where = where[:-2] + ")"
                continue

            ### search a dictionary
            ### TODO take advantage of PostgreSQL JSON types
            elif isinstance(value, dict):
                import json
                where += (f"{leading_and}CAST({key} AS TEXT) = '" + json.dumps(value) + "'")
                continue

            where += f"{leading_and}{key} " + ("IS NULL" if value is None else f"= '{value}'")
        if len(where) > 1: where = "\nWHERE\n    " + where[len(leading_and):]
        return where

    from meerschaum.utils.warnings import error
    from meerschaum.utils.debug import dprint

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

    q = (
            "SELECT DISTINCT\n" +
            "    pipes.connector_keys, pipes.metric_key, pipes.location_key\n" +
            "FROM pipes"
    ) + build_where(parameters)

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

    ### execute the query and return a list of tuples
    try:
        if debug: dprint(q)
        data = self.engine.execute(q).fetchall()
    except Exception as e:
        error(str(e))

    return data

def create_indices(
        self,
        pipe : 'meerschaum.Pipe',
        debug : bool = False
    ) -> 'bool':
    """
    Create indices for a Pipe's datetime and ID columns.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import sql_item_name
    from meerschaum.utils.warnings import warn
    index_queries = dict()

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
    if 'id' in pipe.columns and pipe.get_columns('id'):
        if self.flavor in ('timescaledb', 'postgresql'):
            id_query = f"CREATE INDEX ON {sql_item_name(str(pipe), self.flavor)} ({sql_item_name(pipe.get_columns('id'), self.flavor)})"
        elif self.flavor in ('mysql', 'mariadb'):
            id_query = f"CREATE INDEX ON {sql_item_name(str(pipe), self.flavor)} ({sql_item_name(pipe.get_columns('id'), self.flavor)})"
        else: ### mssql, sqlite, etc.
            id_query = f"CREATE INDEX {pipe.get_columns('id').lower()}_index ON {sql_item_name(str(pipe), self.flavor)} ({sql_item_name(pipe.get_columns('id'), self.flavor)})"

        index_queries[pipe.get_columns('id')] = id_query

    for col, q in index_queries.items():
        if debug: dprint(f"Creating index on column '{col}' for Pipe '{pipe}'")
        if not self.exec(q, debug=debug):
            warn(f"Failed to create index on column '{col}' for Pipe '{pipe}'")
    return True

def delete_pipe(
        self,
        pipe : 'meerschaum.Pipe',
        debug : bool = False,
    ) -> tuple:
    """
    Delete a Pipe's entry and drop its table
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.misc import sql_item_name
    from meerschaum.utils.debug import dprint
    pipe_name = sql_item_name(str(pipe), self.flavor)
    if not pipe.id:
        return False, f"Pipe '{pipe}' is not registered"

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

    q = f"DELETE FROM pipes WHERE pipe_id = {pipe.id}"
    if not self.exec(q, debug=debug):
        return False, f"Failed to delete registration for '{pipe}'"
    
    q = f"DROP TABLE {pipe_name}"
    if self.exec(q, debug=debug) is None:
        q = f"DROP VIEW {pipe_name}"
    if self.exec(q, debug=debug) is None:
        if debug: dprint(f"Failed to drop '{pipe}'. Ignoring...")

    return True, "Success"

def get_backtrack_data(
        self,
        pipe : 'meerschaum.Pipe' = None,
        backtrack_minutes : int = 0,
        begin : 'datetime.datetime' = None,
        debug : bool = False
    ) -> 'pd.DataFrame':
    """
    Get the most recent backtrack_minutes' worth of data from a Pipe
    """
    from meerschaum.utils.warnings import error, warn
    if pipe is None:
        error(f"Pipe must be provided")
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
        pipe : 'meerschaum.Pipe' = None,
        begin : 'datetime.datetime or str' = None,
        end : 'datetime.datetime or str' = None,
        debug : bool = False
    ) -> 'pd.DataFrame':
    """
    Fetch data from a Pipe.

    begin : datetime.datetime : None
        Lower bound for the query (inclusive)

    end : datetime.datetime : None
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
            datepart = 'minute',
            number = 0,
            begin = begin
        )
        where += f"{dt} >= {begin_da}" + (" AND " if end is not None else "")

    if end is not None:
        end_da = dateadd_str(
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
        pipe : 'meerschaum.Pipe',
        debug : bool = False,
    ) -> int:
    """
    Get a Pipe's ID from the pipes table.
    """
    query = f"""
    SELECT pipe_id
    FROM pipes
    WHERE connector_keys = '{pipe.connector_keys}'
        AND metric_key = '{pipe.metric_key}'
        AND location_key """ + ("IS NULL" if pipe.location_key is None else f"= '{pipe.location_key}'")
    return self.value(query, debug=debug)

def get_pipe_attributes(
        self,
        pipe : 'meerschaum.Pipe',
        debug : bool = False
    ) -> dict:
    """
    Get a Pipe's attributes dictionary
    """
    from meerschaum.utils.warnings import warn
    try:
        attributes = self.read(
            ("SELECT * " +
             "FROM pipes " +
            f"WHERE pipe_id = {pipe.id}"),
        ).to_dict('records')[0]

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
        check_existing : bool = True,
        blocking : bool = True,
        callback : 'function' = None,
        error_callback : 'function' = None,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Sync a Pipe using a SQL Connection
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    if df is None:
        msg = f"DataFrame is None. Cannot sync Pipe '{pipe}'"
        warn(msg)
        return False, msg

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
        self.to_sql(
            df.head(0),
            if_exists = 'append',
            name = str(pipe),
            debug = debug
        )
        ### build indices on Pipe's root table
        self.create_indices(pipe, debug=debug)

    def filter_existing():
        from meerschaum.utils.misc import attempt_import, round_time
        np = attempt_import('numpy')
        import datetime as datetime_pkg
        ### begin is the oldest data in the new dataframe
        try:
            min_dt = df[pipe.get_columns('datetime')].min().to_pydatetime()
        except:
            min_dt = None
        if min_dt in (np.nan, None):
            min_dt = pipe.sync_time
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

    new_data_df = filter_existing() if check_existing else df
    if debug: dprint(f"New unseen data:\n" + str(new_data_df))

    if_exists = kw.get('if_exists', 'append')

    ### append new data to Pipe's table
    return self.to_sql(
        new_data_df,
        name = str(pipe),
        if_exists = if_exists,
        debug = debug,
        as_tuple = True,
        **kw
    )

def get_sync_time(
        self,
        pipe : 'meerschaum.Pipe',
        debug : bool = False,
    ) -> 'datetime.datetime':
    """
    Get a Pipe's most recent datetime
    """
    from meerschaum.utils.misc import sql_item_name
    from meerschaum.utils.warnings import warn
    table = sql_item_name(str(pipe), self.flavor)
    dt = sql_item_name(pipe.get_columns('datetime'), self.flavor)

    q = f"SELECT {dt} FROM {table} ORDER BY {dt} DESC LIMIT 1"
    if self.flavor == 'mssql':
        q = f"SELECT TOP 1 {dt} FROM {table} ORDER BY {dt} DESC"
    try:
        from meerschaum.utils.misc import round_time
        import datetime
        db_time = self.value(q, debug=debug)

        ### sqlite returns str
        if db_time is None: return None
        elif isinstance(db_time, str):
            from meerschaum.utils.misc import attempt_import
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
