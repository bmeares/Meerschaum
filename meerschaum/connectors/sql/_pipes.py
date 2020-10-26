#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with Pipes metadata via SQLConnector

NOTE: These methods will only work for connectors which correspond to an
      existing Meerschaum database. Use with caution!
"""

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

    ### creates metadata
    from meerschaum.api.tables import get_tables
    tables = get_tables()

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
    from meerschaum.utils.misc import pg_capital
    index_queries = []

    ### create datetime index
    if 'datetime' in pipe.columns and pipe.columns['datetime']:
        if self.flavor == 'timescaledb':
            ## create hypertable
            dt_query = (
                f"SELECT create_hypertable('{pg_capital(str(pipe))}', " +
                f"'{pg_capital(pipe.columns['datetime'])}', migrate_data => true);"
            )
        elif self.flavor == 'postgresql':
            dt_query = f"CREATE INDEX ON {pg_capital(str(pipe))} ({pg_capital(pipe.columns['datetime'])})"
        elif self.flavor in ('mysql', 'mariadb'):
            dt_query = f"CREATE INDEX ON {pipe} ({pipe.columns['datetime']})"
        else: ### mssql, sqlite, etc.
            dt_query = f"CREATE INDEX {pipe.columns['datetime']}_index ON {pipe} ({pipe.columns['datetime']})"
            
        index_queries.append(dt_query)

    ### create id index
    if 'id' in pipe.columns and pipe.columns['id']:
        if self.flavor in ('timescaledb', 'postgresql'):
            id_query = f"CREATE INDEX ON {pg_capital(str(pipe))} ({pg_capital(pipe.columns['id'])})"
        elif self.flavor in ('mysql', 'mariadb'):
            id_query = f"CREATE INDEX ON {pipe} ({pipe.columns['id']})"
        else: ### mssql, sqlite, etc.
            id_query = f"CREATE INDEX {pipe.columns['id']}_index ON {pipe} ({pipe.columns['id']})"

        index_queries.append(id_query)

    for q in index_queries:
        if debug: dprint(q)
        if not self.exec(q, debug=debug):
            return None
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
    from meerschaum.utils.misc import pg_capital
    from meerschaum.utils.debug import dprint
    pipe_name = str(pipe)
    if self.flavor in ('postgresql', 'timescaledb'):
        pipe_name = pg_capital(pipe_name)
    if not pipe.id:
        return False, f"Pipe '{pipe}' is not registered"
    q = f"DELETE FROM pipes WHERE pipe_id = {pipe.id}"
    if not self.exec(q):
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
    da = dateadd_str(
        flavor = self.flavor,
        datepart = 'minute',
        number = (-1 * backtrack_minutes),
        begin = begin
    )

    query = f"SELECT * FROM {pipe}" + (f" WHERE {pipe.get_columns('datetime')} > {da}" if da else "")
    if debug: dprint(query)

    return self.read(query)

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
    from meerschaum.utils.misc import pg_capital
    from meerschaum.connectors.sql._fetch import dateadd_str
    query = f"SELECT * FROM {pipe}"
    where = ""

    dt = pipe.get_columns('datetime')
    if self.flavor in ('postgresql', 'timescaledb'):
        dt = pg_capital(dt)

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
    return self.read(query, debug=debug)
