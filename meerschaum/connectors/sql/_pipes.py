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
    parameters = dict(params)
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
