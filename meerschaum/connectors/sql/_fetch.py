#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the Connector fetch() method
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Union, Callable, Any

def fetch(
        self,
        pipe : meerschaum.Pipe.Pipe,
        begin : str = 'now',
        chunk_hook : Optional[Callable[[pandas.DataFrame], Any]] = None,
        debug : bool = False,
        **kw : Any
    ) -> Optional['pd.DataFrame']:
    """
    Execute the SQL definition and return a Pandas DataFrame.

    If pipe.columns['datetime'] and
        pipe.parameters['fetch']['backtrack_minutes'] are provided,
        append a `WHERE dt > begin` subquery.

    :param begin:
        Most recent datatime to search for data.
        If `backtrack_minutes` is provided, subtract `backtrack_minutes`.

    :param pipe:
        Below are the various pipe parameters available to pipe.fetch.

        pipe.columns['datetime'] : str
            Name of the datetime column for the remote table.

        pipe.parameters['fetch'] : dict
            Parameters necessary to execute a query.
            See pipe.parameters['fetch'].

            pipe.parameters['fetch']['definition'] : str
                Raw SQL query to execute to generate the pandas DataFrame.

            pipe.parameters['backtrack_minutes'] : Union[int, float]
                How many minutes before `begin` to search for data.

    :param debug: Verbosity toggle.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error
    from meerschaum.connectors.sql._tools import sql_item_name, dateadd_str
    import datetime

    if 'columns' not in pipe.parameters or 'fetch' not in pipe.parameters:
        warn(f"Parameters for '{pipe}' must include 'columns' and 'fetch'", stack=False)
        return None

    datetime = None
    if 'datetime' not in pipe.columns:
        warn(f"Missing datetime column for '{pipe}'. Will select all data instead")
    else:
        datetime = sql_item_name(pipe.get_columns('datetime'), self.flavor)

    instructions = pipe.parameters['fetch']

    try:
        definition = instructions['definition']
    except KeyError:
        error("Cannot fetch without a definition", KeyError)

    if 'order by' in definition.lower() and 'over' not in definition.lower():
        error("Cannot fetch with an ORDER clause in the definition")

    da = None
    if datetime:
        ### default: do not backtrack
        btm = 0
        if 'backtrack_minutes' in instructions:
            btm = instructions['backtrack_minutes']
        da = dateadd_str(flavor=self.flavor, datepart='minute', number=(-1 * btm), begin=begin)

    meta_def = f"WITH definition AS ({definition}) SELECT DISTINCT * FROM definition"
    if datetime and da:
        meta_def += f"\nWHERE {datetime} > {da}"

    df = self.read(meta_def, chunk_hook=chunk_hook, debug=debug)
    ### if sqlite, parse for datetimes
    if self.flavor == 'sqlite':
        from meerschaum.utils.misc import parse_df_datetimes
        df = parse_df_datetimes(df, debug=debug)
    return df
