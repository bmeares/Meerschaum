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
        end : Optional[Union[datetime.datetime, str]] = None,
        chunk_hook : Optional[Callable[[pandas.DataFrame], Any]] = None,
        chunksize : Optional[int] = -1,
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

    :param end:
        The latest datetime to search for data.
        If `end` is `None`, perform an unbounded search.

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
        begin_da = dateadd_str(
            flavor=self.flavor, datepart='minute', number=(-1 * btm), begin=begin,
        )
        end_da = dateadd_str(
            flavor=self.flavor, datepart='minute', number=1, begin=end,
        ) if end else None

    meta_def = f"WITH definition AS ({definition}) SELECT DISTINCT * FROM definition"
    if datetime and (begin_da or end_da):
        meta_def += "\nWHERE "
        if begin_da:
            meta_def += f"{datetime} >= {begin_da}"
        if begin_da and end_da:
            meta_def += " AND "
        if end_da:
            meta_def += f"{datetime} <= {end_da}"

    df = self.read(meta_def, chunk_hook=chunk_hook, chunksize=chunksize, debug=debug)
    ### if sqlite, parse for datetimes
    if self.flavor == 'sqlite':
        from meerschaum.utils.misc import parse_df_datetimes
        df = parse_df_datetimes(df, debug=debug)
    return df
