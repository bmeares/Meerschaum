#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the Connector fetch() method
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Union, Callable, Any

### NOTE: begin is set in pipe.sync().

def fetch(
        self,
        pipe: meerschaum.Pipe,
        begin: Union[datetime.datetime, str, None] = '',
        end: Union[datetime.datetime, str, None] = None,
        chunk_hook: Optional[Callable[[pd.DataFrame], Any]] = None,
        chunksize: Optional[int] = -1,
        debug: bool = False,
        **kw: Any
    ) -> Union['pd.DataFrame', None]:
    """Execute the SQL definition and return a Pandas DataFrame.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe object which contains the `fetch` metadata.
        
        - pipe.columns['datetime']: str
            - Name of the datetime column for the remote table.
        - pipe.parameters['fetch']: Dict[str, Any]
            - Parameters necessary to execute a query.
        - pipe.parameters['fetch']['definition']: str
            - Raw SQL query to execute to generate the pandas DataFrame.
        - pipe.parameters['fetch']['backtrack_minutes']: Union[int, float]
            - How many minutes before `begin` to search for data (*optional*).

    begin: Union[datetime.datetime, str, None], default None
        Most recent datatime to search for data.
        If `backtrack_minutes` is provided, subtract `backtrack_minutes`.

    end: Union[datetime.datetime, str, None], default None
        The latest datetime to search for data.
        If `end` is `None`, do not bound 

    debug: bool, default False
        Verbosity toggle.
       
    Returns
    -------
    A pandas DataFrame or `None`.

    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error
    from meerschaum.utils.sql import sql_item_name, dateadd_str
    from meerschaum.config import get_config
    import datetime

    if 'columns' not in pipe.parameters or 'fetch' not in pipe.parameters:
        warn(f"Parameters for '{pipe}' must include 'columns' and 'fetch'", stack=False)
        return None

    dt_name = None
    if 'datetime' not in pipe.columns:
        warn(f"Missing datetime column for '{pipe}'. Will select all data instead.")
    else:
        dt_name = sql_item_name(pipe.get_columns('datetime'), self.flavor)


    instructions = pipe.parameters['fetch']

    try:
        definition = instructions['definition']
    except KeyError:
        error("Cannot fetch without a definition.", KeyError)

    if 'order by' in definition.lower() and 'over' not in definition.lower():
        error("Cannot fetch with an ORDER clause in the definition")

    begin = (
        begin if not (isinstance(begin, str) and begin == '')
        else pipe.get_sync_time(debug=debug)
    )
        
    da = None
    if dt_name:
        ### default: do not backtrack
        btm = 0
        if 'backtrack_minutes' in instructions:
            btm = instructions['backtrack_minutes']
        begin_da = dateadd_str(
            flavor=self.flavor, datepart='minute', number=(-1 * btm), begin=begin,
        ) if begin else None
        end_da = dateadd_str(
            flavor=self.flavor, datepart='minute', number=1, begin=end,
        ) if end else None

    meta_def = (
        _simple_fetch_query(pipe) if (
            (not pipe.columns.get('id', None))
            or (not get_config('system', 'experimental', 'join_fetch'))
        ) else _join_fetch_query(pipe, debug=debug, **kw)
    )

    if dt_name and (begin_da or end_da):
        definition_dt_name = dateadd_str(self.flavor, 'minute', 0, f"definition.{dt_name}")
        meta_def += "\n" + (
            "AND" if 'where' in meta_def.lower()[meta_def.lower().rfind('select'):]
            else "WHERE"
        ) + " "
        if begin_da:
            meta_def += f"{definition_dt_name} >= {begin_da}"
        if begin_da and end_da:
            meta_def += " AND "
        if end_da:
            meta_def += f"{definition_dt_name} < {end_da}"

    df = self.read(meta_def, chunk_hook=chunk_hook, chunksize=chunksize, debug=debug)
    ### if sqlite, parse for datetimes
    if self.flavor == 'sqlite':
        from meerschaum.utils.misc import parse_df_datetimes
        df = parse_df_datetimes(df, debug=debug)
    return df

def _simple_fetch_query(pipe, debug: bool=False, **kw) -> str:
    """Build a fetch query from a pipe's definition."""
    definition = pipe.parameters['fetch']['definition']
    return f"WITH definition AS ({definition}) SELECT * FROM definition"

def _join_fetch_query(
        pipe,
        debug: bool = False,
        new_ids: bool = True,
        **kw
    ) -> str:
    """Build a fetch query based on the datetime and ID indices."""
    if not pipe.exists(debug=debug):
        return _simple_fetch_query(pipe, debug=debug, **kw)

    from meerschaum.utils.sql import sql_item_name, dateadd_str
    pipe_instance_name = sql_item_name(pipe.target, pipe.instance_connector.flavor)
    #  pipe_remote_name = sql_item_name(pipe.target, pipe.connector.flavor)
    sync_times_table = pipe.target + "_sync_times"
    sync_times_instance_name = sql_item_name(sync_times_table, pipe.instance_connector.flavor)
    sync_times_remote_name = sql_item_name(sync_times_table, pipe.connector.flavor)
    id_instance_name = sql_item_name(pipe.columns['id'], pipe.instance_connector.flavor)
    id_remote_name = sql_item_name(pipe.columns['id'], pipe.connector.flavor)
    dt_instance_name = sql_item_name(pipe.columns['datetime'], pipe.connector.flavor)
    dt_remote_name = sql_item_name(pipe.columns['datetime'], pipe.instance_connector.flavor)
    cols_types = pipe.get_columns_types(debug=debug)
    sync_times_query = f"""
    SELECT {id_instance_name}, MAX({dt_instance_name}) AS {dt_instance_name}
    FROM {pipe_instance_name}
    GROUP BY {id_instance_name}
    """
    sync_times = pipe.instance_connector.read(sync_times_query, debug=debug, silent=False)
    if sync_times is None:
        return _simple_fetch_query(pipe, debug=debug, **kw)
    _sync_times_q = f",\n{sync_times_remote_name} AS ("
    for _id, _st in sync_times.itertuples(index=False):
        _sync_times_q += (
            f"SELECT CAST('{_id}' AS "
            + sql_item_name(cols_types[pipe.columns['id']], pipe.connector.flavor)
            + f") AS {id_remote_name}, "
            + dateadd_str(
                flavor=pipe.connector.flavor,
                begin=_st,
                datepart='minute',
                number=pipe.parameters.get('fetch', {}).get('backtrack_minutes', 0)
            ) + " AS " + dt_remote_name + "\nUNION ALL\n"
        )
    _sync_times_q = _sync_times_q[:(-1 * len('UNION ALL\n'))] + ")"

    definition = pipe.parameters['fetch']['definition']
    query = f"""
    WITH definition AS ({definition}){_sync_times_q}
    SELECT definition.*
    FROM definition
    LEFT OUTER JOIN {sync_times_remote_name} AS st
      ON st.{id_remote_name} = definition.{id_remote_name}
    WHERE definition.{dt_remote_name} > st.{dt_remote_name}
    """ + (f"  OR st.{id_remote_name} IS NULL" if new_ids else "")
    return query

