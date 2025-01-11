#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the Connector fetch() method
"""

from __future__ import annotations

from datetime import datetime, timedelta

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Union, Any, List, Dict


def fetch(
    self,
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, str, None] = '',
    end: Union[datetime, int, str, None] = None,
    check_existing: bool = True,
    chunksize: Optional[int] = -1,
    workers: Optional[int] = None,
    debug: bool = False,
    **kw: Any
) -> Union['pd.DataFrame', List[Any], None]:
    """Execute the SQL definition and return a Pandas DataFrame.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe object which contains the `fetch` metadata.

        - pipe.columns['datetime']: str
            - Name of the datetime column for the remote table.
        - pipe.parameters['fetch']: Dict[str, Any]
            - Parameters necessary to execute a query.
        - pipe.parameters['fetch']['definition']: str
            - Raw SQL query to execute to generate the pandas DataFrame.
        - pipe.parameters['fetch']['backtrack_minutes']: Union[int, float]
            - How many minutes before `begin` to search for data (*optional*).

    begin: Union[datetime, int, str, None], default None
        Most recent datatime to search for data.
        If `backtrack_minutes` is provided, subtract `backtrack_minutes`.

    end: Union[datetime, int, str, None], default None
        The latest datetime to search for data.
        If `end` is `None`, do not bound 

    check_existing: bool, defult True
        If `False`, use a backtrack interval of 0 minutes.

    chunksize: Optional[int], default -1
        How many rows to load into memory at once.
        Otherwise the entire result set is loaded into memory.

    workers: Optional[int], default None
        How many threads to use when consuming the generator.
        Defaults to the number of cores.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A pandas DataFrame generator.
    """
    meta_def = self.get_pipe_metadef(
        pipe,
        begin=begin,
        end=end,
        check_existing=check_existing,
        debug=debug,
        **kw
    )
    chunks = self.read(
        meta_def,
        chunksize=chunksize,
        workers=workers,
        as_iterator=True,
        debug=debug,
    )
    return chunks


def get_pipe_metadef(
    self,
    pipe: mrsm.Pipe,
    params: Optional[Dict[str, Any]] = None,
    begin: Union[datetime, int, str, None] = '',
    end: Union[datetime, int, str, None] = None,
    check_existing: bool = True,
    debug: bool = False,
    **kw: Any
) -> Union[str, None]:
    """
    Return a pipe's meta definition fetch query.

    params: Optional[Dict[str, Any]], default None
        Optional params dictionary to build the `WHERE` clause.
        See `meerschaum.utils.sql.build_where`.

    begin: Union[datetime, int, str, None], default None
        Most recent datatime to search for data.
        If `backtrack_minutes` is provided, subtract `backtrack_minutes`.

    end: Union[datetime, int, str, None], default None
        The latest datetime to search for data.
        If `end` is `None`, do not bound 

    check_existing: bool, default True
        If `True`, apply the backtrack interval.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A pipe's meta definition fetch query string.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.sql import sql_item_name, dateadd_str, build_where
    from meerschaum.utils.dtypes.sql import get_db_type_from_pd_type
    from meerschaum.config import get_config

    dt_col = pipe.columns.get('datetime', None)
    if not dt_col:
        dt_col = pipe.guess_datetime()
        dt_name = sql_item_name(dt_col, self.flavor, None) if dt_col else None
        is_guess = True
    else:
        dt_name = sql_item_name(dt_col, self.flavor, None)
        is_guess = False
    dt_typ = pipe.dtypes.get(dt_col, 'datetime') if dt_col else None
    db_dt_typ = get_db_type_from_pd_type(dt_typ, self.flavor) if dt_typ else None

    if begin not in (None, '') or end is not None:
        if is_guess:
            if dt_col is None:
                warn(
                    f"Unable to determine a datetime column for {pipe}."
                    + "\n    Ignoring begin and end...",
                    stack=False,
                )
                begin, end = '', None
            else:
                warn(
                    f"A datetime wasn't specified for {pipe}.\n"
                    + f"    Using column \"{dt_col}\" for datetime bounds...",
                    stack=False
                )

    apply_backtrack = begin == '' and check_existing
    backtrack_interval = pipe.get_backtrack_interval(check_existing=check_existing, debug=debug)
    btm = (
        int(backtrack_interval.total_seconds() / 60)
        if isinstance(backtrack_interval, timedelta)
        else backtrack_interval
    )
    begin = (
        pipe.get_sync_time(debug=debug)
        if begin == ''
        else begin
    )

    if begin not in (None, '') and end is not None and begin >= end:
        begin = None

    if dt_name:
        begin_da = (
            dateadd_str(
                flavor=self.flavor,
                datepart='minute',
                number=((-1 * btm) if apply_backtrack else 0),
                begin=begin,
                db_type=db_dt_typ,
            )
            if begin not in ('', None)
            else None
        )
        end_da = (
            dateadd_str(
                flavor=self.flavor,
                datepart='minute',
                number=0,
                begin=end,
                db_type=db_dt_typ,
            )
            if end is not None
            else None
        )

    definition_name = sql_item_name('definition', self.flavor, None)
    meta_def = (
        _simple_fetch_query(pipe, self.flavor) if (
            (not (pipe.columns or {}).get('id', None))
            or (not get_config('system', 'experimental', 'join_fetch'))
        ) else _join_fetch_query(pipe, self.flavor, debug=debug, **kw)
    )

    has_where = 'where' in meta_def.lower()[meta_def.lower().rfind('definition'):]
    if dt_name and (begin_da or end_da):
        definition_dt_name = f"{definition_name}.{dt_name}"
        meta_def += "\n" + ("AND" if has_where else "WHERE") + " "
        has_where = True
        if begin_da:
            meta_def += f"\n    {definition_dt_name}\n    >=\n    {begin_da}\n"
        if begin_da and end_da:
            meta_def += "    AND"
        if end_da:
            meta_def += f"\n    {definition_dt_name}\n    <\n    {end_da}\n"

    if params is not None:
        params_where = build_where(params, self, with_where=False)
        meta_def += "\n    " + ("AND" if has_where else "WHERE") + "    "
        has_where = True
        meta_def += params_where

    return meta_def.rstrip()


def get_pipe_query(pipe: mrsm.Pipe, warn: bool = True) -> Union[str, None]:
    """
    Run through the possible keys for a pipe's query and return the first match.

    - fetch, definition
    - definition
    - query
    - sql
    """
    import re
    import textwrap
    from meerschaum.utils.warnings import warn as _warn
    from meerschaum.utils.misc import parse_arguments_str
    from meerschaum.utils.sql import sql_item_name
    if pipe.parameters.get('fetch', {}).get('definition', None):
        definition = pipe.parameters['fetch']['definition']
    elif pipe.parameters.get('definition', None):
        definition = pipe.parameters['definition']
    elif pipe.parameters.get('query', None):
        definition = pipe.parameters['query']
    elif pipe.parameters.get('sql', None):
        definition = pipe.parameters['sql']
    else:
        if warn:
            _warn(
                f"Could not determine a SQL definition for {pipe}.\n"
                + "    Set the key `query` in `pipe.parameters` to a valid SQL query."
            )
        return None

    def replace_pipe_match(pipe_match):
        try:
            args_str = pipe_match.group(1)
            args, kwargs = parse_arguments_str(args_str)
            pipe = mrsm.Pipe(*args, **kwargs)
        except Exception as e:
            if warn:
                _warn(f"Failed to parse pipe from SQL definition:\n{e}")
            raise e

        target = pipe.target
        schema = pipe.instance_connector.get_pipe_schema(pipe)
        return sql_item_name(target, pipe.instance_connector.flavor, schema)

    definition = re.sub(r'\{\{Pipe\((.*?)\)\}\}', replace_pipe_match, definition)
    return textwrap.dedent(definition.lstrip().rstrip())


def set_pipe_query(pipe: mrsm.Pipe, query: str) -> None:
    """
    Run through the possible keys for a pipe's query and set the first match.

    - fetch, definition
    - definition
    - query
    - sql
    """
    if 'fetch' in pipe.parameters and 'definition' in pipe.parameters['fetch']:
        if pipe.parameters.get('fetch', None) is None:
            pipe.parameters['fetch'] = {}
        dict_to_set = pipe.parameters['fetch']
        key_to_set = 'definition'
    elif 'definition' in pipe.parameters:
        dict_to_set = pipe.parameters
        key_to_set = 'definition'
    elif 'query' in pipe.parameters:
        dict_to_set = pipe.parameters
        key_to_set = 'query'
    else:
        dict_to_set = pipe.parameters
        key_to_set = 'sql'

    dict_to_set[key_to_set] = query


def _simple_fetch_query(
    pipe: mrsm.Pipe,
    flavor: str,
    debug: bool = False,
    **kw
) -> str:
    """Build a fetch query from a pipe's definition."""
    from meerschaum.utils.sql import format_cte_subquery
    definition = get_pipe_query(pipe)
    if definition is None:
        raise ValueError(f"No SQL definition could be found for {pipe}.")
    return format_cte_subquery(definition, flavor, 'definition')


def _join_fetch_query(
    pipe: mrsm.Pipe,
    flavor: str,
    debug: bool = False,
    new_ids: bool = True,
    **kw
) -> str:
    """Build a fetch query based on the datetime and ID indices."""
    if not pipe.exists(debug=debug):
        return _simple_fetch_query(pipe, flavor, debug=debug, **kw)

    from meerschaum.utils.sql import sql_item_name, dateadd_str
    pipe_instance_name = sql_item_name(
        pipe.target, pipe.instance_connector.flavor, pipe.instance_connector.schema
    )
    sync_times_table = pipe.target + "_sync_times"
    sync_times_remote_name = sql_item_name(
        sync_times_table, pipe.connector.flavor, None
    )
    id_instance_name = sql_item_name(
        pipe.columns['id'], pipe.instance_connector.flavor, None
    )
    id_remote_name = sql_item_name(pipe.columns['id'], pipe.connector.flavor, None)
    dt_instance_name = sql_item_name(
        pipe.columns['datetime'], pipe.instance_connector.flavor, None
    )
    dt_remote_name = sql_item_name(
        pipe.columns['datetime'], pipe.connector.flavor, None
    )
    cols_types = pipe.get_columns_types(debug=debug)
    sync_times_query = f"""
    SELECT {id_instance_name}, MAX({dt_instance_name}) AS {dt_instance_name}
    FROM {pipe_instance_name}
    GROUP BY {id_instance_name}
    """
    sync_times = pipe.instance_connector.read(sync_times_query, debug=debug, silent=False)
    if sync_times is None:
        return _simple_fetch_query(pipe, flavor, debug=debug, **kw)

    _sync_times_q = f",\n{sync_times_remote_name} AS ("
    for _id, _st in sync_times.itertuples(index=False):
        _sync_times_q += (
            f"SELECT CAST('{_id}' AS "
            + sql_item_name(
                cols_types[pipe.columns['id']], pipe.connector.flavor, None
            )
            + f") AS {id_remote_name}, "
            + dateadd_str(
                flavor = pipe.connector.flavor,
                begin = _st,
                datepart = 'minute',
                number = pipe.parameters.get('fetch', {}).get('backtrack_minutes', 0)
            ) + " AS " + dt_remote_name + "\nUNION ALL\n"
        )
    _sync_times_q = _sync_times_q[:(-1 * len('UNION ALL\n'))] + ")"

    definition = get_pipe_query(pipe)
    query = (
        f"""
    WITH definition AS (\n{definition}\n){_sync_times_q}
    SELECT definition.*
    FROM definition"""
        if pipe.connector.flavor not in ('mysql', 'mariadb')
        else (
        f"""
    SELECT * FROM (\n{definition}\n) AS definition"""
        )
    ) + f"""
    LEFT OUTER JOIN {sync_times_remote_name} AS st
      ON st.{id_remote_name} = definition.{id_remote_name}
    WHERE definition.{dt_remote_name} > st.{dt_remote_name}
    """ + (f"  OR st.{id_remote_name} IS NULL" if new_ids else "")
    return query
