#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the Connector fetch() method
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Union, Callable, Any

def dateadd_str(
        flavor : str = 'postgresql',
        datepart : str = 'day',
        number : Union[int, float] = -1,
        begin : Union[str, datetime.datetime] = 'now'
    ) -> str:
    """
    Generate a DATEADD clause depending on database flavor.
    This function is pretty fragile / complex, so I may depreciate
    it in favor of a pure-Python or ORM solution.

    :param flavor:
        SQL database flavor, e.g. postgresql, sqlite.
        Currently supported flavors:
        - postgresql
        - timescaledb
        - mssql
        - mysql
        - mariadb
        - sqlite
        - oracle

    :param datepart:
        Which part of the date to modify. Supported values* (*AFAIK).
        - year
        - month
        - day
        - hour
        - minute
        - second

    :param number:
        How many units to add to the date part.

    :param begin:
        Base datetime to which to add dateparts.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    import datetime
    dateutil = attempt_import('dateutil')
    if not begin: return None
    begin_time = None
    if not isinstance(begin, datetime.datetime):
        try:
            begin_time = dateutil.parser.parse(begin)
        except Exception:
            begin_time = None
    else: begin_time = begin

    da = ""
    if flavor in ('postgresql', 'timescaledb'):
        if begin == 'now':
            begin = "CAST(NOW() AT TIME ZONE 'utc' AS TIMESTAMP)"
        elif begin_time:
            begin = f"CAST('{begin}' AS TIMESTAMP)"
        da = begin + f" + INTERVAL '{number} {datepart}'"
    elif flavor in ('mssql',):
        if begin == 'now':
            begin = "GETUTCDATE()"
        elif begin_time:
            begin = f"CAST('{begin}' AS DATETIME)"
        da = f"DATEADD({datepart}, {number}, {begin})"
    elif flavor in ('mysql', 'mariadb'):
        if begin == 'now':
            begin = "UTC_TIMESTAMP()"
        elif begin_time:
            begin = f'"{begin}"'
        da = f"DATE_ADD({begin}, INTERVAL {number} {datepart})"
    elif flavor == 'sqlite':
        da = f"datetime('{begin}', '{number} {datepart}')"
    elif flavor == 'oracle':
        if begin == 'now':
            begin = str(
                datetime.datetime.utcnow().strftime('%Y:%m:%d %M:%S.%f')
            )
        elif begin_time:
            begin = str(begin.strftime('%Y:%m:%d %M:%S.%f'))
        dt_format = 'YYYY-MM-DD HH24:MI:SS.FF'
        da = f"TO_TIMESTAMP('{begin}', '{dt_format}') + INTERVAL '{number}' {datepart}"
    return da

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
    from meerschaum.utils.misc import sql_item_name
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
