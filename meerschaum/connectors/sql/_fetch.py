#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the Connector fetch() method
"""

import datetime
from dateutil import parser
from meerschaum.utils.debug import dprint

def dateadd_str(
        flavor : str = 'postgres',
        datepart : str = 'day',
        number : float = -1,
        begin : 'str or datetime.datetime' = 'now'
    ) -> str:
    """
    Generate a DATEADD clause depending on flavor
    """
    if not begin: return None
    begin_time = None
    if not isinstance(begin, datetime.datetime):
        try:
            begin_time = parser.parse(begin)
        except Exception:
            begin_time = None
    else: begin_time = begin

    da = ""
    if flavor in ('postgres', 'timescaledb'):
        if begin == 'now': begin = "CAST(NOW() AT TIME ZONE 'utc' AS TIMESTAMP)"
        elif begin_time: begin = f"CAST('{begin}' AS TIMESTAMP)"
        da = begin + f" + INTERVAL '{number} {datepart}'"
    elif flavor in ('mssql'):
        if begin == 'now': begin = "GETUTCDATE()"
        elif begin_time: begin = f"CAST('{begin}' AS DATETIME)"
        da = f"DATEADD({datepart}, {number}, {begin})"
    elif flavor in ('mysql'):
        if begin == 'now': begin = "UTC_TIMESTAMP()"
        elif begin_time: begin = f'"{begin}"'
        da = f"DATE_ADD({begin}, INTERVAL {number} {datepart})"
    elif flavor == 'sqlite':
        da = f"datetime('{begin}', '{number} {datepart}')"
    ### TODO figure out oracle's TO_DATE syntax
    #  elif flavor == 'oracle':
        #  if begin == 'now': begin
    return da

def fetch(
        self,
        pipe : 'meerschaum.Pipe',
        begin : str = 'now',
        debug : bool = False
    ) -> 'pd.DataFrame':
    """
    Execute the SQL definition and if datetime and backtrack_minutes are provided, append a
        `WHERE dt > begin` subquery.

    begin : str : 'now'
        Most recent datatime to search for data. If backtrack_minutes is provided, subtract backtrack_minutes


    pipe : Pipe
        parameters:fetch : dict
            Parameters necessary to execute a query. See pipe.parameters['fetch']

            Keys:
                definition : str
                    base SQL query to execute

                datetime : str
                    name of the datetime column for the remote table

                backtrack_minutes : int or float
                    how many minutes before `begin` to search for data
                    
    Returns pandas dataframe of the selected data.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn

    if 'columns' not in pipe.parameters or 'fetch' not in pipe.parameters:
        warn(f"Parameters for '{pipe}' must include 'columns' and 'fetch'")
        return None

    datetime = None
    if 'datetime' not in pipe.columns:
        warn(f"Missing datetime column for '{pipe}'. Will select all data instead")
    else: datetime = pipe.columns['datetime']

    instructions = pipe.parameters['fetch']

    try:
        definition = instructions['definition']
    except KeyError:
        raise KeyError("Cannot fetch without a definition")

    if 'order by' in definition.lower():
        raise Exception("Cannot fetch with an ORDER clause in the definition")

    da = None
    if datetime:
        if 'backtrack_minutes' in instructions:
            btm = instructions['backtrack_minutes']
            da = dateadd_str(flavor=self.flavor, datepart='minute', number=(-1 * btm), begin=begin)

    meta_def = f"WITH definition AS ({definition}) SELECT * FROM definition"
    if datetime and da:
        meta_def += f"\nWHERE {datetime} > {da}"

    if debug: dprint(meta_def)

    df = self.read(meta_def)
    ### if sqlite, parse for datetimes
    if self.flavor == 'sqlite':
        from meerschaum.utils.misc import parse_df_datetimes
        return parse_df_datetimes(df, debug=debug)
    return df

