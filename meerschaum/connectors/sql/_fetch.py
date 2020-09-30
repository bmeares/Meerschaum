#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the Connector fetch() method
"""

#  flavor_dateadds = {
    #  'postgres'  : 
#  }

import datetime
from dateutil import parser

def dateadd_str(
        flavor : str = 'postgres',
        datepart : str = 'day',
        number : float = -1,
        begin : str = 'now'
    ):
    begin_time = None
    try:
        begin_time = parser.parse(begin)
    except Exception:
        pass

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
    return da

def fetch(
        self,
        instructions : dict,
        begin : str = 'now',
        debug : bool = False
    ) -> 'pd.DataFrame':
    from meerschaum.utils.debug import dprint
    try:
        definition = instructions['definition']
    except KeyError:
        raise KeyError("Cannot fetch without a definition")

    if 'order by' in definition.lower():
        raise Exception("Cannot fetch with an ORDER clause in the definition")

    datetime, da = None, None
    if 'datetime' in instructions:
        datetime = instructions['datetime']
        if 'backtrack_minutes' in instructions:
            btm = instructions['backtrack_minutes']
            ### TODO change begin from max to newest pipe data
            da = dateadd_str(flavor=self.flavor, datepart='minute', number=(-1 * btm), begin=begin)

    meta_def = f"""WITH definition AS ({definition}) SELECT * FROM definition"""
    if datetime and da:
        meta_def += f"\nWHERE {datetime} > {da}"

    if debug: print(meta_def)

    return self.read(meta_def)
