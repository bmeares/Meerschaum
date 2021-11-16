#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Flavor-specific SQL tools.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any, Union

test_queries = {
    'default' : 'SELECT 1',
    'oracle' : 'SELECT 1 FROM DUAL',
    'informix' : 'SELECT COUNT(*) FROM systables',
    'hsqldb' : 'SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS',
}
table_wrappers = {
    'timescaledb': ('"', '"'),
    'duckdb': ('"', '"'),
    'postgresql': ('"', '"'),
    'sqlite': ('"', '"'),
    'mysql': ('`', '`'),
    'mariadb': ('`', '`'),
    'mssql': ('[', ']'),
    'cockroachdb': ('"', '"'),
    'oracle': ('"', '"'),
}

json_flavors = {'postgresql', 'timescaledb',}

def test_connection(
        self,
        **kw : Any
    ) -> Union[bool, None]:
    """
    Test if a successful connection to the database may be made.
    """
    import warnings
    from meerschaum.utils.misc import retry_connect
    _default_kw = {'max_retries' : 1, 'retry_wait' : 0, 'warn': False, 'connector': self}
    _default_kw.update(kw)
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', 'Could not')
        try:
            return retry_connect(**_default_kw)
        except Exception as e:
            return False

def get_distinct_col_count(
        col : str,
        query : str,
        connector : Optional[meerschaum.connectors.sql.SQLConnector] = None,
        debug : bool = False
    ) -> Optional[int]:
    """
    Returns the number of distinct items in a column of a SQL query.

    :param col:
        The column in the query to count.

    :param query:
        The SQL query to count from.

    :param connector:
        The SQLConnector to execute the query.
    """
    
    if connector is None:
        from meerschaum import get_connector
        connector = get_connector('sql')

    _col_name = sql_item_name(col, connector.flavor)

    _meta_query = f"""
    WITH src AS ( {query} ),
    dist AS ( SELECT DISTINCT {_col_name} FROM src )
    SELECT COUNT(*) FROM dist"""

    result = connector.value(_meta_query, debug=debug)
    try:
        return int(result)
    except Exception as e:
        return None

def sql_item_name(s : str, flavor : str) -> str:
    """
    Parse SQL items depending on the flavor.
    """
    if flavor not in table_wrappers:
        raise Exception(f"Invalid flavor '{flavor}'.")

    return table_wrappers[flavor][0] + str(s) + table_wrappers[flavor][1]

def pg_capital(s : str) -> str:
    """
    If string contains a capital letter, wrap it in double quotes

    returns: string
    """
    if '"' in s:
        return s
    needs_quotes = False
    for c in str(s):
        if ord(c) < ord('a') or ord(c) > ord('z'):
            if not c.isdigit():
                needs_quotes = True
                break
    if needs_quotes:
        return '"' + s + '"'
    return s

def build_where(
        parameters : Dict[str, Any],
        connector : Optional[meerschaum.connectors.sql.SQLConnector] = None
    ) -> str:
    """
    Build the WHERE clause based on the input criteria.
    """
    if connector is None:
        from meerschaum import get_connector
        connector = get_connector('sql')
    where = ""
    leading_and = "\n    AND "
    for key, value in parameters.items():
        _key = sql_item_name(key, connector.flavor)
        ### search across a list (i.e. IN syntax)
        if isinstance(value, list):
            where += f"{leading_and}{_key} IN ("
            for item in value:
                where += f"'{item}', "
            where = where[:-2] + ")"
            continue

        ### search a dictionary
        elif isinstance(value, dict):
            import json
            where += (f"{leading_and}CAST({_key} AS TEXT) = '" + json.dumps(value) + "'")
            continue

        where += f"{leading_and}{_key} " + ("IS NULL" if value is None else f"= '{value}'")
    if len(where) > 1:
        where = "\nWHERE\n    " + where[len(leading_and):]
    return where

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
        - cockroachdb
        - duckdb
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
    from meerschaum.utils.warnings import error
    import datetime
    dateutil = attempt_import('dateutil')
    if not begin:
        return None
    _original_begin = begin
    begin_time = None
    ### Sanity check: make sure `begin` is a valid datetime before we inject anything.
    if not isinstance(begin, datetime.datetime):
        try:
            begin_time = dateutil.parser.parse(begin)
        except Exception:
            begin_time = None
    else:
        begin_time = begin

    ### Unable to parse into a datetime.
    if begin_time is None:
        ### Throw an error if any of these banned symbols are included in the `begin` string.
        banned_symbols = [';', '--', 'drop', 'create', 'alter', 'delete', 'commit']
        for symbol in banned_symbols:
            if symbol in str(begin).lower():
                error(f"Invalid datetime: '{begin}'")
    ### If begin is a valid datetime, wrap it in quotes.
    else:
        begin = f"'{begin}'"

    da = ""
    if flavor in ('postgresql', 'timescaledb', 'cockroachdb'):
        begin = (
            f"CAST({begin} AS TIMESTAMP)" if begin != 'now'
            else "CAST(NOW() AT TIME ZONE 'utc' AS TIMESTAMP)"
        )
        da = begin + (f" + INTERVAL '{number} {datepart}'" if number != 0 else '')
    elif flavor == 'duckdb':
        begin = f"CAST({begin} AS TIMESTAMP)" if begin != 'now' else 'NOW()'
        da = begin + (f" + INTERVAL '{number} {datepart}'" if number != 0 else '')
    elif flavor in ('mssql',):
        begin = f"CAST({begin} AS DATETIME)" if begin != 'now' else 'GETUTCDATE()'
        da = f"DATEADD({datepart}, {number}, {begin})" if number != 0 else begin
    elif flavor in ('mysql', 'mariadb'):
        begin = f"CAST({begin} AS DATETIME(6))" if begin != 'now' else 'UTC_TIMESTAMP(6)'
        da = (f"DATE_ADD({begin}, INTERVAL {number} {datepart})" if number != 0 else begin)
    elif flavor == 'sqlite':
        da = f"datetime({begin}, '{number} {datepart}')"
    elif flavor == 'oracle':
        if begin == 'now':
            begin = str(
                datetime.datetime.utcnow().strftime('%Y:%m:%d %M:%S.%f')
            )
        elif begin_time:
            begin = str(begin_time.strftime('%Y:%m:%d %M:%S.%f'))
        dt_format = 'YYYY-MM-DD HH24:MI:SS.FF'
        da = f"TO_TIMESTAMP('{begin}', '{dt_format}') + INTERVAL '{number}' {datepart}"
    return da

def table_exists(
        table: str,
        connector: Optional[meerschaum.connectors.sql.SQLConnector] = None,
        debug: bool = False,
    ) -> bool:
    """
    Check if a table exists.
    """
    if connector is None:
        from meerschaum import get_connector
        connector = get_connector('sql')

    table_name = sql_item_name(table, connector.flavor)
    ### default: select no rows. NOTE: this might not work for Oracle
    q = f"SELECT COUNT(*) FROM {table_name} WHERE 1 = 0"
    if connector.flavor in ('timescaledb', 'postgresql'):
        q = f"SELECT to_regclass('{table_name}')"
    elif connector.flavor == 'mssql':
        q = f"SELECT OBJECT_ID('{table_name}')"
    elif connector.flavor in ('mysql', 'mariadb'):
        q = f"SHOW TABLES LIKE '{table}'"
    elif connector.flavor == 'sqlite':
        q = f"SELECT name FROM sqlite_master WHERE name='{table}'"
    exists = connector.value(q, debug=debug, silent=True) is not None
    return exists

def get_sqlalchemy_table(
        table: str,
        connector: Optional[meerschaum.connectors.sql.SQLConnector] = None,
        debug: bool = False,
    ) -> sqlalchemy.Table:
    """
    Return a sqlalchemy Table bound to a SQLConnector's engine.
    """
    if connector is None:
        from meerschaum import get_connector
        connector = get_connector('sql')

    from meerschaum.connectors.sql.tables import get_tables
    from meerschaum.utils.packages import attempt_import
    tables = get_tables(mrsm_instance=connector, debug=debug)
    sqlalchemy = attempt_import('sqlalchemy')
    if str(table) not in tables:
        tables[str(table)] = sqlalchemy.Table(
            str(table),
            connector.metadata,
            autoload_with = connector.engine
        )
    return tables[str(table)]

