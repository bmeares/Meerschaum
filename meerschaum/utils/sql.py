#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Flavor-specific SQL tools.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any, Union, List

test_queries = {
    'default'    : 'SELECT 1',
    'oracle'     : 'SELECT 1 FROM DUAL',
    'informix'   : 'SELECT COUNT(*) FROM systables',
    'hsqldb'     : 'SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS',
}
### `table_name` is the escaped name of the table.
### `table` is the unescaped name of the table.
exists_queries = {
    'default'    : "SELECT COUNT(*) FROM {table_name} WHERE 1 = 0",
}
update_queries = {
    'default': """
    UPDATE {target_table_name} AS f
    {sets_subquery_none}
    FROM {target_table_name} AS t
    INNER JOIN (SELECT DISTINCT * FROM {patch_table_name}) AS p
        ON {and_subquery_t}
    WHERE
        {and_subquery_f}
    """,
    'mysql': """
    UPDATE {target_table_name} AS f
    INNER JOIN (SELECT DISTINCT * FROM {patch_table_name}) AS p
        ON {and_subquery_f}
    {sets_subquery_f}
    WHERE
        {and_subquery_f}
    """,
    'mariadb': """
    UPDATE {target_table_name} AS f
    INNER JOIN (SELECT DISTINCT * FROM {patch_table_name}) AS p
        ON {and_subquery_f}
    {sets_subquery_f}
    WHERE
        {and_subquery_f}
    """,
    'mssql': """
    MERGE {target_table_name} t
        USING (SELECT DISTINCT * FROM {patch_table_name}) p
        ON {and_subquery_t}
    WHEN MATCHED THEN
        UPDATE
        {sets_subquery_none};
    """,
    'oracle': """
    MERGE INTO {target_table_name} t
        USING (SELECT DISTINCT * FROM {patch_table_name}) p
        ON (
            {and_subquery_t}
        )
    WHEN MATCHED THEN
        UPDATE
        {sets_subquery_none}
        WHERE (
            {and_subquery_t}
        )
    """,
}
table_wrappers = {
    'default'    : ('"', '"'),
    'timescaledb': ('"', '"'),
    'citus'      : ('"', '"'),
    'duckdb'     : ('"', '"'),
    'postgresql' : ('"', '"'),
    'sqlite'     : ('"', '"'),
    'mysql'      : ('`', '`'),
    'mariadb'    : ('`', '`'),
    'mssql'      : ('[', ']'),
    'cockroachdb': ('"', '"'),
    'oracle'     : ('"', '"'),
}
max_name_lens = {
    'default'    : 64,
    'mssql'      : 128,
    'oracle'     : 30,
    'postgresql' : 64,
    'timescaledb': 64,
    'citus'      : 64,
    'cockroachdb': 64,
    'sqlite'     : 1024, ### Probably more, but 1024 seems more than reasonable.
    'mysql'      : 64,
    'mariadb'    : 64,
}
json_flavors = {'postgresql', 'timescaledb', 'citus'}
OMIT_NULLSFIRST_FLAVORS = {'mariadb', 'mysql', 'mssql'}
DB_TO_PD_DTYPES = {
    'FLOAT': 'float64',
    'DOUBLE_PRECISION': 'float64',
    'DOUBLE': 'float64',
    'BIGINT': 'Int64',
    'TIMESTAMP': 'datetime64[ns]',
    'TIMESTAMP WITH TIMEZONE': 'datetime64[ns, UTC]',
    'TIMESTAMPTZ': 'datetime64[ns, UTC]',
    'DATE': 'datetime64[ns]',
    'DATETIME': 'datetime64[ns]',
    'TEXT': 'object',
    'CLOB': 'object',
    'BOOL': 'bool',
    'BOOLEAN': 'bool',
    'BOOLEAN()': 'bool',
    'substrings': {
        'CHAR': 'str',
        'TIMESTAMP': 'datetime64[ns]',
        'DATE': 'datetime64[ns]',
        'DOUBLE': 'float64',
        'INT': 'Int64',
        'BOOL': 'bool',
    },
    'default': 'object',
}
### MySQL doesn't allow for casting as BIGINT, so this is a workaround.
DB_FLAVORS_CAST_DTYPES = {
    'mariadb': {
        'BIGINT': 'DOUBLE',
    },
    'mysql': {
        'BIGINT': 'DOUBLE',
    },
}


def dateadd_str(
        flavor: str = 'postgresql',
        datepart: str = 'day',
        number: Union[int, float] = 0,
        begin: Union[str, datetime.datetime] = 'now'
    ) -> str:
    """
    Generate a `DATEADD` clause depending on database flavor.

    Parameters
    ----------
    flavor: str, default `'postgresql'`
        SQL database flavor, e.g. `'postgresql'`, `'sqlite'`.

        Currently supported flavors:

        - `'postgresql'`
        - `'timescaledb'`
        - `'citus'`
        - `'cockroachdb'`
        - `'duckdb'`
        - `'mssql'`
        - `'mysql'`
        - `'mariadb'`
        - `'sqlite'`
        - `'oracle'`

    datepart: str, default `'day'`
        Which part of the date to modify. Supported values:

        - `'year'`
        - `'month'`
        - `'day'`
        - `'hour'`
        - `'minute'`
        - `'second'`

    number: Union[int, float], default `0`
        How many units to add to the date part.

    begin: Union[str, datetime.datetime], default `'now'`
        Base datetime to which to add dateparts.

    Returns
    -------
    The appropriate `DATEADD` string for the corresponding database flavor.

    Examples
    --------
    >>> dateadd_str(
    ...     flavor = 'mssql',
    ...     begin = datetime.datetime(2022, 1, 1, 0, 0),
    ...     number = 1,
    ... )
    "DATEADD(day, 1, CAST('2022-01-01 00:00:00' AS DATETIME))"
    >>> dateadd_str(
    ...     flavor = 'postgresql',
    ...     begin = datetime.datetime(2022, 1, 1, 0, 0),
    ...     number = 1,
    ... )
    "CAST('2022-01-01 00:00:00' AS TIMESTAMP) + INTERVAL '1 day'"

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
    if flavor in ('postgresql', 'timescaledb', 'cockroachdb', 'citus'):
        begin = (
            f"CAST({begin} AS TIMESTAMP)" if begin != 'now'
            else "CAST(NOW() AT TIME ZONE 'utc' AS TIMESTAMP)"
        )
        da = begin + (f" + INTERVAL '{number} {datepart}'" if number != 0 else '')

    elif flavor == 'duckdb':
        begin = f"CAST({begin} AS TIMESTAMP)" if begin != 'now' else 'NOW()'
        da = begin + (f" + INTERVAL '{number} {datepart}'" if number != 0 else '')

    elif flavor in ('mssql',):
        if begin_time and begin_time.microsecond != 0:
            begin = begin[:-4] + "'"
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
            begin = str(begin_time.strftime('%Y-%m-%d %H:%M:%S.%f'))
        dt_format = 'YYYY-MM-DD HH24:MI:SS.FF'
        _begin = f"'{begin}'" if begin_time else begin
        da = (
            f"TO_TIMESTAMP({_begin}, '{dt_format}')"
            + (f" + INTERVAL '{number}' {datepart}" if number != 0 else "")
        )
    return da


def test_connection(
        self,
        **kw: Any
    ) -> Union[bool, None]:
    """
    Test if a successful connection to the database may be made.

    Parameters
    ----------
    **kw:
        The keyword arguments are passed to `meerschaum.utils.misc.retry_connect`.

    Returns
    -------
    `True` if a connection is made, otherwise `False` or `None` in case of failure.

    """
    import warnings
    from meerschaum.utils.misc import retry_connect
    _default_kw = {'max_retries': 1, 'retry_wait': 0, 'warn': False, 'connector': self}
    _default_kw.update(kw)
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', 'Could not')
        try:
            return retry_connect(**_default_kw)
        except Exception as e:
            return False


def get_distinct_col_count(
        col: str,
        query: str,
        connector: Optional[meerschaum.connectors.sql.SQLConnector] = None,
        debug: bool = False
    ) -> Optional[int]:
    """
    Returns the number of distinct items in a column of a SQL query.

    Parameters
    ----------
    col: str:
        The column in the query to count.

    query: str:
        The SQL query to count from.

    connector: Optional[meerschaum.connectors.sql.SQLConnector], default None:
        The SQLConnector to execute the query.

    debug: bool, default False:
        Verbosity toggle.

    Returns
    -------
    An `int` of the number of columns in the query or `None` if the query fails.

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


def sql_item_name(item: str, flavor: str) -> str:
    """
    Parse SQL items depending on the flavor.

    Parameters
    ----------
    item: str :
        The database item (table, view, etc.) in need of quotes.
        
    flavor: str :
        The database flavor (`'postgresql'`, `'mssql'`, `'sqllite'`, etc.).

    Returns
    -------
    A `str` which contains the input `item` wrapped in the corresponding escape characters.
    
    Examples
    --------
    >>> sql_item_name('table', 'sqlite')
    '"table"'
    >>> sql_item_name('table', 'mssql')
    "[table]"

    """
    truncated_item = truncate_item_name(str(item), flavor)
    if flavor == 'oracle':
        truncated_item = pg_capital(truncated_item)
        wrappers = ('', '')
    else:
        wrappers = table_wrappers.get(flavor, table_wrappers['default'])
    return wrappers[0] + truncated_item + wrappers[1]


def pg_capital(s: str) -> str:
    """
    If string contains a capital letter, wrap it in double quotes.
    
    Parameters
    ----------
    s: str :
    The string to be escaped.

    Returns
    -------
    The input string wrapped in quotes only if it needs them.

    Examples
    --------
    >>> pg_capital("My Table")
    '"My Table"'
    >>> pg_capital('my_table')
    'my_table'

    """
    if '"' in s:
        return s
    needs_quotes = False
    for c in str(s):
        if ord(c) < ord('a') or ord(c) > ord('z'):
            if not c.isdigit() and c != '_':
                needs_quotes = True
                break
    if needs_quotes:
        return '"' + s + '"'
    return s


def oracle_capital(s: str) -> str:
    """
    Capitalize the string of an item on an Oracle database.
    """
    return s
    #  return s.upper()
    #  return s.upper() if s[0].isalpha() else s


def truncate_item_name(item: str, flavor: str) -> str:
    """
    Truncate item names to stay within the database flavor's character limit.

    Parameters
    ----------
    item: str
        The database item being referenced. This string is the "canonical" name internally.

    flavor: str
        The flavor of the database on which `item` resides.

    Returns
    -------
    The truncated string.
    """
    from meerschaum.utils.misc import truncate_string_sections
    return truncate_string_sections(
        item, max_len=max_name_lens.get(flavor, max_name_lens['default'])
    )


def build_where(
        params: Dict[str, Any],
        connector: Optional[meerschaum.connectors.sql.SQLConnector] = None,
        with_where: bool = True,
    ) -> str:
    """
    Build the `WHERE` clause based on the input criteria.

    Parameters
    ----------
    params: Dict[str, Any]:
        The keywords dictionary to convert into a WHERE clause.

    connector: Optional[meerschaum.connectors.sql.SQLConnector], default None:
        The Meerschaum SQLConnector that will be executing the query.
        The connector is used to extract the SQL dialect.

    with_where: bool, default True:
        If `True`, include the leading `'WHERE'` string.

    Returns
    -------
    A `str` of the `WHERE` clause from the input `params` dictionary for the connector's flavor.

    Examples
    --------
    ```
    >>> print(build_where({'foo': [1, 2, 3]}))
    
    WHERE
        "foo" IN ('1', '2', '3')
    ```
    """
    if connector is None:
        from meerschaum import get_connector
        connector = get_connector('sql')
    where = ""
    leading_and = "\n    AND "
    for key, value in params.items():
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
        where = ("\nWHERE\n    " if with_where else '') + where[len(leading_and):]
    return where


def table_exists(
        table: str,
        connector: Optional[meerschaum.connectors.sql.SQLConnector] = None,
        debug: bool = False,
    ) -> bool:
    """Check if a table exists.

    Parameters
    ----------
    table: str:
        The name of the table in question.
        
    connector: Optional[meerschaum.connectors.sql.SQLConnector] :
        The connector to the database which holds the table.

    debug: bool, default False :
        Verbosity toggle.

    Returns
    -------
    A `bool` indicating whether or not the table exists on the database.

    """
    if connector is None:
        from meerschaum import get_connector
        connector = get_connector('sql')

    table_name = sql_item_name(table, connector.flavor)
    q = exists_queries.get(connector.flavor, exists_queries['default']).format(
        table=table, table_name=table_name,
    )
    exists = connector.exec(q, debug=debug, silent=True) is not None
    return exists

def get_sqlalchemy_table(
        table: str,
        connector: Optional[meerschaum.connectors.sql.SQLConnector] = None,
        debug: bool = False,
    ) -> 'sqlalchemy.Table':
    """
    Construct a SQLAlchemy table from its name.

    Parameters
    ----------
    table: str :
        The name of the table on the database. Does not need to be escaped.
        
    connector: Optional[meerschaum.connectors.sql.SQLConnector], default None:
        The connector to the database which holds the table. 

    debug: bool, default False:
        Verbosity toggle.

    Returns
    -------
    A `sqlalchemy.Table` object for the table. 

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


def update_query(
        target: str,
        patch: str,
        connector: meerschaum.connectors.sql.SQLConnector,
        join_cols: List[str],
        debug: bool = False,
    ) -> str:
    """
    Build a `MERGE` or `UPDATE` query to apply a patch to target table.
    """
    from meerschaum.utils.debug import dprint
    base_query = update_queries.get(connector.flavor, update_queries['default'])
    target_table = get_sqlalchemy_table(target, connector)
    value_cols = []
    if debug:
        dprint(f"target_table.columns: {target_table.columns}")
    for c in target_table.columns:
        c_name, c_type = c.name, str(c.type)
        if c_name in join_cols:
            continue
        if connector.flavor in DB_FLAVORS_CAST_DTYPES:
            c_type = DB_FLAVORS_CAST_DTYPES[connector.flavor].get(c_type, c_type)
        value_cols.append((c_name, c_type))
    if debug:
        dprint(f"value_cols: {value_cols}")

    def sets_subquery(l_prefix: str, r_prefix: str):
        return 'SET ' + ',\n'.join([
            (
                l_prefix + sql_item_name(c_name, connector.flavor)
                + ' = ' + 'CAST(' + r_prefix
                + sql_item_name(c_name, connector.flavor) + ' AS '
                + c_type.replace('_', ' ')
                + ')'
            ) for c_name, c_type in value_cols
        ])

    def and_subquery(l_prefix: str, r_prefix: str):
        return '\nAND\n'.join([
            (
                l_prefix + sql_item_name(c, connector.flavor)
                + ' = '
                + r_prefix + sql_item_name(c, connector.flavor)
            ) for c in join_cols
        ])
    query = base_query.format(
        sets_subquery_none = sets_subquery('', 'p.'),
        sets_subquery_f = sets_subquery('f.', 'p.'),
        and_subquery_f = and_subquery('p.', 'f.'),
        and_subquery_t = and_subquery('p.', 't.'),
        target_table_name = sql_item_name(target, connector.flavor),
        patch_table_name = sql_item_name(patch, connector.flavor),
    )
    return query

    
def get_pd_type(db_type: str) -> str:
    """
    Parse a database type to a pandas data type.

    Parameters
    ----------
    db_type: str
        The database type, e.g. `DATETIME`, `BIGINT`, etc.

    Returns
    -------
    The equivalent datatype for a pandas DataFrame.
    """
    pd_type = DB_TO_PD_DTYPES.get(db_type.upper(), None)
    if pd_type is not None:
        return pd_type
    for db_t, pd_t in DB_TO_PD_DTYPES['substrings'].items():
        if db_t in db_type.upper():
            return pd_t
    return DB_TO_PD_DTYPES['default']

