#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Flavor-specific SQL tools.
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta
import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Dict, Any, Union, List, Iterable, Tuple
### Preserve legacy imports.
from meerschaum.utils.dtypes.sql import (
    DB_TO_PD_DTYPES,
    PD_TO_DB_DTYPES_FLAVORS,
    get_pd_type_from_db_type as get_pd_type,
    get_db_type_from_pd_type as get_db_type,
)
from meerschaum.utils.warnings import warn
from meerschaum.utils.debug import dprint

test_queries = {
    'default'    : 'SELECT 1',
    'oracle'     : 'SELECT 1 FROM DUAL',
    'informix'   : 'SELECT COUNT(*) FROM systables',
    'hsqldb'     : 'SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS',
}
### `table_name` is the escaped name of the table.
### `table` is the unescaped name of the table.
exists_queries = {
    'default': "SELECT COUNT(*) FROM {table_name} WHERE 1 = 0",
}
version_queries = {
    'default': "SELECT VERSION() AS {version_name}",
    'sqlite': "SELECT SQLITE_VERSION() AS {version_name}",
    'mssql': "SELECT @@version",
    'oracle': "SELECT version from PRODUCT_COMPONENT_VERSION WHERE rownum = 1",
}
SKIP_IF_EXISTS_FLAVORS = {'mssql', 'oracle'}
DROP_IF_EXISTS_FLAVORS = {
    'timescaledb', 'postgresql', 'citus', 'mssql', 'mysql', 'mariadb', 'sqlite',
}
COALESCE_UNIQUE_INDEX_FLAVORS = {'timescaledb', 'postgresql', 'citus'}
update_queries = {
    'default': """
        UPDATE {target_table_name} AS f
        {sets_subquery_none}
        FROM {target_table_name} AS t
        INNER JOIN (SELECT DISTINCT {patch_cols_str} FROM {patch_table_name}) AS p
            ON {and_subquery_t}
        WHERE
            {and_subquery_f}
            AND {date_bounds_subquery}
    """,
    'timescaledb-upsert': """
        INSERT INTO {target_table_name} ({patch_cols_str})
        SELECT {patch_cols_str}
        FROM {patch_table_name}
        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}
    """,
    'postgresql-upsert': """
        INSERT INTO {target_table_name} ({patch_cols_str})
        SELECT {patch_cols_str}
        FROM {patch_table_name}
        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}
    """,
    'citus-upsert': """
        INSERT INTO {target_table_name} ({patch_cols_str})
        SELECT {patch_cols_str}
        FROM {patch_table_name}
        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}
    """,
    'cockroachdb-upsert': """
        INSERT INTO {target_table_name} ({patch_cols_str})
        SELECT {patch_cols_str}
        FROM {patch_table_name}
        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}
    """,
    'mysql': """
        UPDATE {target_table_name} AS f
        JOIN (SELECT DISTINCT {patch_cols_str} FROM {patch_table_name}) AS p
        ON {and_subquery_f}
        {sets_subquery_f}
        WHERE {date_bounds_subquery}
    """,
    'mysql-upsert': """
        REPLACE INTO {target_table_name} ({patch_cols_str})
        SELECT {patch_cols_str}
        FROM {patch_table_name}
    """,
    'mariadb': """
        UPDATE {target_table_name} AS f
        JOIN (SELECT DISTINCT {patch_cols_str} FROM {patch_table_name}) AS p
        ON {and_subquery_f}
        {sets_subquery_f}
        WHERE {date_bounds_subquery}
    """,
    'mariadb-upsert': """
        REPLACE INTO {target_table_name} ({patch_cols_str})
        SELECT {patch_cols_str}
        FROM {patch_table_name}
    """,
    'mssql': """
        MERGE {target_table_name} f
            USING (SELECT DISTINCT {patch_cols_str} FROM {patch_table_name}) p
            ON {and_subquery_f}
            AND {date_bounds_subquery}
        WHEN MATCHED THEN
            UPDATE
            {sets_subquery_none};
    """,
    'mssql-upsert': """
        MERGE {target_table_name} f
            USING (SELECT DISTINCT {patch_cols_str} FROM {patch_table_name}) p
            ON {and_subquery_f}
            AND {date_bounds_subquery}
        {when_matched_update_sets_subquery_none}
        WHEN NOT MATCHED THEN
            INSERT ({patch_cols_str})
            VALUES ({patch_cols_prefixed_str});
    """,
    'oracle': """
        MERGE INTO {target_table_name} f
            USING (SELECT DISTINCT {patch_cols_str} FROM {patch_table_name}) p
            ON (
                {and_subquery_f}
                AND {date_bounds_subquery}
            )
        WHEN MATCHED THEN
            UPDATE
            {sets_subquery_none}
    """,
    'sqlite-upsert': """
        INSERT INTO {target_table_name} ({patch_cols_str})
        SELECT {patch_cols_str}
        FROM {patch_table_name}
        WHERE true
        ON CONFLICT ({join_cols_str}) DO {update_or_nothing} {sets_subquery_none_excluded}
    """,
    'sqlite_delete_insert': [
        """
        DELETE FROM {target_table_name} AS f
        WHERE ROWID IN (
            SELECT t.ROWID
            FROM {target_table_name} AS t
            INNER JOIN (SELECT DISTINCT * FROM {patch_table_name}) AS p
                ON {and_subquery_t}
        );
        """,
        """
        INSERT INTO {target_table_name} AS f
        SELECT DISTINCT {patch_cols_str} FROM {patch_table_name} AS p
        """,
    ],
}
columns_types_queries = {
    'default': """
        SELECT
            table_catalog AS database,
            table_schema AS schema,
            table_name AS table,
            column_name AS column,
            data_type AS type
        FROM information_schema.columns
        WHERE table_name IN ('{table}', '{table_trunc}')
    """,
    'sqlite': """
        SELECT
            '' "database",
            '' "schema",
            m.name "table",
            p.name "column",
            p.type "type"
        FROM sqlite_master m
        LEFT OUTER JOIN pragma_table_info((m.name)) p
            ON m.name <> p.name
        WHERE m.type = 'table'
            AND m.name IN ('{table}', '{table_trunc}')
    """,
    'mssql': """
        SELECT
            TABLE_CATALOG AS [database],
            TABLE_SCHEMA AS [schema],
            TABLE_NAME AS [table],
            COLUMN_NAME AS [column],
            DATA_TYPE AS [type]
        FROM {db_prefix}INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME LIKE '{table}%'
            OR TABLE_NAME LIKE '{table_trunc}%'
    """,
    'mysql': """
        SELECT
            TABLE_SCHEMA `database`,
            TABLE_SCHEMA `schema`,
            TABLE_NAME `table`,
            COLUMN_NAME `column`,
            DATA_TYPE `type`
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME IN ('{table}', '{table_trunc}')
    """,
    'mariadb': """
        SELECT
            TABLE_SCHEMA `database`,
            TABLE_SCHEMA `schema`,
            TABLE_NAME `table`,
            COLUMN_NAME `column`,
            DATA_TYPE `type`
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME IN ('{table}', '{table_trunc}')
    """,
    'oracle': """
        SELECT
            NULL AS "database",
            NULL AS "schema",
            TABLE_NAME AS "table",
            COLUMN_NAME AS "column",
            DATA_TYPE AS "type"
        FROM all_tab_columns
        WHERE TABLE_NAME IN (
            '{table}',
            '{table_trunc}',
            '{table_lower}',
            '{table_lower_trunc}',
            '{table_upper}',
            '{table_upper_trunc}'
        )
    """,
}
hypertable_queries = {
    'timescaledb': 'SELECT hypertable_size(\'{table_name}\')',
    'citus': 'SELECT citus_table_size(\'{table_name}\')',
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
json_flavors = {'postgresql', 'timescaledb', 'citus', 'cockroachdb'}
NO_SCHEMA_FLAVORS = {'oracle', 'sqlite', 'mysql', 'mariadb', 'duckdb'}
DEFAULT_SCHEMA_FLAVORS = {
    'postgresql': 'public',
    'timescaledb': 'public',
    'citus': 'public',
    'cockroachdb': 'public',
    'mysql': 'mysql',
    'mariadb': 'mysql',
    'mssql': 'dbo',
}
OMIT_NULLSFIRST_FLAVORS = {'mariadb', 'mysql', 'mssql'}

SINGLE_ALTER_TABLE_FLAVORS = {'duckdb', 'sqlite', 'mssql', 'oracle'}
NO_CTE_FLAVORS = {'mysql', 'mariadb'}
NO_SELECT_INTO_FLAVORS = {'sqlite', 'oracle', 'mysql', 'mariadb', 'duckdb'}


def clean(substring: str) -> str:
    """
    Ensure a substring is clean enough to be inserted into a SQL query.
    Raises an exception when banned words are used.
    """
    from meerschaum.utils.warnings import error
    banned_symbols = [';', '--', 'drop ',]
    for symbol in banned_symbols:
        if symbol in str(substring).lower():
            error(f"Invalid string: '{substring}'")


def dateadd_str(
    flavor: str = 'postgresql',
    datepart: str = 'day',
    number: Union[int, float] = 0,
    begin: Union[str, datetime, int] = 'now'
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

    begin: Union[str, datetime], default `'now'`
        Base datetime to which to add dateparts.

    Returns
    -------
    The appropriate `DATEADD` string for the corresponding database flavor.

    Examples
    --------
    >>> dateadd_str(
    ...     flavor = 'mssql',
    ...     begin = datetime(2022, 1, 1, 0, 0),
    ...     number = 1,
    ... )
    "DATEADD(day, 1, CAST('2022-01-01 00:00:00' AS DATETIME))"
    >>> dateadd_str(
    ...     flavor = 'postgresql',
    ...     begin = datetime(2022, 1, 1, 0, 0),
    ...     number = 1,
    ... )
    "CAST('2022-01-01 00:00:00' AS TIMESTAMP) + INTERVAL '1 day'"

    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.warnings import error
    dateutil_parser = attempt_import('dateutil.parser')
    if 'int' in str(type(begin)).lower():
        return str(begin)
    if not begin:
        return ''

    _original_begin = begin
    begin_time = None
    ### Sanity check: make sure `begin` is a valid datetime before we inject anything.
    if not isinstance(begin, datetime):
        try:
            begin_time = dateutil_parser.parse(begin)
        except Exception:
            begin_time = None
    else:
        begin_time = begin

    ### Unable to parse into a datetime.
    if begin_time is None:
        ### Throw an error if banned symbols are included in the `begin` string.
        clean(str(begin))
    ### If begin is a valid datetime, wrap it in quotes.
    else:
        if isinstance(begin, datetime) and begin.tzinfo is not None:
            begin = begin.astimezone(timezone.utc)
        begin = (
            f"'{begin.replace(tzinfo=None)}'"
            if isinstance(begin, datetime)
            else f"'{begin}'"
        )

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
                datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y:%m:%d %M:%S.%f')
            )
        elif begin_time:
            begin = str(begin_time.strftime('%Y-%m-%d %H:%M:%S.%f'))
        dt_format = 'YYYY-MM-DD HH24:MI:SS.FF'
        _begin = f"'{begin}'" if begin_time else begin
        da = (
            (f"TO_TIMESTAMP({_begin}, '{dt_format}')" if begin_time else _begin)
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
        The keyword arguments are passed to `meerschaum.connectors.poll.retry_connect`.

    Returns
    -------
    `True` if a connection is made, otherwise `False` or `None` in case of failure.

    """
    import warnings
    from meerschaum.connectors.poll import retry_connect
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
        connector: Optional[mrsm.connectors.sql.SQLConnector] = None,
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

    connector: Optional[mrsm.connectors.sql.SQLConnector], default None:
        The SQLConnector to execute the query.

    debug: bool, default False:
        Verbosity toggle.

    Returns
    -------
    An `int` of the number of columns in the query or `None` if the query fails.

    """
    if connector is None:
        connector = mrsm.get_connector('sql')

    _col_name = sql_item_name(col, connector.flavor, None)

    _meta_query = (
        f"""
        WITH src AS ( {query} ),
        dist AS ( SELECT DISTINCT {_col_name} FROM src )
        SELECT COUNT(*) FROM dist"""
    ) if connector.flavor not in ('mysql', 'mariadb') else (
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT {_col_name}
            FROM ({query}) AS src
        ) AS dist"""
    )

    result = connector.value(_meta_query, debug=debug)
    try:
        return int(result)
    except Exception as e:
        return None


def sql_item_name(item: str, flavor: str, schema: Optional[str] = None) -> str:
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
    >>> sql_item_name('table', 'postgresql', schema='abc')
    '"abc"."table"'

    """
    truncated_item = truncate_item_name(str(item), flavor)
    if flavor == 'oracle':
        truncated_item = pg_capital(truncated_item)
        ### NOTE: System-reserved words must be quoted.
        if truncated_item.lower() in (
            'float', 'varchar', 'nvarchar', 'clob',
            'boolean', 'integer', 'table',
        ):
            wrappers = ('"', '"')
        else:
            wrappers = ('', '')
    else:
        wrappers = table_wrappers.get(flavor, table_wrappers['default'])

    ### NOTE: SQLite does not support schemas.
    if flavor == 'sqlite':
        schema = None

    schema_prefix = (
        (wrappers[0] + schema + wrappers[1] + '.')
        if schema is not None
        else ''
    )

    return schema_prefix + wrappers[0] + truncated_item + wrappers[1]


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
    needs_quotes = s.startswith('_')
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
        If a value is a string which begins with an underscore, negate that value
        (e.g. `!=` instead of `=` or `NOT IN` instead of `IN`).
        A value of `_None` will be interpreted as `IS NOT NULL`.

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
    import json
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.dtypes import value_is_null, none_if_null
    negation_prefix = STATIC_CONFIG['system']['fetch_pipes_keys']['negation_prefix']
    try:
        params_json = json.dumps(params)
    except Exception as e:
        params_json = str(params)
    bad_words = ['drop ', '--', ';']
    for word in bad_words:
        if word in params_json.lower():
            warn(f"Aborting build_where() due to possible SQL injection.")
            return ''

    if connector is None:
        from meerschaum import get_connector
        connector = get_connector('sql')
    where = ""
    leading_and = "\n    AND "
    for key, value in params.items():
        _key = sql_item_name(key, connector.flavor, None)
        ### search across a list (i.e. IN syntax)
        if isinstance(value, Iterable) and not isinstance(value, (dict, str)):
            includes = [
                none_if_null(item)
                for item in value
                if not str(item).startswith(negation_prefix)
            ]
            null_includes = [item for item in includes if item is None]
            not_null_includes = [item for item in includes if item is not None]
            excludes = [
                none_if_null(str(item)[len(negation_prefix):])
                for item in value
                if str(item).startswith(negation_prefix)
            ]
            null_excludes = [item for item in excludes if item is None]
            not_null_excludes = [item for item in excludes if item is not None]

            if includes:
                where += f"{leading_and}("
            if not_null_includes:
                where += f"{_key} IN ("
                for item in not_null_includes:
                    quoted_item = str(item).replace("'", "''")
                    where += f"'{quoted_item}', "
                where = where[:-2] + ")"
            if null_includes:
                where += ("\n    OR " if not_null_includes else "") + f"{_key} IS NULL"
            if includes:
                where += ")"

            if excludes:
                where += f"{leading_and}("
            if not_null_excludes:
                where += f"{_key} NOT IN ("
                for item in not_null_excludes:
                    quoted_item = str(item).replace("'", "''")
                    where += f"'{quoted_item}', "
                where = where[:-2] + ")"
            if null_excludes:
                where += ("\n    AND " if not_null_excludes else "") + f"{_key} IS NOT NULL"
            if excludes:
                where += ")"

            continue

        ### search a dictionary
        elif isinstance(value, dict):
            import json
            where += (f"{leading_and}CAST({_key} AS TEXT) = '" + json.dumps(value) + "'")
            continue

        eq_sign = '='
        is_null = 'IS NULL'
        if value_is_null(str(value).lstrip(negation_prefix)):
            value = (
                (negation_prefix + 'None')
                if str(value).startswith(negation_prefix)
                else None
            )
        if str(value).startswith(negation_prefix):
            value = str(value)[len(negation_prefix):]
            eq_sign = '!='
            if value_is_null(value):
                value = None
                is_null = 'IS NOT NULL'
        quoted_value = str(value).replace("'", "''")
        where += (
            f"{leading_and}{_key} "
            + (is_null if value is None else f"{eq_sign} '{quoted_value}'")
        )

    if len(where) > 1:
        where = ("\nWHERE\n    " if with_where else '') + where[len(leading_and):]
    return where


def table_exists(
        table: str,
        connector: mrsm.connectors.sql.SQLConnector,
        schema: Optional[str] = None,
        debug: bool = False,
    ) -> bool:
    """Check if a table exists.

    Parameters
    ----------
    table: str:
        The name of the table in question.
        
    connector: mrsm.connectors.sql.SQLConnector
        The connector to the database which holds the table.

    schema: Optional[str], default None
        Optionally specify the table schema.
        Defaults to `connector.schema`.

    debug: bool, default False :
        Verbosity toggle.

    Returns
    -------
    A `bool` indicating whether or not the table exists on the database.

    """
    sqlalchemy = mrsm.attempt_import('sqlalchemy')
    schema = schema or connector.schema
    insp = sqlalchemy.inspect(connector.engine)
    truncated_table_name = truncate_item_name(str(table), connector.flavor)
    return insp.has_table(truncated_table_name, schema=schema)


def get_sqlalchemy_table(
        table: str,
        connector: Optional[meerschaum.connectors.sql.SQLConnector] = None,
        schema: Optional[str] = None,
        refresh: bool = False,
        debug: bool = False,
    ) -> 'sqlalchemy.Table':
    """
    Construct a SQLAlchemy table from its name.

    Parameters
    ----------
    table: str
        The name of the table on the database. Does not need to be escaped.
        
    connector: Optional[meerschaum.connectors.sql.SQLConnector], default None:
        The connector to the database which holds the table. 

    schema: Optional[str], default None
        Specify on which schema the table resides.
        Defaults to the schema set in `connector`.

    refresh: bool, default False
        If `True`, rebuild the cached table object.

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
    from meerschaum.utils.warnings import warn
    if refresh:
        connector.metadata.clear()
    tables = get_tables(mrsm_instance=connector, debug=debug, create=False)
    sqlalchemy = attempt_import('sqlalchemy')
    truncated_table_name = truncate_item_name(str(table), connector.flavor)
    table_kwargs = {
        'autoload_with': connector.engine,
    }
    if schema:
        table_kwargs['schema'] = schema

    if refresh or truncated_table_name not in tables:
        try:
            tables[truncated_table_name] = sqlalchemy.Table(
                truncated_table_name,
                connector.metadata,
                **table_kwargs
            )
        except sqlalchemy.exc.NoSuchTableError as e:
            warn(f"Table '{truncated_table_name}' does not exist in '{connector}'.")
            return None
    return tables[truncated_table_name]


def get_table_cols_types(
    table: str,
    connectable: Union[
        'mrsm.connectors.sql.SQLConnector',
        'sqlalchemy.orm.session.Session',
        'sqlalchemy.engine.base.Engine'
    ],
    flavor: Optional[str] = None,
    schema: Optional[str] = None,
    database: Optional[str] = None,
    debug: bool = False,
) -> Dict[str, str]:
    """
    Return a dictionary mapping a table's columns to data types.
    This is useful for inspecting tables creating during a not-yet-committed session.

    NOTE: This may return incorrect columns if the schema is not explicitly stated.
        Use this function if you are confident the table name is unique or if you have
        and explicit schema.
        To use the configured schema, get the columns from `get_sqlalchemy_table()` instead.

    Parameters
    ----------
    table: str
        The name of the table (unquoted).

    connectable: Union[
        'mrsm.connectors.sql.SQLConnector',
        'sqlalchemy.orm.session.Session',
    ]
        The connection object used to fetch the columns and types.

    flavor: Optional[str], default None
        The database dialect flavor to use for the query.
        If omitted, default to `connectable.flavor`.

    schema: Optional[str], default None
        If provided, restrict the query to this schema.

    database: Optional[str]. default None
        If provided, restrict the query to this database.

    Returns
    -------
    A dictionary mapping column names to data types.
    """
    from meerschaum.connectors import SQLConnector
    sqlalchemy = mrsm.attempt_import('sqlalchemy')
    flavor = flavor or getattr(connectable, 'flavor', None)
    if not flavor:
        raise ValueError("Please provide a database flavor.")
    if flavor == 'duckdb' and not isinstance(connectable, SQLConnector):
        raise ValueError("You must provide a SQLConnector when using DuckDB.")
    if flavor in NO_SCHEMA_FLAVORS:
        schema = None
    if schema is None:
        schema = DEFAULT_SCHEMA_FLAVORS.get(flavor, None)
    if flavor in ('sqlite', 'duckdb', 'oracle'):
        database = None
    table_trunc = truncate_item_name(table, flavor=flavor)
    table_lower = table.lower()
    table_upper = table.upper()
    table_lower_trunc = truncate_item_name(table_lower, flavor=flavor)
    table_upper_trunc = truncate_item_name(table_upper, flavor=flavor)
    db_prefix = (
        "tempdb."
        if flavor == 'mssql' and table.startswith('#')
        else ""
    )

    cols_types_query = sqlalchemy.text(
        columns_types_queries.get(
            flavor,
            columns_types_queries['default']
        ).format(
            table=table,
            table_trunc=table_trunc,
            table_lower=table_lower,
            table_lower_trunc=table_lower_trunc,
            table_upper=table_upper,
            table_upper_trunc=table_upper_trunc,
            db_prefix=db_prefix,
        )
    )

    cols = ['database', 'schema', 'table', 'column', 'type']
    result_cols_ix = dict(enumerate(cols))

    debug_kwargs = {'debug': debug} if isinstance(connectable, SQLConnector) else {}
    if not debug_kwargs and debug:
        dprint(cols_types_query)

    try:
        result_rows = (
            [
                row
                for row in connectable.execute(cols_types_query, **debug_kwargs).fetchall()
            ]
            if flavor != 'duckdb'
            else [
                tuple([doc[col] for col in cols])
                for doc in connectable.read(cols_types_query, debug=debug).to_dict(orient='records')
            ]
        )
        cols_types_docs = [
            {
                result_cols_ix[i]: val
                for i, val in enumerate(row)
            }
            for row in result_rows
        ]
        cols_types_docs_filtered = [
            doc
            for doc in cols_types_docs
            if (
                (
                    not schema
                    or doc['schema'] == schema
                )
                and
                (
                    not database
                    or doc['database'] == database
                )
            )
        ]

        ### NOTE: This may return incorrect columns if the schema is not explicitly stated.
        if cols_types_docs and not cols_types_docs_filtered:
            cols_types_docs_filtered = cols_types_docs

        return {
            (
                doc['column']
                if flavor != 'oracle' else (
                    (
                        doc['column'].lower()
                        if (doc['column'].isupper() and doc['column'].replace('_', '').isalpha())
                        else doc['column']
                    )
                )
            ): doc['type'].upper()
            for doc in cols_types_docs_filtered
        }
    except Exception as e:
        warn(f"Failed to fetch columns for table '{table}':\n{e}")
        return {}


def get_update_queries(
    target: str,
    patch: str,
    connectable: Union[
        mrsm.connectors.sql.SQLConnector,
        'sqlalchemy.orm.session.Session'
    ],
    join_cols: Iterable[str],
    flavor: Optional[str] = None,
    upsert: bool = False,
    datetime_col: Optional[str] = None,
    schema: Optional[str] = None,
    patch_schema: Optional[str] = None,
    debug: bool = False,
) -> List[str]:
    """
    Build a list of `MERGE`, `UPDATE`, `DELETE`/`INSERT` queries to apply a patch to target table.

    Parameters
    ----------
    target: str
        The name of the target table.

    patch: str
        The name of the patch table. This should have the same shape as the target.

    connectable: Union[meerschaum.connectors.sql.SQLConnector, sqlalchemy.orm.session.Session]
        The `SQLConnector` or SQLAlchemy session which will later execute the queries.

    join_cols: List[str]
        The columns to use to join the patch to the target.

    flavor: Optional[str], default None
        If using a SQLAlchemy session, provide the expected database flavor.

    upsert: bool, default False
        If `True`, return an upsert query rather than an update.

    datetime_col: Optional[str], default None
        If provided, bound the join query using this column as the datetime index.
        This must be present on both tables.

    schema: Optional[str], default None
        If provided, use this schema when quoting the target table.
        Defaults to `connector.schema`.

    patch_schema: Optional[str], default None
        If provided, use this schema when quoting the patch table.
        Defaults to `schema`.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A list of query strings to perform the update operation.
    """
    from meerschaum.connectors import SQLConnector
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.dtypes.sql import DB_FLAVORS_CAST_DTYPES
    flavor = flavor or (connectable.flavor if isinstance(connectable, SQLConnector) else None)
    if not flavor:
        raise ValueError("Provide a flavor if using a SQLAlchemy session.")
    if (
        flavor == 'sqlite'
        and isinstance(connectable, SQLConnector)
        and connectable.db_version < '3.33.0'
    ):
        flavor = 'sqlite_delete_insert'
    flavor_key = (f'{flavor}-upsert' if upsert else flavor)
    base_queries = update_queries.get(
        flavor_key,
        update_queries['default']
    )
    if not isinstance(base_queries, list):
        base_queries = [base_queries]
    schema = schema or (connectable.schema if isinstance(connectable, SQLConnector) else None)
    patch_schema = patch_schema or schema
    target_table_columns = get_table_cols_types(
        target,
        connectable,
        flavor=flavor,
        schema=schema,
        debug=debug,
    )
    patch_table_columns = get_table_cols_types(
        patch,
        connectable,
        flavor=flavor,
        schema=patch_schema,
        debug=debug,
    )

    patch_cols_str = ', '.join(
        [
            sql_item_name(col, flavor)
            for col in patch_table_columns
        ]
    )
    patch_cols_prefixed_str = ', '.join(
        [
            'p.' + sql_item_name(col, flavor)
            for col in patch_table_columns
        ]
    )

    join_cols_str = ', '.join(
        [
            sql_item_name(col, flavor)
            for col in join_cols
        ]
    )

    value_cols = []
    join_cols_types = []
    if debug:
        dprint("target_table_columns:")
        mrsm.pprint(target_table_columns)
    for c_name, c_type in target_table_columns.items():
        if c_name not in patch_table_columns:
            continue
        if flavor in DB_FLAVORS_CAST_DTYPES:
            c_type = DB_FLAVORS_CAST_DTYPES[flavor].get(c_type.upper(), c_type)
        (
            join_cols_types
            if c_name in join_cols
            else value_cols
        ).append((c_name, c_type))
    if debug:
        dprint(f"value_cols: {value_cols}")

    if not join_cols_types:
        return []
    if not value_cols and not upsert:
        return []

    coalesce_join_cols_str = ', '.join(
        [
            'COALESCE('
            + sql_item_name(c_name, flavor)
            + ', '
            + get_null_replacement(c_type, flavor)
            + ')'
            for c_name, c_type in join_cols_types
        ]
    )

    update_or_nothing = ('UPDATE' if value_cols else 'NOTHING')

    def sets_subquery(l_prefix: str, r_prefix: str):
        if not value_cols:
            return ''
        return 'SET ' + ',\n'.join([
            (
                l_prefix + sql_item_name(c_name, flavor, None)
                + ' = '
                + ('CAST(' if flavor != 'sqlite' else '')
                + r_prefix
                + sql_item_name(c_name, flavor, None)
                + (' AS ' if flavor != 'sqlite' else '')
                + (c_type.replace('_', ' ') if flavor != 'sqlite' else '')
                + (')' if flavor != 'sqlite' else '')
            ) for c_name, c_type in value_cols
        ])

    def and_subquery(l_prefix: str, r_prefix: str):
        return '\nAND\n'.join([
            (
                "COALESCE("
                + l_prefix
                + sql_item_name(c_name, flavor, None)
                + ", "
                + get_null_replacement(c_type, flavor)
                + ")"
                + ' = '
                + "COALESCE("
                + r_prefix
                + sql_item_name(c_name, flavor, None)
                + ", "
                + get_null_replacement(c_type, flavor)
                + ")"
            ) for c_name, c_type in join_cols_types
        ])

    target_table_name = sql_item_name(target, flavor, schema)
    patch_table_name = sql_item_name(patch, flavor, patch_schema)
    dt_col_name = sql_item_name(datetime_col, flavor, None) if datetime_col else None
    date_bounds_subquery = (
        f"""
        f.{dt_col_name} >= (SELECT MIN({dt_col_name}) FROM {patch_table_name})
        AND f.{dt_col_name} <= (SELECT MAX({dt_col_name}) FROM {patch_table_name})
        """
        if datetime_col
        else "1 = 1"
    )

    ### NOTE: MSSQL upserts must exclude the update portion if only upserting indices.
    when_matched_update_sets_subquery_none = "" if not value_cols else (
        "WHEN MATCHED THEN"
        f"     UPDATE {sets_subquery('', 'p.')}"
    )

    return [
        base_query.format(
            sets_subquery_none=sets_subquery('', 'p.'),
            sets_subquery_none_excluded=sets_subquery('', 'EXCLUDED.'),
            sets_subquery_f=sets_subquery('f.', 'p.'),
            and_subquery_f=and_subquery('p.', 'f.'),
            and_subquery_t=and_subquery('p.', 't.'),
            target_table_name=target_table_name,
            patch_table_name=patch_table_name,
            patch_cols_str=patch_cols_str,
            patch_cols_prefixed_str=patch_cols_prefixed_str,
            date_bounds_subquery=date_bounds_subquery,
            join_cols_str=join_cols_str,
            coalesce_join_cols_str=coalesce_join_cols_str,
            update_or_nothing=update_or_nothing,
            when_matched_update_sets_subquery_none=when_matched_update_sets_subquery_none,
        )
        for base_query in base_queries
    ]


def get_null_replacement(typ: str, flavor: str) -> str:
    """
    Return a value that may temporarily be used in place of NULL for this type.

    Parameters
    ----------
    typ: str
        The typ to be converted to NULL.

    flavor: str
        The database flavor for which this value will be used.

    Returns
    -------
    A value which may stand in place of NULL for this type.
    `'None'` is returned if a value cannot be determined.
    """
    from meerschaum.utils.dtypes.sql import DB_FLAVORS_CAST_DTYPES
    if 'int' in typ.lower() or typ.lower() in ('numeric', 'number'):
        return '-987654321'
    if 'bool' in typ.lower():
        bool_typ = (
            PD_TO_DB_DTYPES_FLAVORS
            .get('bool', {})
            .get(flavor, PD_TO_DB_DTYPES_FLAVORS['bool']['default'])
        )
        if flavor in DB_FLAVORS_CAST_DTYPES:
            bool_typ = DB_FLAVORS_CAST_DTYPES[flavor].get(bool_typ, bool_typ)
        val_to_cast = (
            -987654321
            if flavor in ('mysql', 'mariadb', 'sqlite', 'mssql')
            else 0
        )
        return f'CAST({val_to_cast} AS {bool_typ})'
    if 'time' in typ.lower() or 'date' in typ.lower():
        return dateadd_str(flavor=flavor, begin='1900-01-01')
    if 'float' in typ.lower() or 'double' in typ.lower() or typ.lower() in ('decimal',):
        return '-987654321.0'
    if typ.lower() in ('uniqueidentifier', 'guid', 'uuid'):
        magic_val = 'DEADBEEF-ABBA-BABE-CAFE-DECAFC0FFEE5'
        if flavor == 'mssql':
            return f"CAST('{magic_val}' AS UNIQUEIDENTIFIER)"
        return f"'{magic_val}'"
    return ('n' if flavor == 'oracle' else '') + "'-987654321'"


def get_db_version(conn: 'SQLConnector', debug: bool = False) -> Union[str, None]:
    """
    Fetch the database version if possible.
    """
    version_name = sql_item_name('version', conn.flavor, None)
    version_query = version_queries.get(
        conn.flavor,
        version_queries['default']
    ).format(version_name=version_name)
    return conn.value(version_query, debug=debug)


def get_rename_table_queries(
    old_table: str,
    new_table: str,
    flavor: str,
    schema: Optional[str] = None,
) -> List[str]:
    """
    Return queries to alter a table's name.

    Parameters
    ----------
    old_table: str
        The unquoted name of the old table.

    new_table: str
        The unquoted name of the new table.

    flavor: str
        The database flavor to use for the query (e.g. `'mssql'`, `'postgresql'`.

    schema: Optional[str], default None
        The schema on which the table resides.

    Returns
    -------
    A list of `ALTER TABLE` or equivalent queries for the database flavor.
    """
    old_table_name = sql_item_name(old_table, flavor, schema)
    new_table_name = sql_item_name(new_table, flavor, None)
    tmp_table = '_tmp_rename_' + new_table
    tmp_table_name = sql_item_name(tmp_table, flavor, schema)
    if flavor == 'mssql':
        return [f"EXEC sp_rename '{old_table}', '{new_table}'"]

    if_exists_str = "IF EXISTS" if flavor in DROP_IF_EXISTS_FLAVORS else ""
    if flavor == 'duckdb':
        return [
            get_create_table_query(f"SELECT * FROM {old_table_name}", tmp_table, 'duckdb', schema),
            get_create_table_query(f"SELECT * FROM {tmp_table_name}", new_table, 'duckdb', schema),
            f"DROP TABLE {if_exists_str} {tmp_table_name}",
            f"DROP TABLE {if_exists_str} {old_table_name}",
        ]

    return [f"ALTER TABLE {old_table_name} RENAME TO {new_table_name}"]


def get_create_table_query(
        query: str,
        new_table: str,
        flavor: str,
        schema: Optional[str] = None,
    ) -> str:
    """
    Return a query to create a new table from a `SELECT` query.

    Parameters
    ----------
    query: str
        The select query to use for the creation of the table.

    new_table: str
        The unquoted name of the new table.

    flavor: str
        The database flavor to use for the query (e.g. `'mssql'`, `'postgresql'`.

    schema: Optional[str], default None
        The schema on which the table will reside.

    Returns
    -------
    A `CREATE TABLE` (or `SELECT INTO`) query for the database flavor.
    """
    import textwrap
    create_cte = 'create_query'
    create_cte_name = sql_item_name(create_cte, flavor, None)
    new_table_name = sql_item_name(new_table, flavor, schema)
    if flavor in ('mssql',):
        query = query.lstrip()
        original_query = query
        if 'with ' in query.lower():
            final_select_ix = query.lower().rfind('select')
            def_name = query[len('WITH '):].split(' ', maxsplit=1)[0]
            return (
                query[:final_select_ix].rstrip() + ',\n'
                + f"{create_cte_name} AS (\n"
                + query[final_select_ix:]
                + "\n)\n"
                + f"SELECT *\nINTO {new_table_name}\nFROM {create_cte_name}"
            )

        create_table_query = f"""
            SELECT *
            INTO {new_table_name}
            FROM ({query}) AS {create_cte_name}
        """
    elif flavor in (None,):
        create_table_query = f"""
            WITH {create_cte_name} AS ({query})
            CREATE TABLE {new_table_name} AS
            SELECT *
            FROM {create_cte_name}
        """
    elif flavor in ('sqlite', 'mysql', 'mariadb', 'duckdb', 'oracle'):
        create_table_query = f"""
            CREATE TABLE {new_table_name} AS
            SELECT *
            FROM ({query})""" + (f""" AS {create_cte_name}""" if flavor != 'oracle' else '') + """
        """
    else:
        create_table_query = f"""
            SELECT *
            INTO {new_table_name}
            FROM ({query}) AS {create_cte_name}
        """

    return textwrap.dedent(create_table_query)


def format_cte_subquery(
        sub_query: str,
        flavor: str,
        sub_name: str = 'src',
        cols_to_select: Union[List[str], str] = '*',
    ) -> str:
    """
    Given a subquery, build a wrapper query that selects from the CTE subquery.

    Parameters
    ----------
    sub_query: str
        The subquery to wrap.

    flavor: str
        The database flavor to use for the query (e.g. `'mssql'`, `'postgresql'`.

    sub_name: str, default 'src'
        If possible, give this name to the CTE (must be unquoted).

    cols_to_select: Union[List[str], str], default ''
        If specified, choose which columns to select from the CTE.
        If a list of strings is provided, each item will be quoted and joined with commas.
        If a string is given, assume it is quoted and insert it into the query.

    Returns
    -------
    A wrapper query that selects from the CTE.
    """
    import textwrap
    quoted_sub_name = sql_item_name(sub_name, flavor, None)
    cols_str = (
        cols_to_select
        if isinstance(cols_to_select, str)
        else ', '.join([sql_item_name(col, flavor, None) for col in cols_to_select])
    )
    return textwrap.dedent(
        f"""
        SELECT {cols_str}
        FROM ({sub_query})"""
        + (f' AS {quoted_sub_name}' if flavor != 'oracle' else '') + """
        """
    )


def session_execute(
        session: 'sqlalchemy.orm.session.Session',
        queries: Union[List[str], str],
        with_results: bool = False,
        debug: bool = False,
    ) -> Union[mrsm.SuccessTuple, Tuple[mrsm.SuccessTuple, List['sqlalchemy.sql.ResultProxy']]]:
    """
    Similar to `SQLConnector.exec_queries()`, execute a list of queries
    and roll back when one fails.

    Parameters
    ----------
    session: sqlalchemy.orm.session.Session
        A SQLAlchemy session representing a transaction.

    queries: Union[List[str], str]
        A query or list of queries to be executed.
        If a query fails, roll back the session.

    with_results: bool, default False
        If `True`, return a list of result objects.

    Returns
    -------
    A `SuccessTuple` indicating the queries were successfully executed.
    If `with_results`, return the `SuccessTuple` and a list of results.
    """
    sqlalchemy = mrsm.attempt_import('sqlalchemy')
    if not isinstance(queries, list):
        queries = [queries]
    successes, msgs, results = [], [], []
    for query in queries:
        query_text = sqlalchemy.text(query)
        fail_msg = "Failed to execute queries."
        try:
            result = session.execute(query_text)
            query_success = result is not None
            query_msg = "Success" if query_success else fail_msg
        except Exception as e:
            query_success = False
            query_msg = f"{fail_msg}\n{e}"
            result = None
        successes.append(query_success)
        msgs.append(query_msg)
        results.append(result)
        if not query_success:
            session.rollback()
            break
    success, msg = all(successes), '\n'.join(msgs)
    if with_results:
        return (success, msg), results
    return success, msg
