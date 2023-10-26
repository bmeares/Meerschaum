#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Flavor-specific SQL tools.
"""

from __future__ import annotations
import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Dict, Any, Union, List, Iterable
### Preserve legacy imports.
from meerschaum.utils.dtypes.sql import (
    DB_TO_PD_DTYPES,
    PD_TO_DB_DTYPES_FLAVORS,
    get_pd_type_from_db_type as get_pd_type,
    get_db_type_from_pd_type as get_db_type,
)

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
version_queries = {
    'default': "SELECT VERSION() AS {version_name}",
    'sqlite': "SELECT SQLITE_VERSION() AS {version_name}",
    'mssql': "SELECT @@version",
    'oracle': "SELECT version from PRODUCT_COMPONENT_VERSION WHERE rownum = 1",
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
        UPDATE {target_table_name} AS f,
            (SELECT DISTINCT * FROM {patch_table_name}) AS p
        {sets_subquery_f}
        WHERE
            {and_subquery_f}
    """,
    'mariadb': """
        UPDATE {target_table_name} AS f,
            (SELECT DISTINCT * FROM {patch_table_name}) AS p
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
        SELECT DISTINCT * FROM {patch_table_name} AS p
        """,
    ],
    'default': """
        UPDATE {target_table_name} AS f
        {sets_subquery_none}
        FROM {target_table_name} AS t
        INNER JOIN (SELECT DISTINCT * FROM {patch_table_name}) AS p
            ON {and_subquery_t}
        WHERE
            {and_subquery_f}
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
OMIT_NULLSFIRST_FLAVORS = {'mariadb', 'mysql', 'mssql'}

SINGLE_ALTER_TABLE_FLAVORS = {'duckdb', 'sqlite', 'mssql', 'oracle'}
NO_CTE_FLAVORS = {'mysql', 'mariadb'}
NO_SELECT_INTO_FLAVORS = {'sqlite', 'oracle', 'mysql', 'mariadb', 'duckdb'}

### MySQL doesn't allow for casting as BIGINT, so this is a workaround.
DB_FLAVORS_CAST_DTYPES = {
    'mariadb': {
        'BIGINT': 'DECIMAL',
        'TINYINT': 'SIGNED INT',
        'TEXT': 'CHAR(10000) CHARACTER SET utf8',
        'BOOL': 'SIGNED INT',
        'BOOLEAN': 'SIGNED INT',
        'DOUBLE PRECISION': 'DECIMAL',
        'DOUBLE': 'DECIMAL',
        'FLOAT': 'DECIMAL',
    },
    'mysql': {
        'BIGINT': 'DECIMAL',
        'TINYINT': 'SIGNED INT',
        'TEXT': 'CHAR(10000) CHARACTER SET utf8',
        'BOOL': 'SIGNED INT',
        'BOOLEAN': 'SIGNED INT',
        'DOUBLE PRECISION': 'DECIMAL',
        'DOUBLE': 'DECIMAL',
        'FLOAT': 'DECIMAL',
    },
    'oracle': {
        'NVARCHAR(2000)': 'NVARCHAR2(2000)'
    },
    'mssql': {
        'NVARCHAR COLLATE "SQL Latin1 General CP1 CI AS"': 'NVARCHAR(MAX)',
        'NVARCHAR COLLATE "SQL_Latin1_General_CP1_CI_AS"': 'NVARCHAR(MAX)',
        'VARCHAR COLLATE "SQL Latin1 General CP1 CI AS"': 'NVARCHAR(MAX)',
        'VARCHAR COLLATE "SQL_Latin1_General_CP1_CI_AS"': 'NVARCHAR(MAX)',
    },
}


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
        begin: Union[str, datetime.datetime, int] = 'now'
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
    dateutil_parser = attempt_import('dateutil.parser')
    if 'int' in str(type(begin)).lower():
        return str(begin)
    if not begin:
        return ''

    _original_begin = begin
    begin_time = None
    ### Sanity check: make sure `begin` is a valid datetime before we inject anything.
    if not isinstance(begin, datetime.datetime):
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
        if truncated_item.lower() in (
            'float', 'varchar', 'nvarchar', 'clob',
            'boolean', 'integer',
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
    negation_prefix = STATIC_CONFIG['system']['fetch_pipes_keys']['negation_prefix']
    try:
        params_json = json.dumps(params)
    except Exception as e:
        params_json = str(params)
    bad_words = ['drop', '--', ';']
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
            includes = [item for item in value if not str(item).startswith(negation_prefix)]
            excludes = [item for item in value if str(item).startswith(negation_prefix)]
            if includes:
                where += f"{leading_and}{_key} IN ("
                for item in includes:
                    quoted_item = str(item).replace("'", "''")
                    where += f"'{quoted_item}', "
                where = where[:-2] + ")"
            if excludes:
                where += f"{leading_and}{_key} NOT IN ("
                for item in excludes:
                    item = str(item)[len(negation_prefix):]
                    quoted_item = str(item).replace("'", "''")
                    where += f"'{quoted_item}', "
                where = where[:-2] + ")"
            continue

        ### search a dictionary
        elif isinstance(value, dict):
            import json
            where += (f"{leading_and}CAST({_key} AS TEXT) = '" + json.dumps(value) + "'")
            continue

        eq_sign = '='
        is_null = 'IS NULL'
        if str(value).startswith(negation_prefix):
            value = str(value)[len(negation_prefix):]
            eq_sign = '!='
            if value == 'None':
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
        connector: Optional[meerschaum.connectors.sql.SQLConnector] = None,
        schema: Optional[str] = None,
        debug: bool = False,
    ) -> bool:
    """Check if a table exists.

    Parameters
    ----------
    table: str:
        The name of the table in question.
        
    connector: Optional[meerschaum.connectors.sql.SQLConnector] :
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
    if connector is None:
        from meerschaum import get_connector
        connector = get_connector('sql')

    schema = schema or connector.schema

    table_name = sql_item_name(table, connector.flavor, schema)
    q = exists_queries.get(connector.flavor, exists_queries['default']).format(
        table=table, table_name=table_name,
    )
    exists = connector.exec(q, debug=debug, silent=True) is not None
    return exists


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
    table: str :
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


def get_update_queries(
        target: str,
        patch: str,
        connector: mrsm.connectors.sql.SQLConnector,
        join_cols: Iterable[str],
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

    connector: meerschaum.connectors.sql.SQLConnector
        The Meerschaum `SQLConnector` which will later execute the queries.

    join_cols: List[str]
        The columns to use to join the patch to the target.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A list of query strings to perform the update operation.
    """
    from meerschaum.utils.debug import dprint
    flavor = connector.flavor
    if connector.flavor == 'sqlite' and connector.db_version < '3.33.0':
        flavor = 'sqlite_delete_insert'
    base_queries = update_queries.get(flavor, update_queries['default'])
    if not isinstance(base_queries, list):
        base_queries = [base_queries]
    target_table = get_sqlalchemy_table(target, connector)
    value_cols = []
    if debug:
        dprint(f"target_table.columns: {dict(target_table.columns)}")
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
                l_prefix + sql_item_name(c_name, connector.flavor, None)
                + ' = '
                + ('CAST(' if connector.flavor != 'sqlite' else '')
                + r_prefix
                + sql_item_name(c_name, connector.flavor, None)
                + (' AS ' if connector.flavor != 'sqlite' else '')
                + (c_type.replace('_', ' ') if connector.flavor != 'sqlite' else '')
                + (')' if connector.flavor != 'sqlite' else '')
            ) for c_name, c_type in value_cols
        ])

    def and_subquery(l_prefix: str, r_prefix: str):
        return '\nAND\n'.join([
            (
                l_prefix + sql_item_name(c, connector.flavor, None)
                + ' = '
                + r_prefix + sql_item_name(c, connector.flavor, None)
            ) for c in join_cols
        ])

    return [base_query.format(
        sets_subquery_none = sets_subquery('', 'p.'),
        sets_subquery_f = sets_subquery('f.', 'p.'),
        and_subquery_f = and_subquery('p.', 'f.'),
        and_subquery_t = and_subquery('p.', 't.'),
        target_table_name = sql_item_name(target, connector.flavor, None),
        patch_table_name = sql_item_name(patch, connector.flavor, None),
    ) for base_query in base_queries]

    

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
    if 'int' in typ.lower() or typ == 'numeric':
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
    if 'float' in typ.lower() or 'double' in typ.lower():
        return '-987654321.0'
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

    if flavor == 'duckdb':
        return [
            get_create_table_query(f"SELECT * FROM {old_table_name}", tmp_table, 'duckdb', schema),
            get_create_table_query(f"SELECT * FROM {tmp_table_name}", new_table, 'duckdb', schema),
            f"DROP TABLE {tmp_table_name}",
            f"DROP TABLE {old_table_name}",
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
