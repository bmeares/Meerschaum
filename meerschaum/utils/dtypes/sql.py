#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for working with SQL data types.
"""

from __future__ import annotations
from meerschaum.utils.typing import Dict, Union, Tuple

NUMERIC_PRECISION_FLAVORS: Dict[str, Tuple[int, int]] = {
    'mariadb': (38, 20),
    'mysql': (38, 20),
    'mssql': (28, 10),
    'duckdb': (15, 4),
    'sqlite': (15, 4),
}

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
        'INT': 'SIGNED INT',
        'INTEGER': 'SIGNED INT',
        'BOOL': 'SIGNED INT',
        'BOOLEAN': 'SIGNED INT',
        'DOUBLE PRECISION': 'DECIMAL',
        'DOUBLE': 'DECIMAL',
        'FLOAT': 'DECIMAL',
    },
    'sqlite': {
        'BOOLEAN': 'INTEGER',
        'REAL': 'FLOAT',
    },
    'oracle': {
        'NVARCHAR(2000)': 'NVARCHAR2(2000)',
        'NVARCHAR': 'NVARCHAR2(2000)',
        'NVARCHAR2': 'NVARCHAR2(2000)',
    },
    'mssql': {
        'NVARCHAR COLLATE "SQL Latin1 General CP1 CI AS"': 'NVARCHAR(MAX)',
        'NVARCHAR COLLATE "SQL_Latin1_General_CP1_CI_AS"': 'NVARCHAR(MAX)',
        'VARCHAR COLLATE "SQL Latin1 General CP1 CI AS"': 'NVARCHAR(MAX)',
        'VARCHAR COLLATE "SQL_Latin1_General_CP1_CI_AS"': 'NVARCHAR(MAX)',
    },
}
for _flavor, (_precision, _scale) in NUMERIC_PRECISION_FLAVORS.items():
    if _flavor not in DB_FLAVORS_CAST_DTYPES:
        DB_FLAVORS_CAST_DTYPES[_flavor] = {}
    DB_FLAVORS_CAST_DTYPES[_flavor].update({
        'NUMERIC': f"NUMERIC({_precision}, {_scale})",
        'DECIMAL': f"DECIMAL({_precision}, {_scale})",
    })

DB_TO_PD_DTYPES: Dict[str, Union[str, Dict[str, str]]] = {
    'FLOAT': 'float64[pyarrow]',
    'REAL': 'float64[pyarrow]',
    'DOUBLE_PRECISION': 'float64[pyarrow]',
    'DOUBLE': 'float64[pyarrow]',
    'DECIMAL': 'numeric',
    'BIGINT': 'int64[pyarrow]',
    'INT': 'int64[pyarrow]',
    'INTEGER': 'int64[pyarrow]',
    'NUMBER': 'numeric',
    'NUMERIC': 'numeric',
    'TIMESTAMP': 'datetime64[ns]',
    'TIMESTAMP WITH TIMEZONE': 'datetime64[ns, UTC]',
    'TIMESTAMPTZ': 'datetime64[ns, UTC]',
    'DATE': 'datetime64[ns]',
    'DATETIME': 'datetime64[ns]',
    'TEXT': 'string[pyarrow]',
    'CLOB': 'string[pyarrow]',
    'BOOL': 'bool[pyarrow]',
    'BOOLEAN': 'bool[pyarrow]',
    'BOOLEAN()': 'bool[pyarrow]',
    'TINYINT': 'bool[pyarrow]',
    'TINYINT(1)': 'bool[pyarrow]',
    'BIT': 'bool[pyarrow]',
    'BIT(1)': 'bool[pyarrow]',
    'JSON': 'json',
    'JSONB': 'json',
    'substrings': {
        'CHAR': 'string[pyarrow]',
        'TIMESTAMP': 'datetime64[ns]',
        'TIME': 'datetime64[ns]',
        'DATE': 'datetime64[ns]',
        'DOUBLE': 'double[pyarrow]',
        'DECIMAL': 'numeric',
        'NUMERIC': 'numeric',
        'NUMBER': 'numeric',
        'INT': 'int64[pyarrow]',
        'BOOL': 'bool[pyarrow]',
        'JSON': 'json',
    },
    'default': 'object',
}
### Map pandas dtypes to flavor-specific dtypes.
PD_TO_DB_DTYPES_FLAVORS: Dict[str, Dict[str, str]] = {
    'int': {
        'timescaledb': 'BIGINT',
        'postgresql': 'BIGINT',
        'mariadb': 'BIGINT',
        'mysql': 'BIGINT',
        'mssql': 'BIGINT',
        'oracle': 'INT',
        'sqlite': 'BIGINT',
        'duckdb': 'BIGINT',
        'citus': 'BIGINT',
        'cockroachdb': 'BIGINT',
        'default': 'INT',
    },
    'float': {
        'timescaledb': 'DOUBLE PRECISION',
        'postgresql': 'DOUBLE PRECISION',
        'mariadb': 'DOUBLE PRECISION',
        'mysql': 'DOUBLE PRECISION',
        'mssql': 'FLOAT',
        'oracle': 'FLOAT',
        'sqlite': 'FLOAT',
        'duckdb': 'DOUBLE PRECISION',
        'citus': 'DOUBLE PRECISION',
        'cockroachdb': 'DOUBLE PRECISION',
        'default': 'DOUBLE',
    },
    'double': {
        'timescaledb': 'DOUBLE PRECISION',
        'postgresql': 'DOUBLE PRECISION',
        'mariadb': 'DOUBLE PRECISION',
        'mysql': 'DOUBLE PRECISION',
        'mssql': 'FLOAT',
        'oracle': 'FLOAT',
        'sqlite': 'FLOAT',
        'duckdb': 'DOUBLE PRECISION',
        'citus': 'DOUBLE PRECISION',
        'cockroachdb': 'DOUBLE PRECISION',
        'default': 'DOUBLE',
    },
    'datetime64[ns]': {
        'timescaledb': 'TIMESTAMP',
        'postgresql': 'TIMESTAMP',
        'mariadb': 'DATETIME',
        'mysql': 'DATETIME',
        'mssql': 'DATETIME',
        'oracle': 'DATE',
        'sqlite': 'DATETIME',
        'duckdb': 'TIMESTAMP',
        'citus': 'TIMESTAMP',
        'cockroachdb': 'TIMESTAMP',
        'default': 'DATETIME',
    },
    'datetime64[ns, UTC]': {
        'timescaledb': 'TIMESTAMP',
        'postgresql': 'TIMESTAMP',
        'mariadb': 'TIMESTAMP',
        'mysql': 'TIMESTAMP',
        'mssql': 'TIMESTAMP',
        'oracle': 'TIMESTAMP',
        'sqlite': 'TIMESTAMP',
        'duckdb': 'TIMESTAMP',
        'citus': 'TIMESTAMP',
        'cockroachdb': 'TIMESTAMP',
        'default': 'TIMESTAMP',
    },
    'bool': {
        'timescaledb': 'BOOLEAN',
        'postgresql': 'BOOLEAN',
        'mariadb': 'BOOLEAN',
        'mysql': 'BOOLEAN',
        'mssql': 'INTEGER',
        'oracle': 'INTEGER',
        'sqlite': 'FLOAT',
        'duckdb': 'BOOLEAN',
        'citus': 'BOOLEAN',
        'cockroachdb': 'BOOLEAN',
        'default': 'BOOLEAN',
    },
    'object': {
        'timescaledb': 'TEXT',
        'postgresql': 'TEXT',
        'mariadb': 'TEXT',
        'mysql': 'TEXT',
        'mssql': 'NVARCHAR(MAX)',
        'oracle': 'NVARCHAR2(2000)',
        'sqlite': 'TEXT',
        'duckdb': 'TEXT',
        'citus': 'TEXT',
        'cockroachdb': 'TEXT',
        'default': 'TEXT',
    },
    'string': {
        'timescaledb': 'TEXT',
        'postgresql': 'TEXT',
        'mariadb': 'TEXT',
        'mysql': 'TEXT',
        'mssql': 'NVARCHAR(MAX)',
        'oracle': 'NVARCHAR2(2000)',
        'sqlite': 'TEXT',
        'duckdb': 'TEXT',
        'citus': 'TEXT',
        'cockroachdb': 'TEXT',
        'default': 'TEXT',
    },
    'json': {
        'timescaledb': 'JSONB',
        'postgresql': 'JSONB',
        'mariadb': 'TEXT',
        'mysql': 'TEXT',
        'mssql': 'NVARCHAR(MAX)',
        'oracle': 'NVARCHAR2(2000)',
        'sqlite': 'TEXT',
        'duckdb': 'TEXT',
        'citus': 'JSONB',
        'cockroachdb': 'JSONB',
        'default': 'TEXT',
    },
    'numeric': {
        'timescaledb': 'NUMERIC',
        'postgresql': 'NUMERIC',
        'mariadb': f'DECIMAL{NUMERIC_PRECISION_FLAVORS["mariadb"]}',
        'mysql': f'DECIMAL{NUMERIC_PRECISION_FLAVORS["mysql"]}',
        'mssql': f'NUMERIC{NUMERIC_PRECISION_FLAVORS["mssql"]}',
        'oracle': 'NUMBER',
        'sqlite': f'DECIMAL{NUMERIC_PRECISION_FLAVORS["sqlite"]}',
        'duckdb': 'NUMERIC',
        'citus': 'NUMERIC',
        'cockroachdb': 'NUMERIC',
        'default': 'NUMERIC',
    },
}
PD_TO_SQLALCHEMY_DTYPES_FLAVORS: Dict[str, Dict[str, str]] = {
    'int': {
        'timescaledb': 'BigInteger',
        'postgresql': 'BigInteger',
        'mariadb': 'BigInteger',
        'mysql': 'BigInteger',
        'mssql': 'BigInteger',
        'oracle': 'BigInteger',
        'sqlite': 'BigInteger',
        'duckdb': 'BigInteger',
        'citus': 'BigInteger',
        'cockroachdb': 'BigInteger',
        'default': 'BigInteger',
    },
    'float': {
        'timescaledb': 'Float',
        'postgresql': 'Float',
        'mariadb': 'Float',
        'mysql': 'Float',
        'mssql': 'Float',
        'oracle': 'Float',
        'sqlite': 'Float',
        'duckdb': 'Float',
        'citus': 'Float',
        'cockroachdb': 'Float',
        'default': 'Float',
    },
    'datetime64[ns]': {
        'timescaledb': 'DateTime',
        'postgresql': 'DateTime',
        'mariadb': 'DateTime',
        'mysql': 'DateTime',
        'mssql': 'DateTime',
        'oracle': 'DateTime',
        'sqlite': 'DateTime',
        'duckdb': 'DateTime',
        'citus': 'DateTime',
        'cockroachdb': 'DateTime',
        'default': 'DateTime',
    },
    'datetime64[ns, UTC]': {
        'timescaledb': 'DateTime',
        'postgresql': 'DateTime',
        'mariadb': 'DateTime',
        'mysql': 'DateTime',
        'mssql': 'DateTime',
        'oracle': 'DateTime',
        'sqlite': 'DateTime',
        'duckdb': 'DateTime',
        'citus': 'DateTime',
        'cockroachdb': 'DateTime',
        'default': 'DateTime',
    },
    'bool': {
        'timescaledb': 'Boolean',
        'postgresql': 'Boolean',
        'mariadb': 'Integer',
        'mysql': 'Integer',
        'mssql': 'Integer',
        'oracle': 'Integer',
        'sqlite': 'Float',
        'duckdb': 'Boolean',
        'citus': 'Boolean',
        'cockroachdb': 'Boolean',
        'default': 'Boolean',
    },
    'object': {
        'timescaledb': 'UnicodeText',
        'postgresql': 'UnicodeText',
        'mariadb': 'UnicodeText',
        'mysql': 'UnicodeText',
        'mssql': 'UnicodeText',
        'oracle': 'UnicodeText',
        'sqlite': 'UnicodeText',
        'duckdb': 'UnicodeText',
        'citus': 'UnicodeText',
        'cockroachdb': 'UnicodeText',
        'default': 'UnicodeText',
    },
    'string': {
        'timescaledb': 'UnicodeText',
        'postgresql': 'UnicodeText',
        'mariadb': 'UnicodeText',
        'mysql': 'UnicodeText',
        'mssql': 'UnicodeText',
        'oracle': 'UnicodeText',
        'sqlite': 'UnicodeText',
        'duckdb': 'UnicodeText',
        'citus': 'UnicodeText',
        'cockroachdb': 'UnicodeText',
        'default': 'UnicodeText',
    },
    'json': {
        'timescaledb': 'JSONB',
        'postgresql': 'JSONB',
        'mariadb': 'UnicodeText',
        'mysql': 'UnicodeText',
        'mssql': 'UnicodeText',
        'oracle': 'UnicodeText',
        'sqlite': 'UnicodeText',
        'duckdb': 'TEXT',
        'citus': 'JSONB',
        'cockroachdb': 'JSONB',
        'default': 'UnicodeText',
    },
    'numeric': {
        'timescaledb': 'Numeric',
        'postgresql': 'Numeric',
        'mariadb': 'Numeric',
        'mysql': 'Numeric',
        'mssql': 'Numeric',
        'oracle': 'Numeric',
        'sqlite': 'Numeric',
        'duckdb': 'Numeric',
        'citus': 'Numeric',
        'cockroachdb': 'Numeric',
        'default': 'Numeric',
    },
}


def get_pd_type_from_db_type(db_type: str, allow_custom_dtypes: bool = False) -> str:
    """
    Parse a database type to a pandas data type.

    Parameters
    ----------
    db_type: str
        The database type, e.g. `DATETIME`, `BIGINT`, etc.

    allow_custom_dtypes: bool, default False
        If `True`, allow for custom data types like `json` and `str`.

    Returns
    -------
    The equivalent datatype for a pandas DataFrame.
    """
    def parse_custom(_pd_type: str, _db_type: str) -> str:
        if 'json' in _db_type.lower():
            return 'json'
        return _pd_type

    pd_type = DB_TO_PD_DTYPES.get(db_type.upper(), None)
    if pd_type is not None:
        return (
            parse_custom(pd_type, db_type)
            if allow_custom_dtypes
            else pd_type
        )
    for db_t, pd_t in DB_TO_PD_DTYPES['substrings'].items():
        if db_t in db_type.upper():
            return (
                parse_custom(pd_t, db_t)
                if allow_custom_dtypes
                else pd_t
            )
    return DB_TO_PD_DTYPES['default']


def get_db_type_from_pd_type(
        pd_type: str,
        flavor: str = 'default',
        as_sqlalchemy: bool = False,
    ) -> Union[str, 'sqlalchemy.sql.visitors.TraversibleType']:
    """
    Parse a Pandas data type into a flavor's database type.

    Parameters
    ----------
    pd_type: str
        The Pandas datatype. This must be a string, not the actual dtype object.

    flavor: str, default 'default'
        The flavor of the database to be mapped to.

    as_sqlalchemy: bool, default False
        If `True`, return a type from `sqlalchemy.types`.

    Returns
    -------
    The database data type for the incoming Pandas data type.
    If nothing can be found, a warning will be thrown and 'TEXT' will be returned.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.dtypes import are_dtypes_equal
    sqlalchemy_types = attempt_import('sqlalchemy.types')

    types_registry = (
        PD_TO_DB_DTYPES_FLAVORS
        if not as_sqlalchemy
        else PD_TO_SQLALCHEMY_DTYPES_FLAVORS
    )

    ### Check whether we are able to match this type (e.g. pyarrow support).
    found_db_type = False
    if pd_type not in types_registry:
        for mapped_pd_type in types_registry:
            if are_dtypes_equal(mapped_pd_type, pd_type):
                pd_type = mapped_pd_type
                found_db_type = True
                break
    else:
        found_db_type = True

    if not found_db_type:
        warn(f"Unknown Pandas data type '{pd_type}'. Falling back to 'TEXT'.")
        return (
            'TEXT'
            if not as_sqlalchemy
            else sqlalchemy_types.UnicodeText
        )
    flavor_types = types_registry.get(
        pd_type,
        {
            'default': (
                'TEXT'
                if not as_sqlalchemy
                else 'UnicodeText'
            ),
        },
    )
    default_flavor_type = flavor_types.get(
        'default',
        (
            'TEXT'
            if not as_sqlalchemy
            else 'UnicodeText'
        ),
    )
    if flavor not in flavor_types:
        warn(f"Unknown flavor '{flavor}'. Falling back to '{default_flavor_type}' (default).")
    db_type = flavor_types.get(flavor, default_flavor_type)
    if not as_sqlalchemy:
        return db_type
    if db_type == 'JSONB':
        sqlalchemy_dialects_postgresql = attempt_import('sqlalchemy.dialects.postgresql')
        return sqlalchemy_dialects_postgresql.JSONB
    if 'numeric' in db_type.lower():
        numeric_type_str = PD_TO_DB_DTYPES_FLAVORS['numeric'].get(flavor, 'NUMERIC')
        if flavor not in NUMERIC_PRECISION_FLAVORS:
            return sqlalchemy_types.Numeric
        precision, scale = NUMERIC_PRECISION_FLAVORS[flavor]
        return sqlalchemy_types.Numeric(precision, scale)
    return getattr(sqlalchemy_types, db_type)
