#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for working with SQL data types.
"""

from __future__ import annotations
from meerschaum.utils.typing import Dict, Union, Tuple, Optional

NUMERIC_PRECISION_FLAVORS: Dict[str, Tuple[int, int]] = {
    'mariadb': (38, 20),
    'mysql': (38, 20),
    'mssql': (28, 10),
}
NUMERIC_AS_TEXT_FLAVORS = {'sqlite', 'duckdb'}
TIMEZONE_NAIVE_FLAVORS = {'oracle', 'mysql', 'mariadb'}

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
        'CHAR': 'CHAR(36)',  # UUID columns
    },
    'mssql': {
        'NVARCHAR COLLATE "SQL Latin1 General CP1 CI AS"': 'NVARCHAR(MAX)',
        'NVARCHAR COLLATE "SQL_Latin1_General_CP1_CI_AS"': 'NVARCHAR(MAX)',
        'VARCHAR COLLATE "SQL Latin1 General CP1 CI AS"': 'NVARCHAR(MAX)',
        'VARCHAR COLLATE "SQL_Latin1_General_CP1_CI_AS"': 'NVARCHAR(MAX)',
        'NVARCHAR': 'NVARCHAR(MAX)',
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
    'INT': 'int32[pyarrow]',
    'INTEGER': 'int32[pyarrow]',
    'NUMBER': 'numeric',
    'NUMERIC': 'numeric',
    'GEOMETRY': 'geometry',
    'GEOMETRY(GEOMETRY)': 'geometry',
    'TIMESTAMP': 'datetime64[ns]',
    'TIMESTAMP WITHOUT TIMEZONE': 'datetime64[ns]',
    'TIMESTAMP WITH TIMEZONE': 'datetime64[ns, UTC]',
    'TIMESTAMP WITH TIME ZONE': 'datetime64[ns, UTC]',
    'TIMESTAMPTZ': 'datetime64[ns, UTC]',
    'DATE': 'datetime64[ns]',
    'DATETIME': 'datetime64[ns]',
    'DATETIME2': 'datetime64[ns]',
    'DATETIMEOFFSET': 'datetime64[ns, UTC]',
    'TEXT': 'string[pyarrow]',
    'VARCHAR': 'string[pyarrow]',
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
    'UUID': 'uuid',
    'UNIQUEIDENTIFIER': 'uuid',
    'BYTEA': 'bytes',
    'BLOB': 'bytes',
    'VARBINARY': 'bytes',
    'VARBINARY(MAX)': 'bytes',
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
        'BYTE': 'bytes',
        'LOB': 'bytes',
        'BINARY': 'bytes',
        'GEOMETRY': 'geometry',
        'GEOGRAPHY': 'geography',
    },
    'default': 'object',
}
### Map pandas dtypes to flavor-specific dtypes.
PD_TO_DB_DTYPES_FLAVORS: Dict[str, Dict[str, str]] = {
    'int': {
        'timescaledb': 'BIGINT',
        'postgresql': 'BIGINT',
        'postgis': 'BIGINT',
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
    'uint': {
        'timescaledb': 'BIGINT',
        'postgresql': 'BIGINT',
        'postgis': 'BIGINT',
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
    'int8': {
        'timescaledb': 'SMALLINT',
        'postgresql': 'SMALLINT',
        'postgis': 'SMALLINT',
        'mariadb': 'SMALLINT',
        'mysql': 'SMALLINT',
        'mssql': 'SMALLINT',
        'oracle': 'INT',
        'sqlite': 'INT',
        'duckdb': 'SMALLINT',
        'citus': 'SMALLINT',
        'cockroachdb': 'SMALLINT',
        'default': 'INT',
    },
    'uint8': {
        'timescaledb': 'SMALLINT',
        'postgresql': 'SMALLINT',
        'postgis': 'SMALLINT',
        'mariadb': 'SMALLINT',
        'mysql': 'SMALLINT',
        'mssql': 'SMALLINT',
        'oracle': 'INT',
        'sqlite': 'INT',
        'duckdb': 'SMALLINT',
        'citus': 'SMALLINT',
        'cockroachdb': 'SMALLINT',
        'default': 'INT',
    },
    'int16': {
        'timescaledb': 'SMALLINT',
        'postgresql': 'SMALLINT',
        'postgis': 'SMALLINT',
        'mariadb': 'SMALLINT',
        'mysql': 'SMALLINT',
        'mssql': 'SMALLINT',
        'oracle': 'INT',
        'sqlite': 'INT',
        'duckdb': 'SMALLINT',
        'citus': 'SMALLINT',
        'cockroachdb': 'SMALLINT',
        'default': 'INT',
    },
    'int32': {
        'timescaledb': 'INT',
        'postgresql': 'INT',
        'postgis': 'INT',
        'mariadb': 'INT',
        'mysql': 'INT',
        'mssql': 'INT',
        'oracle': 'INT',
        'sqlite': 'INT',
        'duckdb': 'INT',
        'citus': 'INT',
        'cockroachdb': 'INT',
        'default': 'INT',
    },
    'int64': {
        'timescaledb': 'BIGINT',
        'postgresql': 'BIGINT',
        'postgis': 'BIGINT',
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
        'postgis': 'DOUBLE PRECISION',
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
        'postgis': 'DOUBLE PRECISION',
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
        'postgis': 'TIMESTAMP',
        'mariadb': 'DATETIME',
        'mysql': 'DATETIME',
        'mssql': 'DATETIME2',
        'oracle': 'TIMESTAMP(9)',
        'sqlite': 'DATETIME',
        'duckdb': 'TIMESTAMP',
        'citus': 'TIMESTAMP',
        'cockroachdb': 'TIMESTAMP',
        'default': 'DATETIME',
    },
    'datetime64[ns, UTC]': {
        'timescaledb': 'TIMESTAMPTZ',
        'postgresql': 'TIMESTAMPTZ',
        'postgis': 'TIMESTAMPTZ',
        'mariadb': 'DATETIME',
        'mysql': 'DATETIME',
        'mssql': 'DATETIMEOFFSET',
        'oracle': 'TIMESTAMP(9)',
        'sqlite': 'TIMESTAMP',
        'duckdb': 'TIMESTAMPTZ',
        'citus': 'TIMESTAMPTZ',
        'cockroachdb': 'TIMESTAMPTZ',
        'default': 'TIMESTAMPTZ',
    },
    'datetime': {
        'timescaledb': 'TIMESTAMPTZ',
        'postgresql': 'TIMESTAMPTZ',
        'postgis': 'TIMESTAMPTZ',
        'mariadb': 'DATETIME',
        'mysql': 'DATETIME',
        'mssql': 'DATETIMEOFFSET',
        'oracle': 'TIMESTAMP(9)',
        'sqlite': 'TIMESTAMP',
        'duckdb': 'TIMESTAMPTZ',
        'citus': 'TIMESTAMPTZ',
        'cockroachdb': 'TIMESTAMPTZ',
        'default': 'TIMESTAMPTZ',
    },
    'datetimetz': {
        'timescaledb': 'TIMESTAMPTZ',
        'postgresql': 'TIMESTAMPTZ',
        'postgis': 'TIMESTAMPTZ',
        'mariadb': 'DATETIME',
        'mysql': 'DATETIME',
        'mssql': 'DATETIMEOFFSET',
        'oracle': 'TIMESTAMP(9)',
        'sqlite': 'TIMESTAMP',
        'duckdb': 'TIMESTAMPTZ',
        'citus': 'TIMESTAMPTZ',
        'cockroachdb': 'TIMESTAMPTZ',
        'default': 'TIMESTAMPTZ',
    },
    'bool': {
        'timescaledb': 'BOOLEAN',
        'postgresql': 'BOOLEAN',
        'postgis': 'BOOLEAN',
        'mariadb': 'BOOLEAN',
        'mysql': 'BOOLEAN',
        'mssql': 'BIT',
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
        'postgis': 'TEXT',
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
        'postgis': 'TEXT',
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
    'unicode': {
        'timescaledb': 'TEXT',
        'postgresql': 'TEXT',
        'postgis': 'TEXT',
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
        'postgis': 'JSONB',
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
        'postgis': 'NUMERIC',
        'mariadb': f'DECIMAL{NUMERIC_PRECISION_FLAVORS["mariadb"]}',
        'mysql': f'DECIMAL{NUMERIC_PRECISION_FLAVORS["mysql"]}',
        'mssql': f'NUMERIC{NUMERIC_PRECISION_FLAVORS["mssql"]}',
        'oracle': 'NUMBER',
        'sqlite': 'TEXT',
        'duckdb': 'TEXT',
        'citus': 'NUMERIC',
        'cockroachdb': 'NUMERIC',
        'default': 'NUMERIC',
    },
    'uuid': {
        'timescaledb': 'UUID',
        'postgresql': 'UUID',
        'postgis': 'UUID',
        'mariadb': 'CHAR(36)',
        'mysql': 'CHAR(36)',
        'mssql': 'UNIQUEIDENTIFIER',
        ### I know this is too much space, but erring on the side of caution.
        'oracle': 'CHAR(36)',
        'sqlite': 'TEXT',
        'duckdb': 'VARCHAR',
        'citus': 'UUID',
        'cockroachdb': 'UUID',
        'default': 'TEXT',
    },
    'bytes': {
        'timescaledb': 'BYTEA',
        'postgresql': 'BYTEA',
        'postgis': 'BYTEA',
        'mariadb': 'BLOB',
        'mysql': 'BLOB',
        'mssql': 'VARBINARY(MAX)',
        'oracle': 'BLOB',
        'sqlite': 'BLOB',
        'duckdb': 'BLOB',
        'citus': 'BYTEA',
        'cockroachdb': 'BYTEA',
        'default': 'BLOB',
    },
    'geometry': {
        'timescaledb': 'TEXT',
        'postgresql': 'TEXT',
        'postgis': 'GEOMETRY',
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
    'geography': {
        'timescaledb': 'TEXT',
        'postgresql': 'TEXT',
        'postgis': 'GEOGRAPHY',
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
}
PD_TO_SQLALCHEMY_DTYPES_FLAVORS: Dict[str, Dict[str, str]] = {
    'int': {
        'timescaledb': 'BigInteger',
        'postgresql': 'BigInteger',
        'postgis': 'BigInteger',
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
    'uint': {
        'timescaledb': 'BigInteger',
        'postgresql': 'BigInteger',
        'postgis': 'BigInteger',
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
    'int8': {
        'timescaledb': 'SmallInteger',
        'postgresql': 'SmallInteger',
        'postgis': 'SmallInteger',
        'mariadb': 'SmallInteger',
        'mysql': 'SmallInteger',
        'mssql': 'SmallInteger',
        'oracle': 'SmallInteger',
        'sqlite': 'SmallInteger',
        'duckdb': 'SmallInteger',
        'citus': 'SmallInteger',
        'cockroachdb': 'SmallInteger',
        'default': 'SmallInteger',
    },
    'uint8': {
        'timescaledb': 'SmallInteger',
        'postgresql': 'SmallInteger',
        'postgis': 'SmallInteger',
        'mariadb': 'SmallInteger',
        'mysql': 'SmallInteger',
        'mssql': 'SmallInteger',
        'oracle': 'SmallInteger',
        'sqlite': 'SmallInteger',
        'duckdb': 'SmallInteger',
        'citus': 'SmallInteger',
        'cockroachdb': 'SmallInteger',
        'default': 'SmallInteger',
    },
    'int16': {
        'timescaledb': 'SmallInteger',
        'postgresql': 'SmallInteger',
        'postgis': 'SmallInteger',
        'mariadb': 'SmallInteger',
        'mysql': 'SmallInteger',
        'mssql': 'SmallInteger',
        'oracle': 'SmallInteger',
        'sqlite': 'SmallInteger',
        'duckdb': 'SmallInteger',
        'citus': 'SmallInteger',
        'cockroachdb': 'SmallInteger',
        'default': 'SmallInteger',
    },
    'int32': {
        'timescaledb': 'Integer',
        'postgresql': 'Integer',
        'postgis': 'Integer',
        'mariadb': 'Integer',
        'mysql': 'Integer',
        'mssql': 'Integer',
        'oracle': 'Integer',
        'sqlite': 'Integer',
        'duckdb': 'Integer',
        'citus': 'Integer',
        'cockroachdb': 'Integer',
        'default': 'Integer',
    },
    'int64': {
        'timescaledb': 'BigInteger',
        'postgresql': 'BigInteger',
        'postgis': 'BigInteger',
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
        'postgis': 'Float',
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
    'datetime': {
        'timescaledb': 'DateTime(timezone=True)',
        'postgresql': 'DateTime(timezone=True)',
        'postgis': 'DateTime(timezone=True)',
        'mariadb': 'DateTime(timezone=True)',
        'mysql': 'DateTime(timezone=True)',
        'mssql': 'sqlalchemy.dialects.mssql.DATETIMEOFFSET',
        'oracle': 'sqlalchemy.dialects.oracle.TIMESTAMP(timezone=True)',
        'sqlite': 'DateTime(timezone=True)',
        'duckdb': 'DateTime(timezone=True)',
        'citus': 'DateTime(timezone=True)',
        'cockroachdb': 'DateTime(timezone=True)',
        'default': 'DateTime(timezone=True)',
    },
    'datetime64[ns]': {
        'timescaledb': 'DateTime',
        'postgresql': 'DateTime',
        'postgis': 'DateTime',
        'mariadb': 'DateTime',
        'mysql': 'DateTime',
        'mssql': 'sqlalchemy.dialects.mssql.DATETIME2',
        'oracle': 'DateTime',
        'sqlite': 'DateTime',
        'duckdb': 'DateTime',
        'citus': 'DateTime',
        'cockroachdb': 'DateTime',
        'default': 'DateTime',
    },
    'datetime64[ns, UTC]': {
        'timescaledb': 'DateTime(timezone=True)',
        'postgresql': 'DateTime(timezone=True)',
        'postgis': 'DateTime(timezone=True)',
        'mariadb': 'DateTime(timezone=True)',
        'mysql': 'DateTime(timezone=True)',
        'mssql': 'sqlalchemy.dialects.mssql.DATETIMEOFFSET',
        'oracle': 'sqlalchemy.dialects.oracle.TIMESTAMP(timezone=True)',
        'sqlite': 'DateTime(timezone=True)',
        'duckdb': 'DateTime(timezone=True)',
        'citus': 'DateTime(timezone=True)',
        'cockroachdb': 'DateTime(timezone=True)',
        'default': 'DateTime(timezone=True)',
    },
    'bool': {
        'timescaledb': 'Boolean',
        'postgresql': 'Boolean',
        'postgis': 'Boolean',
        'mariadb': 'Integer',
        'mysql': 'Integer',
        'mssql': 'sqlalchemy.dialects.mssql.BIT',
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
        'postgis': 'UnicodeText',
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
        'postgis': 'UnicodeText',
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
        'timescaledb': 'sqlalchemy.dialects.postgresql.JSONB',
        'postgresql': 'sqlalchemy.dialects.postgresql.JSONB',
        'postgis': 'sqlalchemy.dialects.postgresql.JSONB',
        'mariadb': 'UnicodeText',
        'mysql': 'UnicodeText',
        'mssql': 'UnicodeText',
        'oracle': 'UnicodeText',
        'sqlite': 'UnicodeText',
        'duckdb': 'TEXT',
        'citus': 'sqlalchemy.dialects.postgresql.JSONB',
        'cockroachdb': 'sqlalchemy.dialects.postgresql.JSONB',
        'default': 'UnicodeText',
    },
    'numeric': {
        'timescaledb': 'Numeric',
        'postgresql': 'Numeric',
        'postgis': 'Numeric',
        'mariadb': 'Numeric',
        'mysql': 'Numeric',
        'mssql': 'Numeric',
        'oracle': 'Numeric',
        'sqlite': 'UnicodeText',
        'duckdb': 'Numeric',
        'citus': 'Numeric',
        'cockroachdb': 'Numeric',
        'default': 'Numeric',
    },
    'uuid': {
        'timescaledb': 'Uuid',
        'postgresql': 'Uuid',
        'postgis': 'Uuid',
        'mariadb': 'sqlalchemy.dialects.mysql.CHAR(36)',
        'mysql': 'sqlalchemy.dialects.mysql.CHAR(36)',
        'mssql': 'Uuid',
        'oracle': 'sqlalchemy.dialects.oracle.CHAR(36)',
        'sqlite': 'UnicodeText',
        'duckdb': 'UnicodeText',
        'citus': 'Uuid',
        'cockroachdb': 'Uuid',
        'default': 'Uuid',
    },
    'bytes': {
        'timescaledb': 'LargeBinary',
        'postgresql': 'LargeBinary',
        'postgis': 'LargeBinary',
        'mariadb': 'LargeBinary',
        'mysql': 'LargeBinary',
        'mssql': 'LargeBinary',
        'oracle': 'LargeBinary',
        'sqlite': 'LargeBinary',
        'duckdb': 'LargeBinary',
        'citus': 'LargeBinary',
        'cockroachdb': 'LargeBinary',
        'default': 'LargeBinary',
    },
    'geometry': {
        'timescaledb': 'UnicodeText',
        'postgresql': 'UnicodeText',
        'postgis': 'geoalchemy2.Geometry',
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
    'geography': {
        'timescaledb': 'UnicodeText',
        'postgresql': 'UnicodeText',
        'postgis': 'geoalchemy2.Geography',
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
}

AUTO_INCREMENT_COLUMN_FLAVORS: Dict[str, str] = {
    'timescaledb': 'GENERATED BY DEFAULT AS IDENTITY',
    'postgresql': 'GENERATED BY DEFAULT AS IDENTITY',
    'postgis': 'GENERATED BY DEFAULT AS IDENTITY',
    'mariadb': 'AUTO_INCREMENT',
    'mysql': 'AUTO_INCREMENT',
    'mssql': 'IDENTITY(1,1)',
    'oracle': 'GENERATED BY DEFAULT ON NULL AS IDENTITY',
    'sqlite': 'AUTOINCREMENT',
    'duckdb': 'GENERATED BY DEFAULT',
    'citus': 'GENERATED BY DEFAULT',
    'cockroachdb': 'GENERATED BY DEFAULT AS IDENTITY',
    'default': 'GENERATED BY DEFAULT AS IDENTITY',
}


def get_pd_type_from_db_type(db_type: str, allow_custom_dtypes: bool = True) -> str:
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
    from meerschaum.utils.dtypes import are_dtypes_equal, get_geometry_type_srid
    def parse_custom(_pd_type: str, _db_type: str) -> str:
        if 'json' in _db_type.lower():
            return 'json'
        if are_dtypes_equal(_pd_type, 'numeric') and _pd_type != 'object':
            precision, scale = get_numeric_precision_scale(None, dtype=_db_type.upper())
            if precision and scale:
                return f"numeric[{precision},{scale}]"
        if are_dtypes_equal(_pd_type, 'geometry') and _pd_type != 'object':
            geometry_type, srid = get_geometry_type_srid(_db_type.upper())
            modifiers = [str(modifier) for modifier in (geometry_type, srid) if modifier]
            typ = "geometry" if 'geography' not in _pd_type.lower() else 'geography'
            if not modifiers:
                return typ
            return f"{typ}[{', '.join(modifiers)}]"
        return _pd_type

    pd_type = DB_TO_PD_DTYPES.get(db_type.upper().split('(', maxsplit=1)[0].strip(), None)
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
    from meerschaum.utils.dtypes import are_dtypes_equal, MRSM_ALIAS_DTYPES, get_geometry_type_srid
    from meerschaum.utils.misc import parse_arguments_str
    sqlalchemy_types = attempt_import('sqlalchemy.types', lazy=False)

    types_registry = (
        PD_TO_DB_DTYPES_FLAVORS
        if not as_sqlalchemy
        else PD_TO_SQLALCHEMY_DTYPES_FLAVORS
    )

    precision, scale = None, None
    geometry_type, geometry_srid = None, None
    og_pd_type = pd_type
    if pd_type in MRSM_ALIAS_DTYPES:
        pd_type = MRSM_ALIAS_DTYPES[pd_type]

    ### Check whether we are able to match this type (e.g. pyarrow support).
    found_db_type = False
    if (
        pd_type not in types_registry
        and not any(
            pd_type.startswith(f'{typ}[')
            for typ in ('numeric', 'geometry', 'geography')
        )
    ):
        for mapped_pd_type in types_registry:
            if are_dtypes_equal(mapped_pd_type, pd_type):
                pd_type = mapped_pd_type
                found_db_type = True
                break
    elif (pd_type.startswith('geometry[') or pd_type.startswith('geography[')):
        og_pd_type = pd_type
        pd_type = 'geometry' if 'geometry' in pd_type else 'geography'
        geometry_type, geometry_srid = get_geometry_type_srid(og_pd_type)
        found_db_type = True
    elif pd_type.startswith('numeric['):
        og_pd_type = pd_type
        pd_type = 'numeric'
        precision, scale = get_numeric_precision_scale(flavor, og_pd_type)
        found_db_type = True
    else:
        found_db_type = True

    if not found_db_type:
        warn(f"Unknown Pandas data type '{pd_type}'. Falling back to 'TEXT'.", stacklevel=3)
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
        if precision is not None and scale is not None:
            db_type_bare = db_type.split('(', maxsplit=1)[0]
            return f"{db_type_bare}({precision},{scale})"
        if geometry_type is not None and geometry_srid is not None:
            if 'geometry' not in db_type.lower() and 'geography' not in db_type.lower():
                return db_type
            db_type_bare = db_type.split('(', maxsplit=1)[0]
            return f"{db_type_bare}({geometry_type.upper()}, {geometry_srid})"
        return db_type

    if db_type.startswith('sqlalchemy.dialects'):
        dialect, typ_class_name = db_type.replace('sqlalchemy.dialects.', '').split('.', maxsplit=2)
        cls_args, cls_kwargs = None, None
        if '(' in typ_class_name:
            typ_class_name, args_str = typ_class_name.split('(', maxsplit=1)
            args_str = args_str.rstrip(')')
            cls_args, cls_kwargs = parse_arguments_str(args_str)
        sqlalchemy_dialects_flavor_module = attempt_import(f'sqlalchemy.dialects.{dialect}')
        cls = getattr(sqlalchemy_dialects_flavor_module, typ_class_name)
        if cls_args is None:
            return cls
        return cls(*cls_args, **cls_kwargs)

    if 'geometry' in db_type.lower() or 'geography' in db_type.lower():
        geoalchemy2 = attempt_import('geoalchemy2', lazy=False)
        geometry_class = (
            geoalchemy2.Geometry
            if 'geometry' in db_type.lower()
            else geoalchemy2.Geography
        )
        if geometry_type is None or geometry_srid is None:
            return geometry_class
        return geometry_class(geometry_type=geometry_type, srid=geometry_srid)

    if 'numeric' in db_type.lower():
        if precision is None or scale is None:
            return sqlalchemy_types.Numeric
        return sqlalchemy_types.Numeric(precision, scale)

    cls_args, cls_kwargs = None, None
    typ_class_name = db_type
    if '(' in db_type:
        typ_class_name, args_str = db_type.split('(', maxsplit=1)
        args_str = args_str.rstrip(')')
        cls_args, cls_kwargs = parse_arguments_str(args_str)

    cls = getattr(sqlalchemy_types, typ_class_name)
    if cls_args is None:
        return cls
    return cls(*cls_args, **cls_kwargs)


def get_numeric_precision_scale(
    flavor: str,
    dtype: Optional[str] = None,
) -> Union[Tuple[int, int], Tuple[None, None]]:
    """
    Return the precision and scale to use for a numeric column for a given database flavor.

    Parameters
    ----------
    flavor: str
        The database flavor for which to return the precision and scale.
    
    dtype: Optional[str], default None
        If provided, return the precision and scale provided in the dtype (if applicable).
        If all caps, treat this as a DB type.

    Returns
    -------
    A tuple of ints or a tuple of Nones.
    """
    if not dtype:
        return None, None

    lbracket = '[' if '[' in dtype else '('
    rbracket = ']' if lbracket == '[' else ')'
    if lbracket in dtype and dtype.count(',') == 1 and dtype.endswith(rbracket):
        try:
            parts = dtype.split(lbracket, maxsplit=1)[-1].rstrip(rbracket).split(',', maxsplit=1)
            return int(parts[0].strip()), int(parts[1].strip())
        except Exception:
            pass

    return NUMERIC_PRECISION_FLAVORS.get(flavor, (None, None))
