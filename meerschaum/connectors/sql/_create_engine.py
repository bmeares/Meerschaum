#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains the logic that builds the sqlalchemy engine string.
"""

import traceback
import meerschaum as mrsm
from meerschaum.utils.debug import dprint

### determine driver and requirements from flavor
default_requirements = {
    'username',
    'password',
    'host',
    'database',
}

### NOTE: These are defined in the `system.json` config file and so this dictionary's values
### will all be overwritten if applicable.
default_create_engine_args = {
    #  'method': 'multi',
    'pool_size': 5,
    'max_overflow': 10,
    'pool_recycle': 3600,
    'connect_args': {},
}
flavor_configs = {
    'timescaledb': {
        'engine': 'postgresql+psycopg',
        'create_engine': default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql': {},
        'requirements': default_requirements,
        'defaults': {
            'port': 5432,
        },
    },
    'postgresql': {
        'engine': 'postgresql+psycopg',
        'create_engine': default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql': {},
        'requirements': default_requirements,
        'defaults': {
            'port': 5432,
        },
    },
    'postgis': {
        'engine': 'postgresql+psycopg',
        'create_engine': default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql': {},
        'requirements': default_requirements,
        'defaults': {
            'port': 5432,
        },
    },
    'citus': {
        'engine': 'postgresql+psycopg',
        'create_engine': default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql': {},
        'requirements': default_requirements,
        'defaults': {
            'port': 5432,
        },
    },
    'mssql': {
        'engine': 'mssql+pyodbc',
        'create_engine': {
            'fast_executemany': True,
            'use_insertmanyvalues': False,
            'isolation_level': 'AUTOCOMMIT',
            'use_setinputsizes': False,
            'pool_pre_ping': True,
            'ignore_no_transaction_on_rollback': True,
        },
        'omit_create_engine': {'method',},
        'to_sql': {
            'method': None,
        },
        'requirements': default_requirements,
        'defaults': {
            'port': 1433,
            'options': (
                "driver=ODBC Driver 18 for SQL Server"
                "&UseFMTONLY=Yes"
                "&TrustServerCertificate=yes"
                "&Encrypt=no"
                "&MARS_Connection=yes"
            ),
        },
    },
    'mysql': {
        'engine': 'mysql+pymysql',
        'create_engine': default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql': {
            'method': 'multi',
        },
        'requirements': default_requirements,
        'defaults': {
            'port': 3306,
        },
    },
    'mariadb': {
        'engine': 'mysql+pymysql',
        'create_engine': default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql': {
            'method': 'multi',
        },
        'requirements': default_requirements,
        'defaults': {
            'port': 3306,
        },
    },
    'oracle': {
        'engine': 'oracle+oracledb',
        'create_engine': default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql': {
            'method': None,
        },
        'requirements': default_requirements,
        'defaults': {
            'port': 1521,
        },
    },
    'sqlite': {
        'engine': 'sqlite',
        'create_engine': default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql': {
            'method': 'multi',
        },
        'requirements': {'database'},
        'defaults': {},
    },
    'duckdb': {
        'engine': 'duckdb',
        'create_engine': {},
        'omit_create_engine': {'ALL',},
        'to_sql': {
            'method': 'multi',
        },
        'requirements': '',
        'defaults': {},
    },
    'cockroachdb': {
        'engine': 'cockroachdb',
        'omit_create_engine': {'method',},
        'create_engine': default_create_engine_args,
        'to_sql': {
            'method': 'multi',
        },
        'requirements': {'host'},
        'defaults': {
            'port': 26257,
            'database': 'defaultdb',
            'username': 'root',
            'password': 'admin',
        },
    },
}
install_flavor_drivers = {
    'sqlite': ['aiosqlite'],
    'duckdb': ['duckdb', 'duckdb_engine'],
    'mysql': ['pymysql'],
    'mariadb': ['pymysql'],
    'timescaledb': ['psycopg'],
    'postgresql': ['psycopg'],
    'postgis': ['psycopg', 'geoalchemy'],
    'citus': ['psycopg'],
    'cockroachdb': ['psycopg', 'sqlalchemy_cockroachdb', 'sqlalchemy_cockroachdb.psycopg'],
    'mssql': ['pyodbc'],
    'oracle': ['oracledb'],
}
require_patching_flavors = {'cockroachdb': [('sqlalchemy-cockroachdb', 'sqlalchemy_cockroachdb')]}

flavor_dialects = {
    'cockroachdb': (
        'cockroachdb', 'sqlalchemy_cockroachdb.psycopg', 'CockroachDBDialect_psycopg'
    ),
    'duckdb': ('duckdb', 'duckdb_engine', 'Dialect'),
}


def create_engine(
    self,
    include_uri: bool = False,
    debug: bool = False,
    **kw
) -> 'sqlalchemy.engine.Engine':
    """Create a sqlalchemy engine by building the engine string."""
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.warnings import error, warn
    sqlalchemy = attempt_import('sqlalchemy', lazy=False)
    import urllib
    import copy
    ### Install and patch required drivers.
    if self.flavor in install_flavor_drivers:
        _ = attempt_import(
            *install_flavor_drivers[self.flavor],
            debug=debug,
            lazy=False,
            warn=False,
        )
        if self.flavor == 'mssql':
            _init_mssql_sqlalchemy()
    if self.flavor in require_patching_flavors:
        from meerschaum.utils.packages import determine_version, _monkey_patch_get_distribution
        import pathlib
        for install_name, import_name in require_patching_flavors[self.flavor]:
            pkg = attempt_import(
                import_name,
                debug=debug,
                lazy=False,
                warn=False
            )
            _monkey_patch_get_distribution(
                install_name, determine_version(pathlib.Path(pkg.__file__), venv='mrsm')
            )

    ### supplement missing values with defaults (e.g. port number)
    for a, value in flavor_configs[self.flavor]['defaults'].items():
        if a not in self.__dict__:
            self.__dict__[a] = value

    ### Verify that everything is in order.
    if self.flavor not in flavor_configs:
        error(f"Cannot create a connector with the flavor '{self.flavor}'.")

    _engine = flavor_configs[self.flavor].get('engine', None)
    _username = self.__dict__.get('username', None)
    _password = self.__dict__.get('password', None)
    _host = self.__dict__.get('host', None)
    _port = self.__dict__.get('port', None)
    _database = self.__dict__.get('database', None)
    _options = self.__dict__.get('options', {})
    if isinstance(_options, str):
        _options = dict(urllib.parse.parse_qsl(_options))
    _uri = self.__dict__.get('uri', None)

    ### Handle registering specific dialects (due to installing in virtual environments).
    if self.flavor in flavor_dialects:
        sqlalchemy.dialects.registry.register(*flavor_dialects[self.flavor])

    ### self._sys_config was deepcopied and can be updated safely
    if self.flavor in ("sqlite", "duckdb"):
        engine_str = f"{_engine}:///{_database}" if not _uri else _uri
        if 'create_engine' not in self._sys_config:
            self._sys_config['create_engine'] = {}
        if 'connect_args' not in self._sys_config['create_engine']:
            self._sys_config['create_engine']['connect_args'] = {}
        self._sys_config['create_engine']['connect_args'].update({"check_same_thread": False})
    else:
        engine_str = (
            _engine + "://" + (_username if _username is not None else '') +
            ((":" + urllib.parse.quote_plus(_password)) if _password is not None else '') +
            "@" + _host + ((":" + str(_port)) if _port is not None else '') +
            (("/" + _database) if _database is not None else '')
            + (("?" + urllib.parse.urlencode(_options)) if _options else '')
        ) if not _uri else _uri

        ### Sometimes the timescaledb:// flavor can slip in.
        if _uri and self.flavor in _uri:
            if self.flavor in ('timescaledb', 'postgis'):
                engine_str = engine_str.replace(self.flavor, 'postgresql', 1)
            elif _uri.startswith('postgresql://'):
                engine_str = engine_str.replace('postgresql://', 'postgresql+psycopg2://')

    if debug:
        dprint(
            (
                (engine_str.replace(':' + _password, ':' + ('*' * len(_password))))
                    if _password is not None else engine_str
            ) + '\n' + f"{self._sys_config.get('create_engine', {}).get('connect_args', {})}"
        )

    _kw_copy = copy.deepcopy(kw)

    ### NOTE: Order of inheritance:
    ###       1. Defaults
    ###       2. System configuration
    ###       3. Connector configuration
    ###       4. Keyword arguments
    _create_engine_args = flavor_configs.get(self.flavor, {}).get('create_engine', {})
    def _apply_create_engine_args(update):
        if 'ALL' not in flavor_configs[self.flavor].get('omit_create_engine', {}):
            _create_engine_args.update(
                { k: v for k, v in update.items()
                    if 'omit_create_engine' not in flavor_configs[self.flavor]
                        or k not in flavor_configs[self.flavor].get('omit_create_engine')
                }
            )
    _apply_create_engine_args(self._sys_config.get('create_engine', {}))
    _apply_create_engine_args(self.__dict__.get('create_engine', {}))
    _apply_create_engine_args(_kw_copy)

    try:
        engine = sqlalchemy.create_engine(
            engine_str,
            ### I know this looks confusing, and maybe it's bad code,
            ### but it's simple. It dynamically parses the config string
            ### and splits it to separate the class name (QueuePool)
            ### from the module name (sqlalchemy.pool).
            poolclass    = getattr(
                attempt_import(
                    ".".join(self._sys_config['poolclass'].split('.')[:-1])
                ),
                self._sys_config['poolclass'].split('.')[-1]
            ),
            echo         = debug,
            **_create_engine_args
        )
    except Exception:
        warn(f"Failed to create connector '{self}':\n{traceback.format_exc()}", stack=False)
        engine = None

    if include_uri:
        return engine, engine_str
    return engine


def _init_mssql_sqlalchemy():
    """
    When first instantiating a SQLAlchemy connection to MSSQL,
    monkey-patch `pyodbc` handling in SQLAlchemy.
    """
    pyodbc, sqlalchemy_dialects_mssql_pyodbc = mrsm.attempt_import(
        'pyodbc',
        'sqlalchemy.dialects.mssql.pyodbc',
        lazy=False,
        warn=False,
    )
    pyodbc.pooling = False

    MSDialect_pyodbc = sqlalchemy_dialects_mssql_pyodbc.MSDialect_pyodbc

    def _handle_geometry(val):
        from binascii import hexlify
        hex_str = f"0x{hexlify(val).decode().upper()}"
        return hex_str

    def custom_on_connect(self):
        super_ = super(MSDialect_pyodbc, self).on_connect()

        def _on_connect(conn):
            if super_ is not None:
                super_(conn)

            self._setup_timestampoffset_type(conn)
            conn.add_output_converter(-151, _handle_geometry)

        return _on_connect

    ### TODO: Parse proprietary MSSQL geometry bytes into WKB.
    #  MSDialect_pyodbc.on_connect = custom_on_connect
