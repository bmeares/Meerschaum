#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains the logic that builds the sqlalchemy engine string.
"""

import traceback

import meerschaum as mrsm
from meerschaum.utils.debug import dprint
from meerschaum._internal.static import STATIC_CONFIG


### NOTE: These are defined in the `system.json` config file and so this dictionary's values
### will all be overwritten if applicable.

flavor_configs = STATIC_CONFIG['sql']['create_engine_flavors']


def import_sqlalchemy(*names: str):
    """
    Import SQLAlchemy modules (`sqlalchemy` by default) for what stays SQLAlchemy (`.engine`,
    `exec(..., with_connection=True)`, connectors with `native_sql: false`; raw SQL otherwise
    runs on the Rust connector, rc15 Q53).

    Raises
    ------
    An `ImportError` naming the `meerschaum[sql]` extra when the package is not installed (and
    could not be installed: `MRSM_NO_AUTO_INSTALL`).
    """
    from meerschaum.utils.packages import attempt_import
    names = names or ('sqlalchemy',)
    modules = attempt_import(*names, lazy=False, warn=False)
    imported = modules if len(names) > 1 else (modules,)
    if any(module is None for module in imported):
        raise ImportError(
            "`.engine` (and raw SQL with `native_sql: false`) needs SQLAlchemy: "
            "install the `sql` extra with `pip install 'meerschaum[sql]'`."
        )
    return modules


def import_pandas_for_sql():
    """Import pandas for `read` / `to_sql`, or raise an `ImportError` naming the extra."""
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()
    if pd is None:
        raise ImportError(
            "`read()` and `to_sql()` use pandas: "
            "install the `pandas` extra with `pip install 'meerschaum[pandas]'`."
        )
    return pd
install_flavor_drivers = {
    'duckdb': ['duckdb', 'duckdb_engine'],
    'mysql': ['pymysql'],
    'mariadb': ['pymysql'],
    'timescaledb': ['psycopg', 'psycopg2'],
    'timescaledb-ha': ['psycopg', 'geoalchemy2'],
    'postgresql': ['psycopg', 'psycopg2'],
    'postgis': ['psycopg', 'psycopg2', 'geoalchemy2'],
    'citus': ['psycopg', 'psycopg2'],
    'cockroachdb': ['psycopg', 'psycopg2', 'sqlalchemy_cockroachdb', 'sqlalchemy_cockroachdb.psycopg'],
    'mssql': ['pyodbc'],
    'oracle': ['oracledb'],
}

flavor_dialects = {
    'cockroachdb': (
        'cockroachdb', 'sqlalchemy_cockroachdb.psycopg', 'CockroachDBDialect_psycopg'
    ),
    'duckdb': ('duckdb', 'duckdb_engine', 'Dialect'),
}


def get_engine_str(self) -> str:
    """
    Return the SQLAlchemy URI of the connector, built from its attributes (no SQLAlchemy needed:
    `SQLConnector.URI`, `DATABASE_URL` and `IS_THREAD_SAFE` read it).
    """
    import urllib.parse
    from meerschaum.utils.warnings import error

    ### supplement missing values with defaults (e.g. port number)
    if self.flavor not in flavor_configs:
        error(f"Cannot create a connector with the flavor '{self.flavor}'.")
    for a, value in flavor_configs[self.flavor]['defaults'].items():
        if a not in self.__dict__:
            self.__dict__[a] = value

    _engine = flavor_configs[self.flavor].get('engine', None)
    _username = self.__dict__.get('username', None)
    _password = self.__dict__.get('password', None)
    _host = self.__dict__.get('host', None)
    _port = self.__dict__.get('port', None)
    _database = self.__dict__.get('database', None)
    if _database == '{SQLITE_DB_PATH}':
        import meerschaum.config.paths as paths
        _database = paths.SQLITE_DB_PATH.as_posix()
    _options = self.__dict__.get('options', {})
    if isinstance(_options, str):
        _options = dict(urllib.parse.parse_qsl(_options))
    _uri = self.__dict__.get('uri', None)

    if self.flavor in ("sqlite", "duckdb", "geopackage"):
        engine_str = f"{_engine}:///{_database}" if not _uri else _uri
        _shared_memory_getter = getattr(self, '_shared_memory_database', None)
        if (
            self.flavor == 'sqlite'
            and not _uri
            and _shared_memory_getter is not None
            and (_shared_memory := _shared_memory_getter()) is not None
        ):
            ### One database file for SQLAlchemy and the Rust pool (see `_shared_memory_database`).
            engine_str = f"{_engine}:///{_shared_memory}"
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
            if self.flavor in ('timescaledb', 'timescaledb-ha', 'postgis'):
                engine_str = engine_str.replace(self.flavor, 'postgresql', 1)
            elif _uri.startswith('postgresql://'):
                engine_str = engine_str.replace('postgresql://', 'postgresql+psycopg2://')
    return engine_str


def create_engine(
    self,
    include_uri: bool = False,
    debug: bool = False,
    **kw
) -> 'sqlalchemy.engine.Engine':
    """Create a sqlalchemy engine by building the engine string."""
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.warnings import error, warn
    if self.flavor == 'duckdb':
        from ._duckdb_transport import guard_sdk, validate_connector_uri
        guard_sdk()
        validate_connector_uri(self)
        for source in (kw, self._sys_config.get('create_engine', {})):
            if any(source.get(key) is not None for key in ('creator', 'module')):
                raise RuntimeError('DuckDB custom creator/module overrides cannot share the native driver safely.')
    sqlalchemy = import_sqlalchemy()
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

    engine_str = get_engine_str(self)
    _password = self.__dict__.get('password', None)

    ### Handle registering specific dialects (due to installing in virtual environments).
    if self.flavor in flavor_dialects:
        sqlalchemy.dialects.registry.register(*flavor_dialects[self.flavor])

    if self.flavor in ("sqlite", "geopackage"):
        if 'create_engine' not in self._sys_config:
            self._sys_config['create_engine'] = {}
        if 'connect_args' not in self._sys_config['create_engine']:
            self._sys_config['create_engine']['connect_args'] = {}
        self._sys_config['create_engine']['connect_args'].update({"check_same_thread": False})

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
    # Flavor defaults may share nested dictionaries across SQL dialects. Per-connector
    # overrides must not mutate STATIC_CONFIG or affect engines created afterwards.
    _create_engine_args = copy.deepcopy(
        flavor_configs.get(self.flavor, {}).get('create_engine', {})
    )
    def _apply_create_engine_args(update):
        if self.flavor == 'duckdb' or 'ALL' not in flavor_configs[self.flavor].get('omit_create_engine', {}):
            _create_engine_args.update(
                { k: v for k, v in update.items()
                    if 'omit_create_engine' not in flavor_configs[self.flavor]
                        or k not in flavor_configs[self.flavor].get('omit_create_engine')
                }
            )
    _apply_create_engine_args(self._sys_config.get('create_engine', {}))
    _apply_create_engine_args(self.__dict__.get('create_engine', {}))
    _apply_create_engine_args(_kw_copy)

    if self.flavor == 'duckdb':
        # Historical system config includes pandas.to_sql's method, not an engine option.
        _create_engine_args.pop('method', None)

    duckdb_creator = None
    if self.flavor == 'duckdb':
        if any(_create_engine_args.get(key) is not None for key in ('creator', 'module')):
            raise RuntimeError('DuckDB custom creator/module overrides cannot share the native driver safely.')
        duckdb_creator = []
        # SQLAlchemy constructs engines lazily; bind the shared creator before first checkout.
        _create_engine_args['creator'] = lambda: duckdb_creator[0]()

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

    if engine is not None and duckdb_creator is not None:
        from ._duckdb_transport import engine_creator
        args = dict(_create_engine_args)
        args.pop('creator', None)
        try:
            duckdb_creator.append(engine_creator(self, engine, args))
        except BaseException:
            engine.dispose()
            raise

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
    if pyodbc is None:
        raise EnvironmentError("Cannot import pyodbc. Is the MSSQL driver installed?")

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
