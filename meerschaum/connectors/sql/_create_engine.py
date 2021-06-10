#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains the logic that builds the sqlalchemy engine string.
"""

from meerschaum.utils.debug import dprint
from meerschaum.config._paths import SQLITE_DB_PATH, DUCKDB_PATH

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
    'timescaledb'      : {
        'engine'       : 'postgresql',
        'create_engine' : default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql' : {},
        'requirements' : default_requirements,
        'defaults'     : {
            'port'     : 5432,
        },
    },
    'postgresql'         : {
        'engine'       : 'postgresql',
        'create_engine' : default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql' : {},
        'requirements' : default_requirements,
        'defaults'     : {
            'port'     : 5432,
        },
    },
    'mssql'            : {
        'engine'       : 'mssql+pyodbc',
        'create_engine' : {
            'fast_executemany': True,
        },
        'omit_create_engine': {'method',},
        'to_sql': {
            'method': None,
        },
        'requirements' : default_requirements,
        'defaults'     : {
            'port'     : 1433,
            'driver'   : 'ODBC+Driver+17+for+SQL+Server',
        },
    },
    'mysql'            : {
        'engine'       : 'mysql+pymysql',
        'create_engine' : default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql': {
            'method': 'multi',
        },
        'requirements' : default_requirements,
        'defaults'     : {
            'port'     : 3306,
        },
    },
    'mariadb'          : {
        'engine'       : 'mysql+pymysql',
        'create_engine' : default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql': {
            'method': 'multi',
        },
        'requirements' : default_requirements,
        'defaults'     : {
            'port'     : 3306,
        },
    },
    'oracle'           : {
        'engine'       : 'oracle+cx_oracle',
        'create_engine' : default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql': {
            'method': 'multi',
        },
        'requirements' : default_requirements,
        'defaults'     : {
            'port'     : 1521,
        },
    },
    'sqlite'           : {
        'engine'       : 'sqlite',
        'create_engine' : default_create_engine_args,
        'omit_create_engine': {'method',},
        'to_sql': {
            'method': 'multi',
        },
        'requirements' : {
        },
        'defaults'     : {
            'database' : SQLITE_DB_PATH,
        },
    },
    'duckdb' : {
        'engine' : 'duckdb',
        'create_engine' : {},
        'omit_create_engine': {'ALL',},
        'to_sql': {
            'method': 'multi',
        },
        'requirements' : '',
        'defaults' : {
            'database' : DUCKDB_PATH,
        },
    },
    'cockroachdb'      : {
        'engine'       : 'cockroachdb',
        'omit_create_engine': {'method',},
        'create_engine': default_create_engine_args,
        'to_sql': {
            'method': 'multi',
        },
        'requirements' : {'host'},
        'defaults'     : {
            'port'     : 26257,
            'database' : 'defaultdb',
            'username' : 'root',
            'password' : 'admin',
        },
    },
}
install_flavor_drivers = {
    'sqlite' : ['aiosqlite'],
    'duckdb' : ['duckdb', 'duckdb_engine'],
    'mysql' : ['pymysql'],
    'mariadb' : ['pymysql'],
    'timescaledb' : ['psycopg2'],
    'postgresql' : ['psycopg2'],
    'cockroachdb' : ['psycopg2', 'cockroachdb'],
    'mssql': ['pyodbc'],
}


def create_engine(
        self,
        include_uri : bool = False,
        debug : bool = False,
        **kw
    ) -> 'sqlalchemy.engine.Engine':
    """
    Create a sqlalchemy engine by building the engine string.

    returns: sqlalchemy engine
    """
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.warnings import error, warn
    sqlalchemy = attempt_import('sqlalchemy')
    import urllib
    if self.flavor in install_flavor_drivers:
        attempt_import(
            *install_flavor_drivers[self.flavor],
            debug=self._debug,
            lazy=False,
            warn=False
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
    _driver = self.__dict__.get('driver', None)

    ### self.sys_config was deepcopied and can be updated safely
    if self.flavor in ("sqlite", "duckdb"):
        ### The duckdb dialect might not be registered.
        if self.flavor == 'duckdb':
            sqlalchemy.engine.url.registry.register("duckdb", "duckdb_engine", "Dialect")

        engine_str = f"{_engine}:///{_database}"
        if 'create_engine' not in self.sys_config:
            self.sys_config['create_engine'] = {}
        if 'connect_args' not in self.sys_config['create_engine']:
            self.sys_config['create_engine']['connect_args'] = {}
        self.sys_config['create_engine']['connect_args'].update({"check_same_thread" : False})
    else:
        engine_str = (
            _engine + "://" + (_username if _username is not None else '') +
            ((":" + urllib.parse.quote_plus(_password)) if _password is not None else '') +
            "@" + _host + ((":" + str(_port)) if _port is not None else '') +
            (("/" + _database) if _database is not None else '')
            + (("?driver=" + _driver) if _driver is not None else '')
        )
    if debug:
        dprint(
            (
                (engine_str.replace(':' + _password, ':' + ('*' * len(_password))))
                    if _password is not None else engine_str
            ) + '\n' + f"{self.sys_config.get('create_engine', {}).get('connect_args', {})}"
        )

    _kw_copy = kw.copy()
    #  _create_engine_args = {
        #  k: v for k, v in self.sys_config.get('create_engine', {}).items()
            #  if k in flavor_configs[self.flavor].get('create_engine', {})
    #  }

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
    _apply_create_engine_args(self.sys_config.get('create_engine', {}))
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
                    ".".join(self.sys_config['poolclass'].split('.')[:-1])
                ),
                self.sys_config['poolclass'].split('.')[-1]
            ),
            echo         = debug,
            **_create_engine_args
        )
    except Exception as e:
        warn(e)
        warn(f"Failed to create connector '{self}'.", stack=False)
        engine = None

    if include_uri:
        return engine, engine_str
    return engine
