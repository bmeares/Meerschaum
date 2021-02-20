#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains the logic that builds the sqlalchemy engine string.
"""

from meerschaum.utils.debug import dprint
from meerschaum.config._paths import SQLITE_DB_PATH

### determine driver and requirements from flavor
default_requirements = {
    'username',
    'password',
    'host',
    'database',
}
flavor_configs = {
    'timescaledb'      : {
        'engine'       : 'postgresql',
        'requirements' : default_requirements,
        'defaults'     : {
            'port'     : 5432,
        },
    },
    'postgresql'         : {
        'engine'       : 'postgresql',
        'requirements' : default_requirements,
        'defaults'     : {
            'port'     : 5432,
        },
    },
    'mssql'            : {
        'engine'       : 'mssql+pyodbc',
        'requirements' : default_requirements,
        'defaults'     : {
            'port'     : 1433,
        },
    },
    'mysql'            : {
        'engine'       : 'mysql+pymysql',
        'requirements' : default_requirements,
        'defaults'     : {
            'port'     : 3306,
        },
    },
    'mariadb'          : {
        'engine'       : 'mysql+pymysql',
        'requirements' : default_requirements,
        'defaults'     : {
            'port'     : 3306,
        },
    },
    'oracle'      : {
        'engine'       : 'oracle+cx_oracle',
        'requirements' : default_requirements,
        'defaults'     : {
            'port' : 1521,
        },
    },
    'sqlite'      : {
        'engine'       : 'sqlite',
        'requirements' : {
        },
        'defaults'     : {
            'database' : SQLITE_DB_PATH,
        },
    },
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
    sqlalchemy = attempt_import('sqlalchemy')
    import urllib
    if self.flavor in ('timescaledb', 'postgresql'):
        ### trigger install if not installed
        psycopg2 = attempt_import('psycopg2', debug=self._debug, lazy=False)

    ### supplement missing values with defaults (e.g. port number)
    for a, value in flavor_configs[self.flavor]['defaults'].items():
        if a not in self.__dict__:
            self.__dict__[a] = value

    ### self.sys_config was deepcopied and can be updated safely
    if self.flavor == "sqlite":
        engine_str = f"sqlite:///{self.database}"
        self.sys_config['connect_args'].update({"check_same_thread" : False})
        aiosqlite = attempt_import('aiosqlite', debug=self._debug, lazy=False)
    else:
        engine_str = (
            flavor_configs[self.flavor]['engine'] + "://" +
            self.username + ":" + urllib.parse.quote_plus(self.password) +
            "@" + self.host + ":" + str(self.port) + "/" + self.database
        )
    if debug: dprint(f"{engine_str}" + '\n' + f"{self.sys_config['connect_args']}")

    engine = sqlalchemy.create_engine(
        engine_str,
        pool_size    = self.sys_config['pool_size'],
        max_overflow = self.sys_config['max_overflow'],
        pool_recycle = self.sys_config['pool_recycle'],
        connect_args = self.sys_config['connect_args'],
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
        **kw
    )

    if include_uri: return engine, engine_str
    return engine

