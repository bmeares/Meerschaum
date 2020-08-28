#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains the logic that builds the sqlalchemy engine string.
"""

### determine driver and requirements from flavor
default_requirements = {
    'username',
    'password',
    'host',
    'database',
}
flavor_configs = {
        'timescaledb' : {
            'engine'       : 'postgres',
            'requirements' : default_requirements,
            'defaults'     : {
                'port' : 5432,
            },
        },
        'postgres'    : {
            'engine'       : 'postgres',
            'requirements' : default_requirements,
            'defaults'     : {
                'port' : 5432,
            },
        },
        'mssql'       : {
            'engine'       : 'mssql+pyodbc',
            'requirements' : default_requirements,
            'defaults'     : {
                'port' : 1433,
            },
        },
        'mysql'       : {
            'engine'       : 'mysql+pymysql',
            'requirements' : default_requirements,
            'defaults'     : {
                'port' : 3306,
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
                'database' : 'meerschaum_local',
            },
        },
}

def create_engine(self, debug=False, **kw) -> 'sqlalchemy.engine.Engine':
    """
    Create a sqlalchemy engine by building the engine string.

    returns: sqlalchemy engine
    """
    import sqlalchemy, urllib
    ### supplement missing values with defaults (e.g. port number)
    for a, value in flavor_configs[self.flavor]['defaults'].items():
        if a not in self.__dict__:
            self.__dict__[a] = value

    if self.flavor == "sqlite":
        engine_str = f"sqlite:///{self.database}.sqlite"
    else:
        engine_str = (
            flavor_configs[self.flavor]['engine'] + "://" +
            self.username + ":" + urllib.parse.quote_plus(self.password) +
            "@" + self.host + ":" + str(self.port) + "/" + self.database
        )
    if debug: print(engine_str)
    return sqlalchemy.create_engine(
        engine_str,
        pool_size=5,
        max_overflow=10,
        pool_recycle=3600,
        poolclass=sqlalchemy.pool.QueuePool,
        **kw
    )

