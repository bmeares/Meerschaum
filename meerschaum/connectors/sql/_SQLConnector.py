#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interface with SQL servers using sqlalchemy
"""

from meerschaum.connectors._Connector import Connector
from meerschaum.utils.warnings import error

class SQLConnector(Connector):
    """
    Create and utilize sqlalchemy engines
    """
    from ._create_engine import flavor_configs, create_engine
    from ._sql import read, value, exec, to_sql
    from ._fetch import fetch
    from ._cli import cli
    from ._pipes import (
        fetch_pipes_keys,
        create_indices,
        delete_pipe,
        get_backtrack_data,
        get_pipe_data,
        register_pipe,
        edit_pipe,
        get_pipe_id,
        get_pipe_attributes,
        get_sync_time,
        pipe_exists,
    )
    
    def __init__(
            self,
            label : str = 'main',
            flavor : str = None,
            wait : bool = False,
            debug : bool = False,
            **kw
        ):
        """
        Build the SQLConnector engine and connect to the database
        """
        from meerschaum.utils.misc import attempt_import
        databases, sqlalchemy, sqlalchemy_orm, asyncio = attempt_import(
            'databases',
            'sqlalchemy',
            'sqlalchemy.orm',
            'asyncio'
        )
        ### set __dict__ in base class
        super().__init__('sql', label=label, **kw)

        ### ensure flavor and label are set accordingly
        if 'flavor' not in self.__dict__ and flavor is None:
            raise Exception("Missing flavor. Update config.yaml or provide flavor as an argument")
        elif 'flavor' not in self.__dict__: self.flavor = flavor

        ### verify the flavor's requirements are met
        if self.flavor not in self.flavor_configs:
            error(f"Flavor '{self.flavor}' is not supported by Meerschaum SQLConnector")
        self.verify_attributes(self.flavor_configs[self.flavor]['requirements'], debug=debug)

        ### build the sqlalchemy engine and set DATABASE_URL
        self.engine, self.DATABASE_URL = self.create_engine(include_uri=True, debug=debug)

        self.wait = wait
        if self.wait:
            from meerschaum.utils.misc import wait_for_connection
            wait_for_connection(connector=self.db, debug=debug)

        ### create a sqlalchemy session for building ORM queries
        #  self.Session = sqlalchemy_orm.sessionmaker()
        #  self.Session.configure(bind=self.engine)
        #  self.session = self.Session()

    @property
    def metadata(self):
        from meerschaum.utils.misc import attempt_import
        sqlalchemy = attempt_import('sqlalchemy')
        if '_metadata' not in self.__dict__:
            self._metadata = sqlalchemy.MetaData(self.engine)
        return self._metadata

    @property
    def db(self):
        from meerschaum.utils.misc import attempt_import
        databases = attempt_import('databases')
        if '_db' not in self.__dict__:
            self._db = databases.Database(self.DATABASE_URL)
        return self._db

    def __del__(self):
        pass
        #  self.session.close()
