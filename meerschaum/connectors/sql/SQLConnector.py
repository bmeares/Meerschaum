#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interface with SQL servers using sqlalchemy.
"""

from meerschaum.connectors import Connector
from meerschaum.utils.warnings import error

class SQLConnector(Connector):
    """
    Create and utilize sqlalchemy engines
    """
    from ._create_engine import flavor_configs, create_engine
    from ._sql import read, value, exec, execute, to_sql
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
        sync_pipe,
        get_sync_time,
        pipe_exists,
        get_pipe_rowcount,
        drop_pipe,
    )
    from ._plugins import (
        register_plugin,
        get_plugin_id,
        get_plugin_version,
        get_plugins,
        get_plugin_user_id,
        get_plugin_username,
        get_plugin_attributes,
    )
    from ._users import (
        register_user,
        get_user_id,
        get_users,
        edit_user,
        delete_user,
        get_user_password_hash,
        get_user_type,
        get_user_attributes,
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
        ### set __dict__ in base class
        super().__init__('sql', label=label, **kw)
        if 'flavor' in self.__dict__ and self.flavor == 'sqlite':
            self._reset_attributes()
            self._set_attributes(
                'sql',
                label = label,
                inherit_default = False,
                **kw
            )

        ### ensure flavor and label are set accordingly
        if 'flavor' not in self.__dict__ and flavor is None:
            raise Exception("Missing flavor. Update config.yaml or provide flavor as an argument")
        elif 'flavor' not in self.__dict__: self.flavor = flavor

        ### verify the flavor's requirements are met
        if self.flavor not in self.flavor_configs:
            error(f"Flavor '{self.flavor}' is not supported by Meerschaum SQLConnector")
        self.verify_attributes(self.flavor_configs[self.flavor]['requirements'], debug=debug)

        self.wait = wait
        if self.wait:
            from meerschaum.utils.misc import wait_for_connection
            wait_for_connection(connector=self.db, debug=debug)

        ### store the PID and thread at initialization so we can dispose of the Pool in child processes or threads
        import os; self._pid = os.getpid()
        import threading; self._thread_ident = threading.current_thread().ident
        self._debug = debug

        ### create a sqlalchemy session for building ORM queries
        #  self.Session = sqlalchemy_orm.sessionmaker()
        #  self.Session.configure(bind=self.engine)
        #  self.session = self.Session()

    @property
    def engine(self):
        import os, threading
        ### build the sqlalchemy engine
        if '_engine' not in self.__dict__:
            self._engine = self.create_engine(debug=self._debug)

        same_process = os.getpid() == self._pid
        same_thread = threading.current_thread().ident == self._thread_ident

        ### handle child processes
        if not same_process:
            self._pid = os.getpid()
            self._thread = threading.current_thread()
            from meerschaum.utils.warnings import warn
            warn(f"Different PID detected. Disposing of connections...")
            self._engine.dispose()

        ### handle different threads
        if not same_thread:
            pass

        return self._engine

    @property
    def DATABASE_URL(self):
        return str(self.engine.url)

    @property
    def metadata(self):
        from meerschaum.utils.packages import attempt_import
        sqlalchemy = attempt_import('sqlalchemy')
        if '_metadata' not in self.__dict__:
            self._metadata = sqlalchemy.MetaData(self.engine)
        return self._metadata

    @property
    def db(self):
        from meerschaum.utils.packages import attempt_import
        databases = attempt_import('databases', lazy=False, install=True)
        if '_db' not in self.__dict__:
            self._db = databases.Database(self.DATABASE_URL)
        return self._db

    def __getstate__(self): return self.__dict__
    def __setstate__(self, d): self.__dict__.update(d)
    def __call__(self): return self

    #  def __del__(self):
        #  pass
        #  self.session.close()
