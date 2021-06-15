#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interface with SQL servers using sqlalchemy.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Any

from meerschaum.connectors import Connector
from meerschaum.utils.warnings import error

class SQLConnector(Connector):
    """
    Connect to SQL databases via `sqlalchemy`.

    SQLConnectors may be used as Meerschaum instance connectors.
    Read more about connectors and instances at
    https://meerschaum.io/reference/connectors/
    """

    from ._create_engine import flavor_configs, create_engine
    from ._sql import read, value, exec, execute, to_sql
    from .tools import test_connection
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
        get_pipe_table,
        get_pipe_columns_types,
    )
    from ._plugins import (
        register_plugin,
        delete_plugin,
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
        flavor : Optional[str] = None,
        wait : bool = False,
        connect : bool = False,
        debug : bool = False,
        **kw : Any
    ):
        """
        :param label:
            The identifying label for the connector.
            E.g. for `sql:main`, 'main' is the label.
            Defaults to 'main'.

        :param flavor:
            The database flavor.
            E.g. 'sqlite', 'postgresql', 'cockroachdb', etc.
            To see supported flavors, run the `bootstrap connectors` command.

        :param wait:
            If `True`, block until a database connection has been made.
            Defaults to `False`.

        :param connect:
            If `True`, immediately attempt to connect the database and raise
            a warning if the connection fails.
            Defaults to `False`.

        :param debug:
            Verbosity toggle.
            Defaults to `False`.

        :param kw:
            All other arguments will be passed to the connector's attributes.
            Therefore, a connector may be made without being registered,
            as long enough parameters are supplied to the constructor.
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
        elif 'flavor' not in self.__dict__:
            self.flavor = flavor

        self._debug = debug
        ### Store the PID and thread at initialization
        ### so we can dispose of the Pool in child processes or threads.
        import os, threading
        self._pid = os.getpid()
        self._thread_ident = threading.current_thread().ident

        ### verify the flavor's requirements are met
        if self.flavor not in self.flavor_configs:
            error(f"Flavor '{self.flavor}' is not supported by Meerschaum SQLConnector")
        self.verify_attributes(self.flavor_configs[self.flavor].get('requirements', set()), debug=debug)

        self.wait = wait
        if self.wait:
            from meerschaum.utils.misc import retry_connect
            retry_connect(connector=self, debug=debug)

        if connect:
            if not self.test_connection(debug=debug):
                from meerschaum.utils.warnings import warn
                warn(f"Failed to connect with connector '{self}'!", stack=False)

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
    def db(self) -> Optional[databases.Database]:
        from meerschaum.utils.packages import attempt_import
        databases = attempt_import('databases', lazy=False, install=True)
        url = self.DATABASE_URL
        if 'mysql' in url:
            url = url.replace('+pymysql', '')
        if '_db' not in self.__dict__:
            try:
                self._db = databases.Database(url)
            except KeyError:
                ### Likely encountered an unsupported flavor.
                from meerschaum.utils.warnings import warn
                self._db = None
        return self._db

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__.update(d)

    def __call__(self):
        return self
