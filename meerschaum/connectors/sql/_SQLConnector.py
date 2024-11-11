#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interface with SQL servers using sqlalchemy.
"""

from __future__ import annotations
import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Any, Union

from meerschaum.connectors import Connector
from meerschaum.utils.warnings import error, warn


class SQLConnector(Connector):
    """
    Connect to SQL databases via `sqlalchemy`.
    
    SQLConnectors may be used as Meerschaum instance connectors.
    Read more about connectors and instances at
    https://meerschaum.io/reference/connectors/

    """

    IS_INSTANCE: bool = True

    from ._create_engine import flavor_configs, create_engine
    from ._sql import (
        read,
        value,
        exec,
        execute,
        to_sql,
        exec_queries,
        get_connection,
        _cleanup_connections,
    )
    from meerschaum.utils.sql import test_connection
    from ._fetch import fetch, get_pipe_metadef
    from ._cli import cli, _cli_exit
    from ._pipes import (
        fetch_pipes_keys,
        create_indices,
        drop_indices,
        get_create_index_queries,
        get_drop_index_queries,
        get_add_columns_queries,
        get_alter_columns_queries,
        delete_pipe,
        get_pipe_data,
        get_pipe_data_query,
        register_pipe,
        edit_pipe,
        get_pipe_id,
        get_pipe_attributes,
        sync_pipe,
        sync_pipe_inplace,
        get_sync_time,
        pipe_exists,
        get_pipe_rowcount,
        drop_pipe,
        clear_pipe,
        deduplicate_pipe,
        get_pipe_table,
        get_pipe_columns_types,
        get_to_sql_dtype,
        get_pipe_schema,
        create_pipe_table_from_df,
        get_pipe_columns_indices,
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
    from ._uri import from_uri, parse_uri
    from ._instance import (
        _log_temporary_tables_creation,
        _drop_temporary_table,
        _drop_temporary_tables,
        _drop_old_temporary_tables,
    )

    def __init__(
        self,
        label: Optional[str] = None,
        flavor: Optional[str] = None,
        wait: bool = False,
        connect: bool = False,
        debug: bool = False,
        **kw: Any
    ):
        """
        Parameters
        ----------
        label: str, default 'main'
            The identifying label for the connector.
            E.g. for `sql:main`, 'main' is the label.
            Defaults to 'main'.

        flavor: Optional[str], default None
            The database flavor, e.g.
            `'sqlite'`, `'postgresql'`, `'cockroachdb'`, etc.
            To see supported flavors, run the `bootstrap connectors` command.

        wait: bool, default False
            If `True`, block until a database connection has been made.
            Defaults to `False`.

        connect: bool, default False
            If `True`, immediately attempt to connect the database and raise
            a warning if the connection fails.
            Defaults to `False`.

        debug: bool, default False
            Verbosity toggle.
            Defaults to `False`.

        kw: Any
            All other arguments will be passed to the connector's attributes.
            Therefore, a connector may be made without being registered,
            as long enough parameters are supplied to the constructor.
        """
        if 'uri' in kw:
            uri = kw['uri']
            if uri.startswith('postgres') and not uri.startswith('postgresql'):
                uri = uri.replace('postgres', 'postgresql', 1)
            if uri.startswith('postgresql') and not uri.startswith('postgresql+'):
                uri = uri.replace('postgresql://', 'postgresql+psycopg://', 1)
            if uri.startswith('timescaledb://'):
                uri = uri.replace('timescaledb://', 'postgresql+psycopg://', 1)
                flavor = 'timescaledb'
            kw['uri'] = uri
            from_uri_params = self.from_uri(kw['uri'], as_dict=True)
            label = label or from_uri_params.get('label', None)
            _ = from_uri_params.pop('label', None)

            ### Sometimes the flavor may be provided with a URI.
            kw.update(from_uri_params)
            if flavor:
                kw['flavor'] = flavor

        ### set __dict__ in base class
        super().__init__(
            'sql',
            label = label or self.__dict__.get('label', None),
            **kw
        )

        if self.__dict__.get('flavor', None) == 'sqlite':
            self._reset_attributes()
            self._set_attributes(
                'sql',
                label = label,
                inherit_default = False,
                **kw
            )
            ### For backwards compatability reasons, set the path for sql:local if its missing.
            if self.label == 'local' and not self.__dict__.get('database', None):
                from meerschaum.config._paths import SQLITE_DB_PATH
                self.database = str(SQLITE_DB_PATH)

        ### ensure flavor and label are set accordingly
        if 'flavor' not in self.__dict__:
            if flavor is None and 'uri' not in self.__dict__:
                raise Exception(
                    f"    Missing flavor. Provide flavor as a key for '{self}'."
                )
            self.flavor = flavor or self.parse_uri(self.__dict__['uri']).get('flavor', None)

        if self.flavor == 'postgres':
            self.flavor = 'postgresql'

        self._debug = debug
        ### Store the PID and thread at initialization
        ### so we can dispose of the Pool in child processes or threads.
        import os, threading
        self._pid = os.getpid()
        self._thread_ident = threading.current_thread().ident
        self._sessions = {}
        self._locks = {'_sessions': threading.RLock(), }

        ### verify the flavor's requirements are met
        if self.flavor not in self.flavor_configs:
            error(f"Flavor '{self.flavor}' is not supported by Meerschaum SQLConnector")
        if not self.__dict__.get('uri'):
            self.verify_attributes(
                self.flavor_configs[self.flavor].get('requirements', set()),
                debug=debug,
            )

        if wait:
            from meerschaum.connectors.poll import retry_connect
            retry_connect(connector=self, debug=debug)

        if connect:
            if not self.test_connection(debug=debug):
                warn(f"Failed to connect with connector '{self}'!", stack=False)

    @property
    def Session(self):
        if '_Session' not in self.__dict__:
            if self.engine is None:
                return None

            from meerschaum.utils.packages import attempt_import
            sqlalchemy_orm = attempt_import('sqlalchemy.orm')
            session_factory = sqlalchemy_orm.sessionmaker(self.engine)
            self._Session = sqlalchemy_orm.scoped_session(session_factory)

        return self._Session

    @property
    def engine(self):
        """
        Return the SQLAlchemy engine connected to the configured database.
        """
        import os
        import threading
        if '_engine' not in self.__dict__:
            self._engine, self._engine_str = self.create_engine(include_uri=True)

        same_process = os.getpid() == self._pid
        same_thread = threading.current_thread().ident == self._thread_ident

        ### handle child processes
        if not same_process:
            self._pid = os.getpid()
            self._thread = threading.current_thread()
            warn("Different PID detected. Disposing of connections...")
            self._engine.dispose()

        ### handle different threads
        if not same_thread:
            if self.flavor == 'duckdb':
                warn("Different thread detected.")
                self._engine.dispose()

        return self._engine

    @property
    def DATABASE_URL(self) -> str:
        """
        Return the URI connection string (alias for `SQLConnector.URI`.
        """
        _ = self.engine
        return str(self._engine_str)

    @property
    def URI(self) -> str:
        """
        Return the URI connection string.
        """
        _ = self.engine
        return str(self._engine_str)

    @property
    def IS_THREAD_SAFE(self) -> str:
        """
        Return whether this connector may be multithreaded.
        """
        if self.flavor in ('duckdb', 'oracle'):
            return False
        if self.flavor == 'sqlite':
            return ':memory:' not in self.URI
        return True


    @property
    def metadata(self):
        """
        Return the metadata bound to this configured schema.
        """
        from meerschaum.utils.packages import attempt_import
        sqlalchemy = attempt_import('sqlalchemy')
        if '_metadata' not in self.__dict__:
            self._metadata = sqlalchemy.MetaData(schema=self.schema)
        return self._metadata


    @property
    def instance_schema(self):
        """
        Return the schema name for Meerschaum tables. 
        """
        return self.schema


    @property
    def internal_schema(self):
        """
        Return the schema name for internal tables. 
        """
        from meerschaum.config.static import STATIC_CONFIG
        from meerschaum.utils.packages import attempt_import
        from meerschaum.utils.sql import NO_SCHEMA_FLAVORS
        schema_name = self.__dict__.get('internal_schema', None) or (
            STATIC_CONFIG['sql']['internal_schema']
            if self.flavor not in NO_SCHEMA_FLAVORS
            else self.schema
        )

        if '_internal_schema' not in self.__dict__:
            self._internal_schema = schema_name
        return self._internal_schema


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


    @property
    def db_version(self) -> Union[str, None]:
        """
        Return the database version.
        """
        _db_version = self.__dict__.get('_db_version', None)
        if _db_version is not None:
            return _db_version

        from meerschaum.utils.sql import get_db_version
        self._db_version = get_db_version(self)
        return self._db_version


    @property
    def schema(self) -> Union[str, None]:
        """
        Return the default schema to use.
        A value of `None` will not prepend a schema.
        """
        if 'schema' in self.__dict__:
            return self.__dict__['schema']

        from meerschaum.utils.sql import NO_SCHEMA_FLAVORS
        if self.flavor in NO_SCHEMA_FLAVORS:
            self.__dict__['schema'] = None
            return None

        sqlalchemy = mrsm.attempt_import('sqlalchemy')
        _schema = sqlalchemy.inspect(self.engine).default_schema_name
        self.__dict__['schema'] = _schema
        return _schema


    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__.update(d)

    def __call__(self):
        return self
