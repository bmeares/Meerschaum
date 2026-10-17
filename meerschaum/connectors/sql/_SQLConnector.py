#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interface with SQL servers using sqlalchemy.
"""

from __future__ import annotations

import pathlib
import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Any, Union

from meerschaum.connectors import InstanceConnector
from meerschaum.utils.warnings import error, warn

#: Flavors with a Rust driver (`meerschaum-connector-sql`).
_RUST_FLAVORS = {
    'postgresql', 'postgis', 'timescaledb', 'timescaledb-ha', 'citus', 'cockroachdb',
    'mysql', 'mariadb', 'sqlite', 'geopackage', 'mssql', 'oracle', 'duckdb',
}


def _remove_memory_directory(directory: str, owner_pid: int) -> None:
    """
    Remove the database directory of an in-memory SQLite connector, in the process that created
    it only: a forked child (a v4 job daemon) that exits normally must not delete its parent's
    database.
    """
    import os
    import shutil
    if os.getpid() == owner_pid:
        shutil.rmtree(directory, ignore_errors=True)


_ORACLE_CLIENT: Optional[bool] = None


def _oracle_client_available() -> bool:
    """
    Whether the Oracle client library (`libclntsh`) the Rust `oracle` driver loads is present.
    Without it `oracle` instances raise an error naming the requirement (`_core_connector`).
    """
    global _ORACLE_CLIENT
    if _ORACLE_CLIENT is None:
        import ctypes
        import sys
        names = {
            'win32': ('oci.dll',),
            'darwin': ('libclntsh.dylib',),
        }.get(sys.platform, ('libclntsh.so',))
        for name in names:
            try:
                ctypes.CDLL(name)
                _ORACLE_CLIENT = True
                break
            except OSError:
                continue
        else:
            _ORACLE_CLIENT = False
    return _ORACLE_CLIENT


class SQLConnector(InstanceConnector):
    """
    Connect to SQL databases via `sqlalchemy`.
    
    SQLConnectors may be used as Meerschaum instance connectors.
    Read more about connectors and instances at
    https://meerschaum.io/reference/connectors/

    """

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
    from ._compress import (
        get_pipe_size,
        compress_pipe,
        decompress_pipe,
        apply_compression_policy,
        set_integer_now_func,
    )
    from ._maintenance import (
        vacuum_pipe,
        analyze_pipe,
    )
    from ._partition import (
        get_partition_info,
        partition_pipe,
    )
    from ._pipes import (
        fetch_pipes_keys,
        create_indices,
        drop_indices,
        get_create_index_queries,
        get_drop_index_queries,
        delete_pipe,
        get_pipe_data,
        get_pipe_docs,
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
        get_pipe_schema,
        get_pipe_columns_indices,
        get_temporary_target,
        create_pipe_indices,
        drop_pipe_indices,
        get_pipe_index_names,
    )
    from ._plugins import (
        get_plugins_pipe,
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
        get_users_pipe,
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
            if uri.startswith('timescaledb-ha://'):
                uri = uri.replace('timescaledb-ha://', 'postgresql+psycopg://', 1)
                flavor = 'timescaledb-ha'
            if uri.startswith('postgis://'):
                uri = uri.replace('postgis://', 'postgresql+psycopg://', 1)
                flavor = 'postgis'
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

        if self.__dict__.get('flavor', None) in ('sqlite', 'geopackage'):
            self._reset_attributes()
            self._set_attributes(
                'sql',
                label = label,
                inherit_default = False,
                **kw
            )
            ### For backwards compatability reasons, set the path for sql:local if its missing.
            if (
                self.label == 'local'
                and self.__dict__.get('database', None) in (None, '{SQLITE_DB_PATH}')
            ):
                import meerschaum.config.paths as paths
                self.database = paths.SQLITE_DB_PATH.as_posix()

        ### ensure flavor and label are set accordingly
        if 'flavor' not in self.__dict__:
            if flavor is None and 'uri' not in self.__dict__:
                raise ValueError(
                    f"    Missing flavor. Provide flavor as a key for '{self}'."
                )
            self.flavor = flavor or self.parse_uri(self.__dict__['uri']).get('flavor', None)

        if self.flavor == 'postgres':
            self.flavor = 'postgresql'

        self._debug = debug
        ### Store the PID and thread at initialization
        ### so we can dispose of the Pool in child processes or threads.
        import os
        import threading
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
            sqlalchemy_orm = attempt_import('sqlalchemy.orm', lazy=False)
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
            try:
                self._engine, self._engine_str = self.create_engine(include_uri=True)
            except ImportError as e:
                ### A property must raise `AttributeError` for `hasattr(conn, 'engine')` to be
                ### `False`; Python cannot make one exception both an `ImportError` and an
                ### `AttributeError`, so the message names the `sql` extra instead.
                raise AttributeError(str(e)) from e

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
        Return the URI connection string (alias for `SQLConnector.URI`).
        """
        return self.URI

    @property
    def URI(self) -> str:
        """
        Return the URI connection string, built from the connector's attributes
        (no SQLAlchemy needed).
        """
        if '_engine_str' not in self.__dict__:
            from ._create_engine import get_engine_str
            self._engine_str = get_engine_str(self)
        return str(self._engine_str)

    @property
    def IS_THREAD_SAFE(self) -> str:
        """
        Return whether this connector may be multithreaded.
        """
        if self.flavor in ('duckdb', 'oracle'):
            return False
        if self.flavor in ('sqlite', 'geopackage'):
            return (
                ':memory:' not in self.URI
                and self.__dict__.get('_memory_path', None) is None
            )
        return True

    @property
    def metadata(self):
        """
        Return the metadata bound to this configured schema.
        """
        from ._create_engine import import_sqlalchemy
        sqlalchemy = import_sqlalchemy()
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
        from meerschaum._internal.static import STATIC_CONFIG
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

        ### Ask the database (`SELECT current_schema()` on the PostgreSQL family, `SELECT
        ### SCHEMA_NAME()` on MSSQL), natively and once: a role's `search_path` or a login's
        ### default schema is what v4's `sqlalchemy.inspect(engine).default_schema_name`
        ### reports, and the base wheel has no SQLAlchemy. Raises when the database is unreachable.
        _schema = self._core_connector().discover_schema()
        self.__dict__['schema'] = _schema
        self.__dict__['_schema_pending'] = False
        return _schema

    def get_metadata_cache_path(self, kind: str = 'json') -> pathlib.Path:
        """
        Return the path to the file to which to write metadata cache.
        """
        import meerschaum.config.paths as paths
        filename = (
            f'{self.label}-metadata.pkl'
            if kind == 'pkl'
            else f'{self.label}.json'
        )
        return paths.SQL_CONN_CACHE_RESOURCES_PATH / filename

    def __getstate__(self):
        if '_rust_connector' not in self.__dict__:
            return self.__dict__
        return {
            k: v
            for k, v in self.__dict__.items()
            if k not in ('_rust_connector', '_rust_connector_pid')
        }

    def _shared_memory_database(self) -> Optional[str]:
        """
        For an in-memory SQLite connector (`database=':memory:'`), the path of the one database
        file that SQLAlchemy and the Rust pool both open, so `Pipe` methods and `read()` /
        `exec()` / `.engine` see the same tables. `None` for any other connector.

        The Python `sqlite3` module and the extension each carry their own SQLite library, which
        cannot share a `cache=shared` memory database, so the "memory" database is a file in a
        private temporary directory (a RAM disk where the system has one) that is removed when
        the connector is collected or the process exits.
        """
        if self.__dict__.get('flavor', None) != 'sqlite' or self.__dict__.get('uri', None):
            return None
        if self.__dict__.get('database', None) != ':memory:':
            return None
        path = self.__dict__.get('_memory_path', None)
        if path is None:
            import os
            import tempfile
            import weakref
            shm = '/dev/shm'
            base = shm if os.path.isdir(shm) and os.access(shm, os.W_OK) else None
            directory = tempfile.mkdtemp(prefix='mrsm_mem_', dir=base)
            path = os.path.join(directory, 'memory.db')
            self.__dict__['_memory_path'] = path
            weakref.finalize(self, _remove_memory_directory, directory, os.getpid())
        return path

    def _core_connector(self):
        """
        Return the `meerschaum._core.SqlConnector` behind this connector (built once per process,
        see `crates/meerschaum-connector-sql`), which implements every pipe, user and plugin method.

        Every flavor has a Rust driver (`mssql`, `oracle` and `duckdb` are compiled into the
        wheels). An in-memory SQLite database (`sql:memory`) is one database file that the Rust
        pool and SQLAlchemy both open (`_shared_memory_database`), so `Pipe` methods and
        `read()` / `exec()` / `.engine` see the same tables.
        `read`, `exec`, `value`, `to_sql` and `.engine` always stay SQLAlchemy (D16).

        The Rust connector (and its pool) is built once per process: a forked child (a job
        daemon) builds its own, like SQLAlchemy engines must.

        Raises
        ------
        `RuntimeError` naming the cause when no driver can be built (the Oracle client library,
        `libclntsh`, is not installed; an unknown flavor; a failed pool setup).
        """
        import os
        rust = self.__dict__.get('_rust_connector', None)
        if rust is False:
            raise RuntimeError(self._rust_unavailable())
        if rust is not None and self.__dict__.get('_rust_connector_pid', None) != os.getpid():
            rust = None
        if rust is None:
            from meerschaum import _core
            if self.flavor not in _RUST_FLAVORS:
                self.__dict__['_rust_connector'] = False
                self.__dict__['_rust_error'] = f"the flavor '{self.flavor}' has no Rust driver"
                raise RuntimeError(self._rust_unavailable())
            if self.flavor == 'oracle' and not _oracle_client_available():
                self.__dict__['_rust_connector'] = False
                self.__dict__['_rust_error'] = (
                    "the Oracle client library (libclntsh, e.g. Oracle Instant Client) "
                    "is not installed; install it and make it loadable "
                    "(`LD_LIBRARY_PATH` / `ldconfig`) to use `oracle` instances"
                )
                raise RuntimeError(self._rust_unavailable())
            attributes = {
                key: self.__dict__.get(key, None)
                for key in ('flavor', 'uri', 'host', 'port', 'username', 'password', 'database')
            }
            if (shared := self._shared_memory_database()) is not None:
                attributes['database'] = shared
            ### Only a configured schema: the Rust connector discovers the default itself.
            attributes['schema'] = self.__dict__.get('schema', None)
            try:
                rust = _core.SqlConnector(self.label, attributes)
            except Exception as e:
                self.__dict__['_rust_connector'] = False
                self.__dict__['_rust_error'] = str(e)
                raise RuntimeError(self._rust_unavailable()) from e
            self.__dict__['_rust_connector'] = rust
            self.__dict__['_rust_connector_pid'] = os.getpid()
            from meerschaum.utils.sql import NO_SCHEMA_FLAVORS
            self.__dict__['_schema_pending'] = (
                not attributes['schema'] and self.flavor not in NO_SCHEMA_FLAVORS
            )
        if self.__dict__.get('_schema_pending', False):
            ### The default schema is asked of the database before the first query runs, so the
            ### Rust methods see the same schema as `self.schema`. While the database cannot be
            ### asked, nothing runs: the flavor default (`public` / `dbo`) is the wrong schema
            ### for a role with another `search_path`, and a write would create duplicates.
            try:
                self.__dict__['schema'] = rust.discover_schema()
            except Exception as e:
                raise RuntimeError(
                    f"Could not determine the default schema of '{self}': {e}"
                ) from e
            self.__dict__['_schema_pending'] = False
        return rust

    def _rust_unavailable(self) -> str:
        """The error text of a connector that has no usable Rust driver."""
        cause = self.__dict__.get('_rust_error', None)
        return (
            f"Cannot use the SQL connector '{self}': no usable Rust driver"
            + (f" ({cause})." if cause else ".")
        )

    def _rust_method(self, name: str):
        """
        Return the `meerschaum._core.SqlConnector` method `name`. Raises `AttributeError` naming
        the method when the extension does not define it (`read`, `exec`, ... stay SQLAlchemy:
        D16) and `RuntimeError` when the connector has no usable Rust driver
        (see `_core_connector`).
        """
        rust = self._core_connector()
        try:
            return getattr(rust, name)
        except AttributeError:
            raise AttributeError(f"`_core.SqlConnector` has no method '{name}'.") from None

    def __setstate__(self, d):
        self.__dict__.update(d)

    def __call__(self):
        return self
