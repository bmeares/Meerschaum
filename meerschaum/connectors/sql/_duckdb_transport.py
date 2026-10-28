"""Share DuckDB's public Python driver between native SQL operations and SQLAlchemy.

Only connection ownership and driver configuration live here. Pipe algorithms and SQL remain
in Rust. Importing this module never imports DuckDB or SQLAlchemy, or opens a database.
"""

import importlib.util
import os
import sys
from pathlib import Path
import threading
import uuid
import weakref


_PID = os.getpid()
_LOCK = threading.RLock()
_POLICY = None
_OVERRIDE = None
_ANCHORS = weakref.WeakValueDictionary()
_MEMORY_ALIASES = {}
_CONFIGURED_MEMORY_ANCHORS = {}
_DRIVER = None
_LOCAL_SETTINGS = None
_CONNECTIONS = weakref.WeakSet()
_CONNECTION_LOCK = threading.Lock()
_FORK_QUARANTINE = None


def _check_pid():
    if os.getpid() != _PID:
        raise RuntimeError(
            'DuckDB driver state belongs to the parent process. Start a fresh interpreter '
            '(spawn or exec) instead of reusing DuckDB connections after fork.'
        )


def _native_message():
    return (
        'This process selected the native DuckDB C API. SQLAlchemy cannot open the same '
        'database through a separate DuckDB instance without losing data. Unset '
        'MRSM_DUCKDB_LIB, install meerschaum[sql-duckdb], and restart the interpreter to '
        'use both Pipe operations and SQLAlchemy. The standalone binary continues to use '
        'the native library.'
    )


def guard_sdk():
    """Called before optional driver imports or installation in SQLAlchemy engine creation."""
    _check_pid()
    with _LOCK:
        if os.environ.get('MRSM_DUCKDB_LIB') or _POLICY == 'native':
            raise RuntimeError(_native_message())


def policy():
    """Pin one transport per Python process, lazily and without installing optional packages."""
    global _POLICY, _OVERRIDE
    _check_pid()
    with _LOCK:
        override = os.environ.get('MRSM_DUCKDB_LIB') or ''
        explicit = bool(override)
        if _POLICY is not None and override != _OVERRIDE:
            raise RuntimeError('MRSM_DUCKDB_LIB changed after DuckDB transport selection; restart the interpreter.')
        if _POLICY is None:
            _OVERRIDE = override
            if explicit:
                _POLICY = 'native'
            else:
                try:
                    found = importlib.util.find_spec('duckdb') is not None
                except (ImportError, ValueError):
                    found = False
                _POLICY = 'python' if found else 'native'
        return _POLICY



def _reject_reserved(options):
    if any(str(key).lower().startswith('_duckdb_') for key in options):
        raise RuntimeError('Reserved DuckDB transport keys cannot be supplied in URI/options/config.')


def validate_connector_uri(connector):
    _check_pid()
    from urllib.parse import urlsplit, parse_qsl
    uri = connector.__dict__.get('uri')
    if uri:
        _reject_reserved(dict(parse_qsl(urlsplit(uri).query, keep_blank_values=True)))
    options = connector.__dict__.get('options', {})
    if isinstance(options, str):
        options = dict(parse_qsl(options, keep_blank_values=True))
    if isinstance(options, dict):
        _reject_reserved(options)

def _root():
    # No path access that creates directories.
    from meerschaum.config import paths
    return str(paths.ROOT_DIR_PATH)


def memory_id(connector):
    _check_pid()
    with _LOCK:
        token = connector.__dict__.setdefault('_duckdb_memory_id', uuid.uuid4().hex)
        from meerschaum.connectors import connectors
        if connectors.get('sql', {}).get(connector.label) is connector:
            alias_key = (_root(), connector.label)
            # A configured embedded command may open memory before the cached SDK object.
            # Only the first canonical object adopts it; refreshing that object starts anew.
            fallback = _CONFIGURED_MEMORY_ANCHORS.get(('memory', alias_key))
            if alias_key not in _MEMORY_ALIASES and fallback is not None:
                if ('memory', token) not in _ANCHORS:
                    _ANCHORS[('memory', token)] = fallback
            _MEMORY_ALIASES[alias_key] = (token, weakref.ref(connector))
        return token


def _database(database):
    if not database or database == ':memory:':
        return ':memory:'
    if database.startswith((':', 'md:', 'motherduck:')):
        raise RuntimeError('Shared DuckDB transport requires a local file or :memory: database.')
    return str(Path(database).expanduser().resolve())


def _key(database, label, token):
    if not database or database == ':memory:':
        if not token:
            alias = _MEMORY_ALIASES.get((_root(), label))
            if alias is not None and alias[1]() is not None:
                token = alias[0]
        return ('memory', token or (_root(), label))
    return ('file', str(Path(database).expanduser().resolve()))


def _canonical(settings):
    """Preserve case-sensitive values and distinguish values of different types."""
    return {
        str(key).lower(): (type(value).__name__, value)
        for key, value in settings.items()
    }


def _no_install(settings):
    settings = dict(settings)
    if os.environ.get('MRSM_NO_AUTO_INSTALL', '').lower() in ('1', 'true', 'yes'):
        settings['autoinstall_known_extensions'] = False
    return settings


class _Anchor:
    def __init__(self, database, settings, opener, native=False):
        self.database = database or ':memory:'
        self.settings = dict(settings)
        self.signature = _canonical(settings)
        self.opener = opener
        self.primary = None
        self.lock = threading.RLock()
        self.local_settings = None
        self.configured = not native
        self.engines = weakref.WeakSet()

    def configure(self, settings, opener, native=False):
        signature = _canonical(settings)
        with self.lock:
            if native:
                compatible = all(self.signature.get(key) == value for key, value in signature.items())
            elif self.primary is None and not self.configured:
                # SDK settings may refine an unopened implicit native default, but must
                # preserve every explicit native constraint (especially READ_ONLY).
                compatible = all(signature.get(key) == value for key, value in self.signature.items())
            else:
                compatible = signature == self.signature
            if not compatible:
                raise RuntimeError(
                    'Conflicting DuckDB connection settings for an already registered database; '
                    'use consistent settings or restart the interpreter before changing them.'
                )
            if self.primary is None and not native:
                self.settings = dict(settings)
                self.signature = signature
                self.opener = opener
                self.configured = True

    def connect(self, native=False):
        _check_pid()
        local_names = _local_names()
        with self.lock:
            for engine in list(self.engines):
                _guard_engine(engine)
            if self.primary is None:
                self.primary = self.opener()
                rows = self.primary.execute('SELECT name, scope FROM duckdb_settings()').fetchall()
                self.local_settings = local_names | {name.lower() for name, scope in rows if scope.upper() == 'LOCAL'}
            connection = self.primary.duplicate()
            with _CONNECTION_LOCK:
                _CONNECTIONS.add(connection)
            try:
                # A duplicate has its own client context. Apply local settings explicitly,
                # including search_path and TimeZone, rather than assuming they were copied.
                for name, value in self.settings.items():
                    if name.lower() in self.local_settings:
                        escaped_name = name.replace('"', '""')
                        escaped_value = str(value).replace("'", "''")
                        connection.execute(f'SET "{escaped_name}" = \'{escaped_value}\'')
                if native:
                    connection.execute("SET TimeZone = 'UTC'")
                return connection
            except BaseException:
                connection.close()
                raise


def _anchor(database, label, token, settings, opener, native=False):
    _check_pid()
    with _LOCK:
        key = _key(database, label, token)
        anchor = _ANCHORS.get(key)
        if anchor is None:
            anchor = _Anchor(database, settings, opener, native=native)
            _ANCHORS[key] = anchor
            if key[0] == 'memory' and not isinstance(key[1], str):
                # Native embedded commands rebuild registries; configured memory must live
                # for the host session, as the canonical SDK connector registry does.
                _CONFIGURED_MEMORY_ANCHORS[key] = anchor
        else:
            anchor.configure(settings, opener, native=native)
        return anchor


def native_anchor(database, label, token, options):
    """The lazy Rust hook. Return None for the pinned native C API, else a public-driver anchor."""
    _check_pid()
    _reject_reserved(options)
    if policy() == 'native':
        if options:
            raise RuntimeError('Native DuckDB C API transport does not support these connection options; unset MRSM_DUCKDB_LIB and restart to use public driver settings.')
        return None
    duckdb = _driver()
    database = _database(database)
    settings = _no_install(dict(options))
    global_settings = _global_settings(settings)

    def opener():
        return duckdb.connect(database=database or ':memory:', config=global_settings)

    return _anchor(database, label, token, settings, opener, native=True)


def prepare_connector(connector):
    """Register SDK configuration before Rust can open a connection; do not require SQL extras."""
    validate_connector_uri(connector)
    token = memory_id(connector)
    if policy() != 'python':
        return token
    _driver()
    try:
        available = importlib.util.find_spec('sqlalchemy') and importlib.util.find_spec('duckdb_engine')
    except ImportError:
        available = False
    if available:
        import duckdb_engine
        _check_engine_version(duckdb_engine)
        # create_engine is lazy: preparing it registers its real dialect options, without
        # opening an anchor. Its creator retains the same anchor the Rust hook will find.
        engine = connector.engine
        if engine is None:
            raise RuntimeError('Could not prepare the configured DuckDB SQLAlchemy engine; refusing native database access.')
        _guard_engine(engine)
    return token


def engine_creator(connector, engine, create_args):
    """Preserve the dialect and return a creator whose connections share the native anchor."""
    guard_sdk()
    if policy() != 'python':
        raise RuntimeError(_native_message())
    driver = _driver()
    import duckdb_engine
    _check_engine_version(duckdb_engine)
    if duckdb_engine.duckdb is not driver:
        raise RuntimeError('DuckDB driver module changed after transport selection; restart the interpreter.')
    args, kwargs = engine.dialect.create_connect_args(engine.url)
    kwargs = dict(kwargs)
    kwargs.update(create_args.get('connect_args', {}))
    _reject_reserved(kwargs)
    database = _database(kwargs.get('database', ':memory:'))
    kwargs['database'] = database
    if create_args.get('creator') is not None or create_args.get('module') is not None:
        raise RuntimeError('DuckDB custom creator/module overrides cannot share the native driver safely.')
    unsupported = set(kwargs) - {'database', 'read_only', 'config', 'url_config'}
    if unsupported:
        raise RuntimeError('Unsupported shared DuckDB connect_args: ' + ', '.join(sorted(unsupported)))
    if 'database' in create_args.get('connect_args', {}):
        raise RuntimeError('DuckDB connect_args cannot override database; set the connector database or URI.')
    settings = dict(kwargs.get('config', {}))
    settings.update(kwargs.get('url_config', {}))
    _reject_reserved(settings)
    settings = _no_install(settings)
    if kwargs.get('read_only'):
        # Access mode is a DB-level DuckDB setting and part of the shared identity.
        settings['access_mode'] = 'READ_ONLY'
    kwargs['config'] = _global_settings(settings)
    kwargs.pop('url_config', None)
    token = memory_id(connector)

    def opener():
        _guard_engine(engine)
        return engine.dialect.connect(*args, **kwargs)

    anchor = _anchor(database, connector.label, token, settings, opener)
    with anchor.lock:
        anchor.engines.add(engine)
    connector.__dict__['_duckdb_database'] = database

    wrapper = _wrapper_class()

    def creator():
        _guard_engine(engine)
        return wrapper(anchor.connect())

    return creator





def _guard_engine(engine):
    if engine.dialect.dispatch.do_connect:
        raise RuntimeError('DuckDB do_connect routing listeners cannot share the native driver safely; use supported connect_args instead.')

def _wrapper_class():
    """Retain the dialect's public wrapper shape while refusing inherited driver calls."""
    from duckdb_engine import ConnectionWrapper, CursorWrapper

    class Cursor(CursorWrapper):
        def execute(self, *args, **kwargs):
            _check_pid()
            return super().execute(*args, **kwargs)

        def executemany(self, *args, **kwargs):
            _check_pid()
            return super().executemany(*args, **kwargs)

        def fetchmany(self, *args, **kwargs):
            _check_pid()
            return super().fetchmany(*args, **kwargs)

        def __getattr__(self, name):
            _check_pid()
            return super().__getattr__(name)

    class Connection(ConnectionWrapper):
        def __init__(self, connection):
            super().__init__(connection)
            self._mrsm_driver_connection = connection

        def cursor(self):
            _check_pid()
            return Cursor(self._mrsm_driver_connection, self)

        def close(self):
            # SQLAlchemy cleanup may call close while collecting an inherited pool. The
            # child quarantine owns the raw handle, so cleanup must leave it untouched.
            if os.getpid() != _PID:
                self.closed = True
                return
            return super().close()

        def __getattr__(self, name):
            _check_pid()
            return super().__getattr__(name)

    return Connection

def _check_engine_version(module):
    version = tuple(int(part) for part in module.__version__.split('.')[:3])
    if version < (0, 17, 0):
        raise RuntimeError('Shared DuckDB transport requires duckdb-engine >= 0.17.0; upgrade and restart.')

def _driver():
    """Resolve once; an installed but broken/unsupported driver must never fall back to C API."""
    global _DRIVER
    _check_pid()
    with _LOCK:
        if _DRIVER is None:
            import duckdb
            version = tuple(int(part) for part in duckdb.__version__.split('.')[:3])
            if version < (1, 5, 6):
                raise RuntimeError('Shared DuckDB transport requires duckdb >= 1.5.6; upgrade and restart.')
            _DRIVER = duckdb
        return _DRIVER



def _local_names():
    _check_pid()
    global _LOCAL_SETTINGS
    with _LOCK:
        if _LOCAL_SETTINGS is None:
            connection = _driver().connect(':memory:', config=_no_install({}))
            try:
                rows = connection.execute('SELECT name, scope FROM duckdb_settings()').fetchall()
                _LOCAL_SETTINGS = {name.lower() for name, scope in rows if scope.upper() == 'LOCAL'}
                # ICU settings may be registered only after its first autoload. They are
                # session settings and cannot be passed as connect-time global config.
                _LOCAL_SETTINGS.update({'timezone', 'calendar'})
            finally:
                connection.close()
        return _LOCAL_SETTINGS


def _global_settings(settings):
    local = _local_names()
    return {name: value for name, value in settings.items() if name.lower() not in local}

def execute(connection, sql, params):
    _check_pid()
    result = connection.execute(sql, params) if params else connection.execute(sql)
    # DuckDB reports rowcount=-1; DML returns a single Count row instead.
    description = result.description
    if description and len(description) == 1 and description[0][0] == 'Count':
        row = result.fetchone()
        return int(row[0]) if row else 0
    return 0


def query(connection, sql, params):
    """Read without Arrow/pandas; cast complex values inside DuckDB before Python decoding."""
    _check_pid()
    # DuckDB 2 executes lazily through its DBAPI cursor and refuses a relation while
    # a preceding cursor is unfinished (even a one-row query read with fetchone()).
    # Earlier versions completed execution before returning from execute().
    complete = getattr(connection, 'complete', None)
    if complete is not None:
        complete()
    names = None
    if params:
        # Bound relations materialize immediately and deduplicate output names. DESCRIBE
        # retains the original names without evaluating the data plan. Never prepend it
        # to multiple statements: a trailing write could otherwise run during metadata.
        statements = connection.extract_statements(sql)
        if len(statements) != 1 or statements[0].type != _driver().StatementType.SELECT:
            raise RuntimeError(
                'Parameterized native DuckDB reads require exactly one SELECT statement; '
                'use SQLConnector.exec() for writes or multiple statements.'
            )
        names = [row[0] for row in connection.execute('DESCRIBE ' + sql, params).fetchall()]
    relation = connection.sql(sql, params=params) if params else connection.sql(sql)
    if relation is None:
        raise RuntimeError('The DuckDB query did not produce a result set.')
    if names is not None and len(names) != len(relation.types):
        raise RuntimeError('DuckDB query metadata does not match its result width.')
    columns = list(zip(names if names is not None else relation.columns, map(str, relation.types)))
    scalar_types = {
        'BOOLEAN', 'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'UTINYINT', 'USMALLINT',
        'UINTEGER', 'UBIGINT', 'HUGEINT', 'UHUGEINT', 'FLOAT', 'DOUBLE', 'VARCHAR',
        'BLOB', 'GEOMETRY', 'UUID', 'DATE', 'TIME', 'TIME_NS', 'JSON',
    }
    expressions = []
    needs_cast = False
    # Position-based renaming handles duplicate output names without ambiguous references.
    for _, dtype in columns:
        upper = dtype.upper()
        supported = (upper in scalar_types or upper.startswith(('DECIMAL(', 'TIMESTAMP')))
        # Public DBAPI TIMESTAMPTZ decoding otherwise imports optional pytz. DuckDB's own
        # text retains offsets and infinities; the Rust bridge parses them without extras.
        supported = supported and upper != 'TIMESTAMP WITH TIME ZONE'
        position = len(expressions) + 1
        expressions.append(f'#{position}' if supported else f'CAST(#{position} AS VARCHAR)')
        needs_cast |= not supported
    if needs_cast:
        # Positional references preserve duplicate names and never execute the input twice.
        relation = relation.project(', '.join(expressions))
    if callable(getattr(relation, 'fetchmany', None)):
        # DuckDB 1.x supports stateful relation results. Its sql_query() cannot
        # re-expose a materialized parameter source as executable SQL.
        return relation, columns
    # Fetch through the DBAPI cursor. DuckDB 2 removed the stateful `fetchmany()` on
    # relations; their `fetchall()` re-executes the plan each time. `sql_query()`
    # retains bound relation sources (including their materialized parameter values),
    # so the projected query executes once and the connection consumes that result.
    projected_sql = relation.sql_query()
    # A bound relation may still expose its original placeholders, or may refer to
    # a materialized source with none (e.g. a query with a volatile expression).
    remaining_params = connection.extract_statements(projected_sql)[0].named_parameters
    cursor = (
        connection.execute(projected_sql, params)
        if params and remaining_params
        else connection.execute(projected_sql)
    )
    return cursor, columns


def _after_fork_child():
    # No inherited DuckDB close/destructor may checkpoint a parent's snapshot. Leak one
    # reference to the child-only quarantine until exec/_exit; all child operations refuse.
    global _FORK_QUARANTINE
    _FORK_QUARANTINE = list(_ANCHORS.values()) + list(_CONNECTIONS)
    native = sys.modules.get('meerschaum._core._native')
    if native is not None and hasattr(native, '_duckdb_fork_quarantine'):
        native._duckdb_fork_quarantine(_FORK_QUARANTINE)


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_after_fork_child)
