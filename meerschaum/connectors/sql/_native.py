#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Raw SQL through the Rust connector (rc15, Q50 / Q53).

`SQLConnector.read()`, `exec()` / `execute()`, `exec_queries()`, `value()` and `to_sql()` run on
the connector's Rust connection pool (`meerschaum._core.SqlConnector`, the one `Pipe` methods
use), for every flavor in `NATIVE_SQL_FLAVORS`, instead of SQLAlchemy:

- one database library per process: on SQLite, Python's `sqlite3` (SQLAlchemy's driver) and the
  extension's bundled SQLite kept separate lock bookkeeping for the same file, so closing it in
  one dropped the other's locks (sqlite.org/howtocorrupt.html, 2.2.1);
- no SQLAlchemy (the `sql` extra) is needed for raw SQL, and its per-row overhead is gone.

The results keep Meerschaum 4.1's contract: `read()` returns the frame `pd.read_sql_query()`
built from the DBAPI's values (or a Polars frame, see `meerschaum.utils.dataframe`), `exec()` a
result with `fetchall()`, `first()`, `scalar()`, `mappings()`, `rowcount`, ..., whose rows hold
the values the flavor's DBAPI returns, and database errors raise PEP 249 exceptions (also
instances of SQLAlchemy's classes of the same name when SQLAlchemy is imported).

SQLAlchemy stays for `.engine` and for calls that hand the caller a SQLAlchemy object or take
one (`exec(..., with_connection=True)`, `_connection=`, a custom `to_sql(method=...)`, Dask
frames): those fall back explicitly (a debug message names the reason).
"""

from __future__ import annotations

import re
import sys
import threading
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, Union

#: The flavors whose raw SQL (`read`, `exec`, `exec_queries`, `value`, `to_sql`) runs through the
#: Rust connector. Removing a flavor sends its raw SQL back to SQLAlchemy.
NATIVE_SQL_FLAVORS = frozenset({
    'postgresql', 'postgis', 'timescaledb', 'timescaledb-ha', 'citus', 'cockroachdb',
    'mysql', 'mariadb',
    'mssql',
    'oracle',
    'sqlite', 'geopackage',
    'duckdb',
})

_PG_FAMILY = {'postgresql', 'postgis', 'timescaledb', 'timescaledb-ha', 'citus', 'cockroachdb'}
_QMARK_FLAVORS = {'sqlite', 'geopackage', 'mysql', 'mariadb'}


def uses_native(connector: Any) -> bool:
    """
    Whether this connector's raw SQL runs through the Rust connector: its flavor is in
    `NATIVE_SQL_FLAVORS` and the connector does not set `native_sql: false`.
    """
    if connector.__dict__.get('flavor', None) not in NATIVE_SQL_FLAVORS:
        return False
    value = connector.__dict__.get('native_sql', True)
    if isinstance(value, str):
        return value.strip().lower() not in ('false', '0', 'no', 'off')
    return bool(value)


def fallback(connector: Any, method: str, reason: str, debug: bool = False) -> None:
    """Note (with `debug`) that a raw SQL call uses SQLAlchemy, and why."""
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"[{connector}] `{method}()` uses SQLAlchemy: {reason}.")


# ------------------------------------------------------------------------------------------------
# Errors

class DatabaseError(Exception):
    """
    A database error from raw SQL through the Rust connector (PEP 249's `DatabaseError`).

    Attributes: `statement` (the SQL that failed), `params` (its parameters), `sqlstate` (the
    SQLSTATE or the driver's error code, when the database reports one) and `orig` (`None`: there
    is no DBAPI exception underneath).
    """

    def __init__(
        self,
        message: str = '',
        statement: Optional[str] = None,
        params: Any = None,
        sqlstate: Optional[str] = None,
    ):
        Exception.__init__(self, message)
        self.message = message
        self.statement = statement
        self.params = params
        self.sqlstate = sqlstate
        self.code = sqlstate
        self.orig = None
        self.detail = []
        self.hide_parameters = False
        self.connection_invalidated = False
        self.ismulti = False

    def __str__(self) -> str:
        if self.statement:
            return f"{self.message}\n[SQL: {self.statement}]"
        return self.message

    def __reduce__(self):
        return (type(self), (self.message, self.statement, self.params, self.sqlstate))


class InterfaceError(DatabaseError):
    """PEP 249 `InterfaceError`."""


class OperationalError(DatabaseError):
    """PEP 249 `OperationalError`: a lost connection, a locked database, a missing SQLite table."""


class ProgrammingError(DatabaseError):
    """PEP 249 `ProgrammingError`: a syntax error, a missing table or column."""


class IntegrityError(DatabaseError):
    """PEP 249 `IntegrityError`: a violated unique, foreign key, not-null or check constraint."""


class DataError(DatabaseError):
    """PEP 249 `DataError`: a value out of range or of the wrong type."""


class InternalError(DatabaseError):
    """PEP 249 `InternalError`."""


class NotSupportedError(DatabaseError):
    """PEP 249 `NotSupportedError`."""


class ResourceClosedError(Exception):
    """A result of a statement that returns no rows was asked for rows (SQLAlchemy's error)."""

    def __init__(self, message: str = (
        "This result object does not return rows. It has been closed automatically."
    )):
        super().__init__(message)


_ERRORS = {
    cls.__name__: cls
    for cls in (
        DatabaseError, InterfaceError, OperationalError, ProgrammingError, IntegrityError,
        DataError, InternalError, NotSupportedError,
    )
}
_MIXED_CLASSES: Dict[Tuple[Any, ...], type] = {}
_MIXED_LOCK = threading.Lock()


def _error_class(name: str, for_read: bool = False) -> type:
    """
    The exception class for a PEP 249 error name: the class above, combined with SQLAlchemy's
    class of the same name when SQLAlchemy is already imported (so `except
    sqlalchemy.exc.OperationalError` keeps catching it), and for `read()` with
    `pandas.errors.DatabaseError` when pandas is imported (`pd.read_sql_query()` raised that).
    """
    base = _ERRORS.get(name, DatabaseError)
    bases = [base]
    sa_exc = sys.modules.get('sqlalchemy.exc', None)
    sa_cls = getattr(sa_exc, base.__name__, None) if sa_exc is not None else None
    if sa_cls is not None:
        bases.append(sa_cls)
    pd_errors = sys.modules.get('pandas.errors', None) if for_read else None
    pd_cls = getattr(pd_errors, 'DatabaseError', None) if pd_errors is not None else None
    if pd_cls is not None:
        bases.append(pd_cls)
    if len(bases) == 1:
        return base
    key = (base.__name__,) + tuple(id(b) for b in bases)
    with _MIXED_LOCK:
        cls = _MIXED_CLASSES.get(key, None)
        if cls is None:
            cls = type(base.__name__, tuple(bases), {
                '__init__': DatabaseError.__init__,
                '__str__': DatabaseError.__str__,
                '__reduce__': DatabaseError.__reduce__,
                '__module__': __name__,
            })
            _MIXED_CLASSES[key] = cls
    return cls


def _resource_closed() -> Exception:
    sa_exc = sys.modules.get('sqlalchemy.exc', None)
    sa_cls = getattr(sa_exc, 'ResourceClosedError', None) if sa_exc is not None else None
    if sa_cls is None:
        return ResourceClosedError()
    with _MIXED_LOCK:
        key = ('ResourceClosedError', id(sa_cls))
        cls = _MIXED_CLASSES.get(key, None)
        if cls is None:
            cls = type('ResourceClosedError', (ResourceClosedError, sa_cls), {
                '__init__': ResourceClosedError.__init__,
                '__module__': __name__,
            })
            _MIXED_CLASSES[key] = cls
    return cls()


def database_error(
    exc: BaseException,
    statement: Optional[str] = None,
    params: Any = None,
    for_read: bool = False,
):
    """
    The PEP 249 exception for an error the Rust connector raised (`_core.SqlDatabaseError` with
    args `(message, kind, sqlstate)`; `for_read`: also a `pandas.errors.DatabaseError`); other
    exceptions are returned unchanged.
    """
    from meerschaum import _core
    if not isinstance(exc, getattr(_core, 'SqlDatabaseError', ())):
        return exc
    args = tuple(exc.args) + (None, None, None)
    message, kind, sqlstate = args[0], args[1], args[2]
    if kind == 'PermissionError':
        return PermissionError(message)
    cls = _error_class(kind or 'DatabaseError', for_read=for_read)
    return cls(str(message), statement, params, sqlstate)


# ------------------------------------------------------------------------------------------------
# Statements and parameters

#: SQLAlchemy `text()`'s bind parameters (`TextClause._bind_params_regex`).
_BIND_RE = re.compile(r"(?<![:\w\x5c]):(\w+)(?!:)", re.UNICODE)
#: SQLAlchemy `text()`'s escaped colons (`\:name` is the literal `:name`).
_ESCAPED_COLON_RE = re.compile(r"\x5c(:[\w\$]*)(?![:\w\$])", re.UNICODE)
#: psycopg / PyMySQL named markers (`%(name)s`).
_PYFORMAT_RE = re.compile(r"%\((\w+)\)s")


def _placeholder(flavor: str, n: int) -> str:
    """The flavor's `n`-th (1-based) positional placeholder."""
    if flavor in _QMARK_FLAVORS:
        return '?'
    if flavor == 'mssql':
        return f'@P{n}'
    if flavor == 'oracle':
        return f':{n}'
    return f'${n}'


def _pg_literal(value: str, flavor: str) -> str:
    """
    An untyped PostgreSQL string literal for `value`: psycopg sends `str` parameters without a
    type, so PostgreSQL casts them to what the statement needs (`ts > :begin` with
    `'2024-01-01'`); the Rust driver binds typed values, so text inlines as a dollar-quoted
    literal (whose tag never occurs in the value) instead.
    """
    if flavor == 'cockroachdb':
        return "'" + value.replace("'", "''") + "'"
    tag = 'mrsm'
    while f'${tag}$' in value:
        tag += '_'
    return f'${tag}${value}${tag}$'


def _inline_as_literal(value: Any, flavor: str) -> bool:
    """Whether a parameter value inlines as an untyped literal (PostgreSQL text-like values)."""
    if flavor not in _PG_FAMILY:
        return False
    if isinstance(value, str):
        return True
    import decimal
    import uuid
    return isinstance(value, (decimal.Decimal, uuid.UUID))


def _scan(sql: str) -> Iterator[Tuple[int, str]]:
    """
    Yield `(position, character)` for each character of `sql` outside string literals, quoted
    identifiers and comments.
    """
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ''
        if c == '-' and nxt == '-':
            j = sql.find('\n', i)
            i = n if j < 0 else j
            continue
        if c == '/' and nxt == '*':
            j = sql.find('*/', i + 2)
            i = n if j < 0 else j + 2
            continue
        if c in ("'", '"', '`'):
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:
                        j += 2
                        continue
                    break
                j += 1
            i = j + 1
            continue
        yield i, c
        i += 1


def _positional_markers(sql: str) -> Dict[str, List[int]]:
    """The positions of `?` and `%s` markers outside literals and comments."""
    found: Dict[str, List[int]] = {'?': [], '%s': []}
    for i, c in _scan(sql):
        if c == '?':
            found['?'].append(i)
        elif c == '%' and sql[i + 1:i + 2] == 's' and (i == 0 or sql[i - 1] != '%'):
            found['%s'].append(i)
    return found


class PreparedStatement:
    """A statement ready for the Rust connector: SQL text and its positional values."""

    __slots__ = ('sql', 'params', 'many', 'original')

    def __init__(self, sql: str, params: Any, many: bool, original: str):
        self.sql = sql
        self.params = params
        self.many = many
        self.original = original


def _bind_named(
    sql: str,
    values: Dict[str, Any],
    flavor: str,
    compiled: bool = False,
) -> Tuple[str, List[Any]]:
    """
    Replace SQLAlchemy `:name` binds (or `%(name)s` markers) with positional placeholders.
    `compiled`: the text of a compiled SQLAlchemy construct (`%(name)s` markers, `%%` for `%`).
    With `%(name)s` markers, `%%` in the statement's own text is a literal `%` (inlined values
    are never unescaped).
    """
    if compiled:
        regex = _PYFORMAT_RE
    else:
        regex = _BIND_RE
        if not _BIND_RE.search(sql) and _PYFORMAT_RE.search(sql):
            regex = _PYFORMAT_RE
    pyformat = regex is _PYFORMAT_RE
    names = [m.group(1) for m in regex.finditer(sql)]
    missing = [name for name in names if name not in values]
    if missing:
        raise ProgrammingError(
            f"A value is required for bind parameter '{missing[0]}'", statement=sql,
        )

    def text_of(segment: str) -> str:
        if pyformat:
            return segment.replace('%%', '%')
        return _ESCAPED_COLON_RE.sub(lambda m: m.group(1), segment)

    out: List[Any] = []
    pieces: List[str] = []
    last = 0
    for match in regex.finditer(sql):
        pieces.append(text_of(sql[last:match.start()]))
        value = values[match.group(1)]
        if _inline_as_literal(value, flavor):
            pieces.append(_pg_literal(str(value), flavor))
        else:
            out.append(value)
            pieces.append(_placeholder(flavor, len(out)))
        last = match.end()
    pieces.append(text_of(sql[last:]))
    return ''.join(pieces), out


def _bind_positional(sql: str, values: Sequence[Any], flavor: str) -> Tuple[str, List[Any]]:
    """Replace `?` (or `%s`) markers with the flavor's placeholders, in order."""
    values = list(values)
    markers = _positional_markers(sql)
    order = ('%s', '?') if flavor in _PG_FAMILY or flavor in ('mysql', 'mariadb') else ('?', '%s')
    chosen = None
    for style in order:
        if len(markers[style]) == len(values):
            chosen = style
            break
    if chosen is None:
        ### The statement already uses the flavor's own placeholders (`$1`, `:1`, `@P1`).
        return sql, values
    out: List[Any] = []
    pieces: List[str] = []
    last = 0
    width = 1 if chosen == '?' else 2
    ### psycopg's convention: with `%s` markers, `%%` is a literal `%`.
    unescape = (lambda text: text.replace('%%', '%')) if chosen == '%s' else (lambda text: text)
    for pos, value in zip(markers[chosen], values):
        pieces.append(unescape(sql[last:pos]))
        if _inline_as_literal(value, flavor):
            pieces.append(_pg_literal(str(value), flavor))
        else:
            out.append(value)
            pieces.append(_placeholder(flavor, len(out)))
        last = pos + width
    pieces.append(unescape(sql[last:]))
    return ''.join(pieces), out


def _dialect(connector: Any):
    """
    A SQLAlchemy dialect for the connector's flavor, with `pyformat` parameters (`%(name)s`:
    unambiguous next to casts such as `::INTEGER`, every literal `%` doubled).
    """
    from meerschaum.connectors.sql._create_engine import import_sqlalchemy
    sqlalchemy = import_sqlalchemy()
    flavor = connector.flavor
    modules = {
        'sqlite': 'sqlite', 'geopackage': 'sqlite',
        'mysql': 'mysql', 'mariadb': 'mysql',
        'mssql': 'mssql', 'oracle': 'oracle',
    }
    import importlib
    name = modules.get(flavor, 'postgresql')
    if flavor == 'duckdb':
        try:
            import duckdb_engine
            return duckdb_engine.Dialect(paramstyle='pyformat')
        except Exception:
            pass
    module = importlib.import_module(f'sqlalchemy.dialects.{name}')
    _ = sqlalchemy
    return module.dialect(paramstyle='pyformat')


def _compile(connector: Any, query: Any) -> Tuple[str, Dict[str, Any]]:
    """
    A SQLAlchemy construct (`text()`, `select()`, `insert()`, DDL, ...) as SQL text with
    `%(name)s` binds and the values it carries (expanding `IN` parameters rendered).
    """
    compiled = query.compile(
        dialect=_dialect(connector),
        compile_kwargs={'render_postcompile': True},
    )
    return str(compiled), dict(compiled.params or {})


def prepare(connector: Any, query: Any, params: Any = None) -> PreparedStatement:
    """
    The statement `query` (text, or a SQLAlchemy construct) with `params` (a dict for `:name`
    binds or `%(name)s` markers, a list / tuple of values for `?` / `%s` markers, a list of
    dicts for `executemany`) as text with the flavor's positional placeholders.
    """
    flavor = connector.flavor
    compiled_params: Dict[str, Any] = {}
    compiled = False
    if isinstance(query, str):
        sql = query
    elif hasattr(query, 'compile'):
        sql, compiled_params = _compile(connector, query)
        compiled = True
    else:
        sql = str(query)

    if isinstance(params, (list, tuple)) and params and all(isinstance(p, dict) for p in params):
        sets = []
        text = sql
        for values in params:
            merged = {**compiled_params, **values}
            text, ordered = _bind_named(sql, merged, flavor, compiled)
            sets.append(ordered)
        if flavor in _PG_FAMILY and any(_inline_as_literal(v, flavor) for p in params for v in p.values()):
            ### Inlined text differs per row: one statement per parameter set.
            statements = []
            for values in params:
                merged = {**compiled_params, **values}
                statements.append(_bind_named(sql, merged, flavor, compiled))
            return PreparedStatement(statements, None, True, sql)
        return PreparedStatement(text, sets, True, sql)

    if isinstance(params, dict) or compiled:
        merged = {**compiled_params, **(params or {})}
        text, ordered = _bind_named(sql, merged, flavor, compiled)
        return PreparedStatement(text, ordered, False, sql)

    if isinstance(params, (list, tuple)) and params:
        text, ordered = _bind_positional(sql, params, flavor)
        return PreparedStatement(text, ordered, False, sql)

    return PreparedStatement(_ESCAPED_COLON_RE.sub(lambda m: m.group(1), sql), [], False, sql)


def returns_rows(sql: str, flavor: str) -> bool:
    """Whether a statement returns rows (`driver::raw::returns_rows`)."""
    from meerschaum import _core
    return _core.sql_returns_rows(sql, flavor)


# ------------------------------------------------------------------------------------------------
# Results

class Row(tuple):
    """
    A result row: a tuple (index, unpack, compare with tuples) whose values are also reachable by
    column name (`row.name`, `row._mapping['name']`, `row._asdict()`), like SQLAlchemy's `Row`.
    """

    __slots__ = ()
    _fields: Tuple[str, ...] = ()
    _index: Dict[str, int] = {}

    def __getattr__(self, name: str) -> Any:
        try:
            return self[type(self)._index[name]]
        except KeyError:
            raise AttributeError(name) from None

    @property
    def _mapping(self) -> 'RowMapping':
        return RowMapping(zip(self._fields, self))

    def _asdict(self) -> Dict[str, Any]:
        return dict(zip(self._fields, self))

    @property
    def _t(self) -> tuple:
        return tuple(self)

    def _tuple(self) -> tuple:
        return tuple(self)


class RowMapping(dict):
    """A row as a dictionary of column name to value (`result.mappings()`)."""


def _row_class(keys: Sequence[str]) -> type:
    index: Dict[str, int] = {}
    for i, key in enumerate(keys):
        index.setdefault(key, i)
    return type('Row', (Row,), {'__slots__': (), '_fields': tuple(keys), '_index': index})


class _ScalarResult:
    """`result.scalars()`: the values of one column."""

    def __init__(self, values: List[Any]):
        self._values = values

    def all(self) -> List[Any]:
        values, self._values = self._values, []
        return values

    def first(self) -> Any:
        return self._values[0] if self._values else None

    def one(self) -> Any:
        if len(self._values) != 1:
            raise _no_result(len(self._values))
        return self._values[0]

    def one_or_none(self) -> Any:
        if len(self._values) > 1:
            raise _no_result(len(self._values))
        return self._values[0] if self._values else None

    def __iter__(self):
        return iter(self.all())


class _MappingResult(_ScalarResult):
    """`result.mappings()`: each row as a `RowMapping`."""


def _no_result(count: int) -> Exception:
    sa_exc = sys.modules.get('sqlalchemy.exc', None)
    message = (
        "No row was found when one was required"
        if count == 0
        else "Multiple rows were found when exactly one was required"
    )
    cls_name = 'NoResultFound' if count == 0 else 'MultipleResultsFound'
    cls = getattr(sa_exc, cls_name, None) if sa_exc is not None else None
    return cls(message) if cls is not None else LookupError(message)


class NativeResult:
    """
    The result of a raw statement through the Rust connector, with the parts of SQLAlchemy's
    `CursorResult` that code written for Meerschaum 4 uses: `fetchall()`, `fetchone()`,
    `fetchmany()`, `all()`, `first()`, `one()`, `one_or_none()`, `scalar()`, `scalar_one()`,
    `scalars()`, `mappings()`, `keys()`, iteration, `rowcount`, `returns_rows`, `close()`.

    A statement that returns no rows (`INSERT`, `CREATE TABLE`, ...) has `returns_rows=False` and
    its `rowcount`; asking it for rows raises `ResourceClosedError`, as SQLAlchemy does.
    """

    def __init__(
        self,
        keys: Optional[Sequence[str]] = None,
        rows: Optional[List[tuple]] = None,
        rowcount: int = -1,
    ):
        self._keys = list(keys) if keys is not None else []
        self.returns_rows = rows is not None
        cls = _row_class(self._keys)
        self._rows: List[Row] = [cls(row) for row in rows] if rows is not None else []
        self._pos = 0
        self.rowcount = rowcount
        self.closed = rows is None
        self.lastrowid = None

    def __repr__(self) -> str:
        if not self.returns_rows:
            return f"<NativeResult rowcount={self.rowcount}>"
        return f"<NativeResult rows={len(self._rows)} keys={self._keys}>"

    def _check(self) -> None:
        if not self.returns_rows:
            raise _resource_closed()

    def keys(self) -> List[str]:
        """The column names."""
        return list(self._keys)

    def fetchall(self) -> List[Row]:
        """The remaining rows."""
        self._check()
        rows = self._rows[self._pos:]
        self._pos = len(self._rows)
        return rows

    def all(self) -> List[Row]:
        """The remaining rows (`fetchall()`)."""
        return self.fetchall()

    def fetchone(self) -> Optional[Row]:
        """The next row, or `None`."""
        self._check()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: Optional[int] = None) -> List[Row]:
        """The next `size` rows (1 by default)."""
        self._check()
        size = size or 1
        rows = self._rows[self._pos:self._pos + size]
        self._pos += len(rows)
        return rows

    def first(self) -> Optional[Row]:
        """The first remaining row (or `None`); the result is closed afterwards."""
        self._check()
        row = self._rows[self._pos] if self._pos < len(self._rows) else None
        self.close()
        return row

    def one(self) -> Row:
        """The only row (an error when there are none or several)."""
        rows = self.fetchall()
        if len(rows) != 1:
            raise _no_result(len(rows))
        return rows[0]

    def one_or_none(self) -> Optional[Row]:
        """The only row or `None` (an error when there are several)."""
        rows = self.fetchall()
        if len(rows) > 1:
            raise _no_result(len(rows))
        return rows[0] if rows else None

    def scalar(self) -> Any:
        """The first column of the first row, or `None`."""
        row = self.first()
        return row[0] if row is not None and len(row) else None

    def scalar_one(self) -> Any:
        """The first column of the only row."""
        return self.one()[0]

    def scalar_one_or_none(self) -> Any:
        """The first column of the only row, or `None`."""
        row = self.one_or_none()
        return row[0] if row is not None else None

    def scalars(self, index: int = 0) -> _ScalarResult:
        """The values of column `index` of the remaining rows."""
        return _ScalarResult([row[index] for row in self.fetchall()])

    def mappings(self) -> _MappingResult:
        """The remaining rows as dictionaries (`RowMapping`)."""
        return _MappingResult([row._mapping for row in self.fetchall()])

    def tuples(self) -> 'NativeResult':
        """This result (rows are already tuples)."""
        return self

    def close(self) -> None:
        """Close the result (the rows were fetched when the statement ran)."""
        self._pos = len(self._rows)
        self.closed = True

    def __iter__(self) -> Iterator[Row]:
        self._check()
        while self._pos < len(self._rows):
            row = self._rows[self._pos]
            self._pos += 1
            yield row


_DML_WORDS = {'INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'MERGE', 'UPSERT', 'COPY'}
_FIRST_WORD_RE = re.compile(r"^(?:\s+|--[^\n]*\n?|/\*.*?\*/|\()*([A-Za-z]+)", re.DOTALL)


def _rowcount_of_statement(sql: str, flavor: str, affected: int) -> int:
    """
    `cursor.rowcount` after a statement without rows: the rows affected by a write, `-1` for
    other statements (DDL, ...) on the drivers that report it so (`sqlite3`, psycopg, pyodbc,
    duckdb); PyMySQL and python-oracledb report the count.
    """
    if flavor in ('mysql', 'mariadb', 'oracle'):
        return affected
    match = _FIRST_WORD_RE.match(sql or '')
    word = match.group(1).upper() if match else ''
    if word in _DML_WORDS or word == 'WITH':
        return affected
    return -1


def _rowcount_of_query(flavor: str, nrows: int) -> int:
    """`cursor.rowcount` after a query: `-1` on `sqlite3` and duckdb, else the rows returned."""
    if flavor in ('sqlite', 'geopackage', 'duckdb'):
        return -1
    return nrows


def frame_result(frame: Any, flavor: str) -> NativeResult:
    """A `NativeResult` of a `_core.SqlFrame` (a statement that returned no columns has none)."""
    columns = list(frame.columns)
    if not columns:
        return NativeResult(rowcount=-1)
    rows = frame.rows()
    return NativeResult(keys=columns, rows=rows, rowcount=_rowcount_of_query(flavor, len(rows)))


class NativeSession:
    """
    The `session` an `exec_queries()` hook receives: `execute(query)` runs in the same
    transaction as the queries before it.
    """

    def __init__(self, connector: Any, tx: Any):
        self._connector = connector
        self._tx = tx

    def execute(self, query: Any, params: Any = None, **kw: Any) -> NativeResult:
        params = params if params is not None else kw.get('parameters', None)
        stmt = prepare(self._connector, query, params)
        return run_in_tx(self._connector, self._tx, stmt)

    def flush(self) -> None:
        """Nothing to flush (statements run when executed)."""


def run_in_tx(connector: Any, tx: Any, stmt: PreparedStatement) -> NativeResult:
    """Run a prepared statement inside an open `_core.SqlTx`."""
    flavor = connector.flavor
    try:
        if stmt.many:
            total = 0
            if stmt.params is None:
                for sql, values in stmt.sql:
                    total += tx.execute(sql, values)
            else:
                for values in stmt.params:
                    total += tx.execute(stmt.sql, values)
            return NativeResult(rowcount=total)
        if returns_rows(stmt.sql, flavor):
            return frame_result(tx.query(stmt.sql, stmt.params), flavor)
        affected = tx.execute(stmt.sql, stmt.params)
        return NativeResult(rowcount=_rowcount_of_statement(stmt.sql, flavor, affected))
    except Exception as e:
        raise database_error(e, stmt.original, stmt.params) from None


def run(
    connector: Any,
    stmt: PreparedStatement,
    commit: bool = True,
    read_only: bool = False,
) -> NativeResult:
    """
    Run a prepared statement on the connector's pool: a write commits (each statement on its
    own, as autocommit drivers do), `commit=False` runs it in a transaction that is rolled back,
    `read_only` in a read-only transaction that is rolled back.
    """
    rust = connector._core_connector()
    flavor = connector.flavor
    if read_only or not commit:
        tx = rust.sql_begin(read_only=read_only)
        try:
            return run_in_tx(connector, tx, stmt)
        finally:
            tx.rollback()
    try:
        if stmt.many:
            if stmt.params is None:
                tx = rust.sql_begin(write=True)
                try:
                    total = 0
                    for sql, values in stmt.sql:
                        total += tx.execute(sql, values)
                    tx.commit()
                finally:
                    tx.rollback()
                return NativeResult(rowcount=total)
            return NativeResult(rowcount=rust.sql_execute_many(stmt.sql, stmt.params))
        if returns_rows(stmt.sql, flavor):
            chunks = rust.sql_query(stmt.sql, stmt.params, sys.maxsize)
            frames = list(chunks)
            if not frames:
                return NativeResult(keys=list(chunks.columns), rows=[], rowcount=0)
            if len(frames) == 1:
                return frame_result(frames[0], flavor)
            keys = list(frames[0].columns)
            rows: List[tuple] = []
            for frame in frames:
                rows.extend(frame.rows())
            return NativeResult(keys=keys, rows=rows, rowcount=_rowcount_of_query(flavor, len(rows)))
        affected = rust.sql_execute(stmt.sql, stmt.params)
        return NativeResult(rowcount=_rowcount_of_statement(stmt.sql, flavor, affected))
    except Exception as e:
        raise database_error(e, stmt.original, stmt.params) from None


# ------------------------------------------------------------------------------------------------
# DataFrames

def _pandas_options() -> Tuple[bool, str]:
    """pandas' string inference setting and datetime unit (`us` from pandas 3, else `ns`)."""
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()
    try:
        infer_string = bool(pd.get_option('future.infer_string'))
    except Exception:
        infer_string = False
    try:
        major = int(str(pd.__version__).split('.', 1)[0])
    except ValueError:
        major = 3
    return infer_string, ('us' if major >= 3 else 'ns')


def to_dataframe(
    frame: Any,
    library: str,
    coerce_float: bool = True,
    dtype: Optional[Dict[str, Any]] = None,
    index_col: Optional[Union[str, List[str]]] = None,
    debug: bool = False,
) -> Any:
    """
    One `_core.SqlFrame` as the requested library's frame: the one conversion of `read()`.

    pandas: the frame v4's `pd.read_sql_query()` built from the DBAPI's values (`SqlFrame.to_pandas`,
    pandas' own inference rules for raw SQL results), then `astype(dtype)` and `set_index(index_col)`.
    Polars: `meerschaum.utils.dataframe.frame_from_arrow` over the Arrow columns (`dtype` declares
    `json` / `geometry` columns).
    """
    if library == 'polars':
        from meerschaum.utils.dataframe import frame_from_arrow
        dtypes = {str(k): str(v) for k, v in dtype.items()} if dtype else None
        return frame_from_arrow(frame, as_polars=True, dtypes=dtypes, debug=debug)
    infer_string, unit = _pandas_options()
    df = frame.to_pandas(coerce_float=coerce_float, infer_string=infer_string, unit=unit)
    if dtype:
        df = df.astype(dtype)
    if index_col is not None:
        df = df.set_index(index_col)
    return df


def _concat(frames: List[Any], library: str) -> Any:
    if library == 'polars':
        import polars as pl
        return pl.concat(frames, how='diagonal_relaxed') if len(frames) > 1 else frames[0]
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()
    if len(frames) == 1:
        return frames[0]
    return pd.concat(frames)


def _resolve_library(as_polars: Optional[bool], as_pandas: Optional[bool]) -> str:
    try:
        from meerschaum.utils.dataframe import resolve_dataframe_library
    except ImportError:
        return 'polars' if as_polars else 'pandas'
    return resolve_dataframe_library(as_polars=as_polars, as_pandas=as_pandas)


# ------------------------------------------------------------------------------------------------
# The connector methods (`_sql.py` dispatches here for flavors in `NATIVE_SQL_FLAVORS`)

def read(
    self,
    query_or_table: Any,
    params: Any = None,
    dtype: Optional[Dict[str, Any]] = None,
    coerce_float: bool = True,
    chunksize: Optional[int] = -1,
    workers: Optional[int] = None,
    chunk_hook: Optional[Any] = None,
    as_hook_results: bool = False,
    chunks: Optional[int] = None,
    schema: Optional[str] = None,
    as_chunks: bool = False,
    as_iterator: bool = False,
    index_col: Optional[str] = None,
    silent: bool = False,
    debug: bool = False,
    as_polars: Optional[bool] = None,
    as_pandas: Optional[bool] = None,
    **kw: Any
) -> Any:
    """`SQLConnector.read()` through the Rust connector (see `_sql.read` for the parameters)."""
    import time
    import traceback
    from meerschaum.utils.sql import sql_item_name, truncate_item_name
    from meerschaum.utils.dtypes import are_dtypes_equal, coerce_timezone
    from meerschaum.utils.dtypes.sql import TIMEZONE_NAIVE_FLAVORS
    from meerschaum.utils.pool import get_pool
    from meerschaum.utils.misc import filter_arguments
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.connectors.sql._sql import (
        _disallow_chunks_flavors, _max_chunks_flavors, _read_guard,
    )

    if chunks is not None and chunks <= 0:
        return []

    library = _resolve_library(as_polars, as_pandas)
    if library == 'pandas':
        from meerschaum.connectors.sql._create_engine import import_pandas_for_sql
        import_pandas_for_sql()

    schema = schema or self.schema
    utc_dt_cols = [
        col
        for col, typ in dtype.items()
        if are_dtypes_equal(str(typ), 'datetime') and 'utc' in str(typ).lower()
    ] if dtype else []
    if dtype and utc_dt_cols and self.flavor in TIMEZONE_NAIVE_FLAVORS:
        dtype = dtype.copy()
        for col in utc_dt_cols:
            dtype[col] = 'datetime64[us]'

    default_chunksize = self._sys_config.get('chunksize', None)
    chunksize = chunksize if chunksize != -1 else default_chunksize
    if chunksize is None and as_iterator:
        if not silent and self.flavor not in _disallow_chunks_flavors:
            warn(
                "An iterator may only be generated if chunksize is not None.\n"
                + "Falling back to a chunksize of 1000.", stacklevel=3,
            )
        chunksize = 1000
    if chunksize is not None and self.flavor in _max_chunks_flavors:
        if chunksize > _max_chunks_flavors[self.flavor]:
            if chunksize != default_chunksize:
                warn(
                    f"The specified chunksize of {chunksize} exceeds the maximum of "
                    + f"{_max_chunks_flavors[self.flavor]} for flavor '{self.flavor}'.\n"
                    + f"    Falling back to a chunksize of {_max_chunks_flavors[self.flavor]}.",
                    stacklevel=3,
                )
            chunksize = _max_chunks_flavors[self.flavor]
    if chunksize is not None and self.flavor in _disallow_chunks_flavors:
        chunksize = None

    if debug:
        start = time.perf_counter()
        dprint(f"[{self}]\n{query_or_table}")
        dprint(f"[{self}] Fetching with chunksize: {chunksize}")

    ### A table name (no spaces, or a quoted name).
    query_text = str(query_or_table) if isinstance(query_or_table, str) else None
    if query_text is not None and (
        ' ' not in query_text
        or (query_text.startswith('"') and query_text.endswith('"'))
    ):
        truncated_table_name = truncate_item_name(query_text, self.flavor)
        if truncated_table_name != query_text and not silent:
            if self.flavor not in ('oracle', 'mysql', 'mariadb'):
                warn(
                    f"Table '{query_text}' is too long for '{self.flavor}',"
                    + f" will instead read the table '{truncated_table_name}'."
                )
        table_name = sql_item_name(query_text, self.flavor, schema)
        if debug:
            dprint(f"[{self}] Reading from table {table_name}")
        query_or_table = f"SELECT * FROM {table_name}"

    read_only, refusal = _read_guard(self, str(query_or_table))
    if refusal is not None:
        if silent:
            return None
        raise PermissionError(refusal)

    try:
        stmt = prepare(self, query_or_table, params)
    except Exception:
        if silent:
            return None
        raise

    ### Chunks are visible to the caller only through these: otherwise the rows are read in one
    ### batch and typed once (v4 concatenated per-chunk frames; see PARITY.md).
    chunked = chunksize is not None and (
        as_iterator or as_chunks or chunk_hook is not None or chunks is not None
    )
    batch_size = chunksize if chunked else sys.maxsize

    def convert(frame):
        df = to_dataframe(frame, library, coerce_float, dtype, index_col, debug=debug)
        if library == 'pandas' and self.flavor in TIMEZONE_NAIVE_FLAVORS:
            for col in utc_dt_cols:
                if col in df.columns:
                    df[col] = coerce_timezone(df[col], strip_utc=False)
        return df

    def open_frames() -> Tuple[Iterable[Any], Any]:
        """The source of `SqlFrame` batches, and how to close it."""
        rust = self._core_connector()
        if read_only:
            tx = rust.sql_begin(read_only=True)
            try:
                whole = tx.query(stmt.sql, stmt.params)
            finally:
                tx.rollback()
            frames = (
                [whole.slice(i, batch_size) for i in range(0, max(len(whole), 1), batch_size)]
                if chunked and len(whole) > batch_size
                else [whole]
            )
            return frames, (lambda: None)
        source = rust.sql_query(stmt.sql, stmt.params, batch_size)
        return source, source.close

    try:
        frames, close = open_frames()
    except Exception as e:
        if debug:
            dprint(f"[{self}] Failed to execute query:\n\n{query_or_table}\n\n")
        if silent:
            return None
        raise database_error(e, stmt.original, stmt.params, for_read=True) from None

    if as_iterator:
        def _iterate():
            try:
                for frame in frames:
                    yield convert(frame)
            except Exception as e:
                raise database_error(e, stmt.original, stmt.params, for_read=True) from None
            finally:
                close()
        return _iterate()

    def _get_chunk_args_kwargs(_chunk):
        return filter_arguments(
            chunk_hook,
            _chunk,
            workers=workers,
            chunksize=chunksize,
            debug=debug,
            **kw
        )

    def _process_chunk(_chunk, _retry_on_failure: bool = True):
        chunk_args, chunk_kwargs = _get_chunk_args_kwargs(_chunk)
        result = None
        try:
            result = chunk_hook(*chunk_args, **chunk_kwargs)
        except Exception:
            result = False, traceback.format_exc()
            from meerschaum.utils.formatting import print_exception
            if not silent:
                print_exception()
        if isinstance(result, tuple) and result[0] is False and _retry_on_failure:
            return _process_chunk(_chunk, _retry_on_failure=False)
        return result

    chunk_list: List[Any] = []
    chunk_hook_results: List[Any] = []
    try:
        try:
            if chunk_hook is not None and as_hook_results and chunked:
                pool = get_pool(workers=workers)
                return list(pool.imap(_process_chunk, (convert(f) for f in frames)))
            read_chunks = 0
            for frame in frames:
                chunk = convert(frame)
                if chunk_hook is not None and chunked:
                    chunk_args, chunk_kwargs = _get_chunk_args_kwargs(chunk)
                    chunk_hook_results.append(chunk_hook(*chunk_args, **chunk_kwargs))
                chunk_list.append(chunk)
                read_chunks += 1
                if chunks is not None and read_chunks >= chunks:
                    break
        finally:
            close()
    except Exception as e:
        if debug:
            dprint(f"[{self}] Failed to execute query:\n\n{query_or_table}\n\n")
        if silent:
            return None
        raise database_error(e, stmt.original, stmt.params, for_read=True) from None

    ### Call the hook on any missed chunks (an unchunked read).
    if chunk_hook is not None and len(chunk_list) > len(chunk_hook_results):
        for c in chunk_list[len(chunk_hook_results):]:
            chunk_args, chunk_kwargs = _get_chunk_args_kwargs(c)
            chunk_hook_results.append(chunk_hook(*chunk_args, **chunk_kwargs))

    if debug:
        end = time.perf_counter()
        dprint(f"Fetched {len(chunk_list)} chunks in {round(end - start, 2)} seconds.")

    if as_hook_results:
        return chunk_hook_results

    if as_chunks:
        if library == 'pandas' and index_col is None:
            for c in chunk_list:
                c.reset_index(drop=True, inplace=True)
        return chunk_list

    df = _concat(chunk_list, library)
    if library == 'pandas' and index_col is None and len(chunk_list) > 1:
        df = df.reset_index(drop=True)
    return df


def value(self, query: Any, *args: Any, use_pandas: bool = False, **kw: Any) -> Any:
    """`SQLConnector.value()` through the Rust connector (see `_sql.value`)."""
    from meerschaum.connectors.sql._sql import _read_guard
    silent = kw.get('silent', False)
    debug = kw.get('debug', False)
    if self.flavor == 'duckdb':
        use_pandas = True
    if use_pandas:
        df = self.read(query, *args, as_pandas=True, **kw)
        if df is None:
            return None
        try:
            return df.iloc[0, 0]
        except IndexError:
            return None

    _close = kw.pop('close', True)
    _commit = kw.pop('commit', (self.flavor != 'mssql'))
    _ = _close
    params = args[0] if args else kw.get('params', kw.get('parameters', None))

    read_only, refusal = _read_guard(self, str(query))
    if refusal is not None:
        if silent:
            return None
        raise PermissionError(refusal)

    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"[{self}] Executing query:\n{query}")
    try:
        stmt = prepare(self, query, params)
        if stmt.many or not returns_rows(stmt.sql, self.flavor):
            ### v4 ran the statement, then `result.first()` raised (no rows to return).
            run(self, stmt, commit=bool(_commit), read_only=read_only)
            raise _resource_closed()
        rust = self._core_connector()
        if read_only:
            tx = rust.sql_begin(read_only=True)
            try:
                frame = tx.query(stmt.sql, stmt.params)
            finally:
                tx.rollback()
            return frame.first_value()
        if not _commit:
            tx = rust.sql_begin()
            try:
                frame = tx.query(stmt.sql, stmt.params)
            finally:
                tx.rollback()
            return frame.first_value()
        source = rust.sql_query(stmt.sql, stmt.params, 1)
        try:
            for frame in source:
                return frame.first_value()
            return None
        finally:
            source.close()
    except Exception as e:
        if silent:
            return None
        raise database_error(e, str(query), params) from None


def exec(
    self,
    query: Any,
    *args: Any,
    silent: bool = False,
    debug: bool = False,
    commit: Optional[bool] = None,
    close: Optional[bool] = None,
    **kw: Any
) -> Optional[NativeResult]:
    """`SQLConnector.exec()` through the Rust connector (see `_sql.exec`)."""
    _ = close
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"[{self}] Executing query:\n{query}")
    _commit = commit if commit is not None else (
        (self.flavor != 'mssql' or 'select' not in str(query).lower())
    )
    params = args[0] if args else kw.pop('parameters', kw.pop('params', None))
    try:
        stmt = prepare(self, query, params)
        result = run(self, stmt, commit=bool(_commit))
    except Exception as e:
        if debug:
            from meerschaum.utils.debug import dprint
            dprint(f"[{self}] Failed to execute query:\n\n{query}\n\n{e}")
        if silent:
            return None
        raise database_error(e, str(query), params) from None
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"[{self}] Done executing.")
    return result


def exec_queries(
    self,
    queries: List[Any],
    break_on_error: bool = False,
    rollback: bool = True,
    silent: bool = False,
    debug: bool = False,
) -> List[Any]:
    """
    `SQLConnector.exec_queries()` through the Rust connector: the queries run in one
    transaction; a failed query is `None` in the results (and warned about unless `silent`); a
    `(query, hook)` pair calls `hook(session)` after `query` succeeds and runs the queries it
    returns in the same transaction (v4 opened a second one, which waited forever for the first
    one's lock on SQLite), its result then being `(result, hook_results)`.
    """
    rust = self._core_connector()
    tx = rust.sql_begin(write=True)
    try:
        results, rolled_back = _exec_queries_in(
            self, tx, queries, break_on_error, rollback, silent, debug,
        )
        if not rolled_back:
            try:
                tx.commit()
            except Exception as e:
                if not silent:
                    from meerschaum.utils.warnings import warn
                    warn(f"Encountered error while committing:\n{database_error(e)}")
    finally:
        tx.rollback()
    return results


def _exec_queries_in(
    self,
    tx: Any,
    queries: List[Any],
    break_on_error: bool,
    rollback: bool,
    silent: bool,
    debug: bool,
) -> Tuple[List[Any], bool]:
    """Run `exec_queries()`' queries in `tx`: the results and whether `tx` was rolled back."""
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    session = NativeSession(self, tx)
    results: List[Any] = []
    for query in queries:
        hook = None
        if isinstance(query, tuple):
            query, hook = query
        if debug:
            dprint(f"[{self}]\n" + str(query))
        result = None
        try:
            stmt = prepare(self, query, None)
            result = run_in_tx(self, tx, stmt)
        except Exception as e:
            msg = f"Encountered error while executing:\n{e}"
            if not silent:
                warn(msg)
            elif debug:
                dprint(f"[{self}]\n" + str(msg))
            result = None
        if debug:
            dprint(f"[{self}] Finished executing.")
        if result is None and break_on_error:
            if rollback:
                if debug:
                    dprint(f"[{self}] Rolling back...")
                tx.rollback()
            results.append(result)
            return results, rollback
        if result is not None and hook is not None:
            hook_queries = hook(session)
            if hook_queries:
                hook_results, rolled_back = _exec_queries_in(
                    self, tx, hook_queries, break_on_error, rollback, silent, debug,
                )
                result = (result, hook_results)
                if rolled_back:
                    results.append(result)
                    return results, True
        results.append(result)
    return results, False


#: pandas' `to_sql` column types (`SQLTable._sqlalchemy_type`) as each flavor's DDL writes them.
_TO_SQL_TYPES: Dict[str, Dict[str, str]] = {
    'sqlite': {
        'bigint': 'BIGINT', 'int': 'INTEGER', 'smallint': 'SMALLINT', 'float': 'FLOAT',
        'real': 'FLOAT', 'bool': 'BOOLEAN', 'datetime': 'DATETIME', 'datetimetz': 'TIMESTAMP',
        'date': 'DATE', 'time': 'TIME', 'text': 'TEXT',
    },
    'postgresql': {
        'bigint': 'BIGINT', 'int': 'INTEGER', 'smallint': 'SMALLINT', 'float': 'FLOAT(53)',
        'real': 'FLOAT(23)', 'bool': 'BOOLEAN', 'datetime': 'TIMESTAMP WITHOUT TIME ZONE',
        'datetimetz': 'TIMESTAMP WITH TIME ZONE', 'date': 'DATE',
        'time': 'TIME WITHOUT TIME ZONE', 'text': 'TEXT',
    },
    'mysql': {
        'bigint': 'BIGINT', 'int': 'INTEGER', 'smallint': 'SMALLINT', 'float': 'FLOAT(53)',
        'real': 'FLOAT(23)', 'bool': 'BOOL', 'datetime': 'DATETIME(6)', 'datetimetz': 'DATETIME(6)',
        'date': 'DATE', 'time': 'TIME(6)', 'text': 'TEXT',
    },
    'mssql': {
        'bigint': 'BIGINT', 'int': 'INTEGER', 'smallint': 'SMALLINT', 'float': 'FLOAT(53)',
        'real': 'FLOAT(24)', 'bool': 'BIT', 'datetime': 'DATETIME2(7)',
        'datetimetz': 'DATETIMEOFFSET(7)', 'date': 'DATE', 'time': 'TIME', 'text': 'NVARCHAR(MAX)',
    },
    ### v4 forced `INTEGER` for every integer column and `NVARCHAR(2000)` for text on Oracle.
    'oracle': {
        'bigint': 'INTEGER', 'int': 'INTEGER', 'smallint': 'INTEGER', 'float': 'BINARY_DOUBLE',
        'real': 'BINARY_FLOAT', 'bool': 'INTEGER', 'datetime': 'TIMESTAMP(9)',
        'datetimetz': 'TIMESTAMP(9) WITH TIME ZONE', 'date': 'DATE', 'time': 'NVARCHAR2(2000)',
        'text': 'NVARCHAR2(2000)',
    },
    'duckdb': {
        'bigint': 'BIGINT', 'int': 'INTEGER', 'smallint': 'SMALLINT', 'float': 'DOUBLE',
        'real': 'FLOAT', 'bool': 'BOOLEAN', 'datetime': 'TIMESTAMP', 'datetimetz': 'TIMESTAMPTZ',
        'date': 'DATE', 'time': 'TIME', 'text': 'TEXT',
    },
}
_TO_SQL_FAMILY = {
    'geopackage': 'sqlite', 'mariadb': 'mysql', 'postgis': 'postgresql',
    'timescaledb': 'postgresql', 'timescaledb-ha': 'postgresql', 'citus': 'postgresql',
    'cockroachdb': 'postgresql',
}


def to_sql_column_type(series: Any, flavor: str) -> str:
    """
    The database type pandas' `to_sql()` gives a column (`lib.infer_dtype(col, skipna=True)` as
    `SQLTable._sqlalchemy_type` maps it), in the flavor's DDL.
    """
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()
    types = _TO_SQL_TYPES[_TO_SQL_FAMILY.get(flavor, flavor)]
    col_type = pd.api.types.infer_dtype(series, skipna=True)
    if col_type in ('datetime64', 'datetime'):
        tz = getattr(getattr(series, 'dt', None), 'tz', None)
        return types['datetimetz'] if tz is not None else types['datetime']
    if col_type == 'timedelta64':
        return types['bigint']
    if col_type == 'floating':
        return types['real'] if str(series.dtype) == 'float32' else types['float']
    if col_type == 'integer':
        name = str(series.dtype).lower()
        if name in ('int8', 'uint8', 'int16'):
            return types['smallint']
        if name in ('uint16', 'int32'):
            return types['int']
        if name == 'uint64':
            raise ValueError("Unsigned 64 bit integer datatype is not supported")
        return types['bigint']
    if col_type == 'boolean':
        return types['bool']
    if col_type == 'date':
        return types['date']
    if col_type == 'time':
        return types['time']
    if col_type == 'complex':
        raise ValueError("Complex datatypes not supported")
    return types['text']


def _db_type_text(connector: Any, typ: Any) -> str:
    """A `dtype=` value as DDL text: SQLAlchemy types compile for the flavor; text stays."""
    if isinstance(typ, str):
        return typ
    if isinstance(typ, type):
        typ = typ()
    if hasattr(typ, 'compile'):
        return typ.compile(dialect=_dialect(connector))
    return str(typ)


def to_sql_write(
    self,
    df: Any,
    name: str,
    schema: Optional[str],
    if_exists: str,
    index: bool,
    chunksize: Optional[int],
    dtype: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    """
    Write the prepared frame of `SQLConnector.to_sql()` through the Rust connector: the table is
    created with pandas' column types (or `dtype`) as `if_exists` asks, then the rows are inserted
    in the same transaction. Returns `(success, error message)`.
    """
    if if_exists not in ('fail', 'replace', 'append'):
        raise ValueError(f"'{if_exists}' is not valid for if_exists")
    index_columns: List[str] = []
    if index:
        before = list(df.columns)
        df = df.reset_index()
        index_columns = [str(c) for c in df.columns if c not in before]
    column_types = {
        str(col): to_sql_column_type(df[col], self.flavor)
        for col in df.columns
    }
    for col, typ in (dtype or {}).items():
        if str(col) in column_types:
            column_types[str(col)] = _db_type_text(self, typ)
    rust = self._core_connector()
    ok, msg = rust.sql_to_sql(
        df,
        name,
        schema,
        if_exists,
        column_types,
        index_columns,
        chunksize,
    )
    return ok, msg


def polars_column_type(series: Any, flavor: str) -> str:
    """The `to_sql()` column type of a Polars column (the type pandas would give its values)."""
    import polars as pl
    types = _TO_SQL_TYPES[_TO_SQL_FAMILY.get(flavor, flavor)]
    dtype = series.dtype
    if dtype in (pl.Int8, pl.Int16, pl.UInt8):
        return types['smallint']
    if dtype in (pl.Int32, pl.UInt16):
        return types['int']
    if dtype in (pl.Int64, pl.UInt32, pl.Duration):
        return types['bigint']
    if dtype == pl.UInt64:
        raise ValueError("Unsigned 64 bit integer datatype is not supported")
    if dtype == pl.Float32:
        return types['real']
    if dtype == pl.Float64:
        return types['float']
    if dtype == pl.Boolean:
        return types['bool']
    if isinstance(dtype, pl.Datetime):
        return types['datetimetz'] if dtype.time_zone else types['datetime']
    if dtype == pl.Date:
        return types['date']
    if dtype == pl.Time:
        return types['time']
    return types['text']


def to_sql_polars(
    self,
    df: Any,
    name: str,
    if_exists: str = 'replace',
    chunksize: Optional[int] = -1,
    schema: Optional[str] = None,
    silent: bool = False,
    debug: bool = False,
    as_tuple: bool = False,
    as_dict: bool = False,
    dtype: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    `SQLConnector.to_sql()` of a Polars frame through the Rust connector (the columns cross as
    Arrow: JSON, UUID, decimal and datetime values are encoded for the flavor in Rust).
    """
    import time
    from datetime import timedelta
    from meerschaum.utils.misc import interval_str
    from meerschaum.utils.sql import truncate_item_name
    from meerschaum.utils.warnings import warn
    from meerschaum.connectors.sql._sql import _max_chunks_flavors

    default_chunksize = self._sys_config.get('chunksize', None)
    chunksize = chunksize if chunksize != -1 else default_chunksize
    if chunksize is not None and self.flavor in _max_chunks_flavors:
        chunksize = min(chunksize, _max_chunks_flavors[self.flavor])
    truncated_name = truncate_item_name(name, self.flavor)
    if name != truncated_name and self.flavor not in ('oracle', 'mysql', 'mariadb'):
        warn(
            f"Table '{name}' is too long for '{self.flavor}',"
            f" will instead create the table '{truncated_name}'."
        )
    column_types = {col: polars_column_type(df[col], self.flavor) for col in df.columns}
    for col, typ in (dtype or {}).items():
        if str(col) in column_types:
            column_types[str(col)] = _db_type_text(self, typ)
    stats = {'target': name, 'method': 'multi', 'chunksize': chunksize, 'num_rows': len(df)}
    start = time.perf_counter()
    try:
        success, msg = self._core_connector().sql_to_sql(
            df, truncated_name, schema, if_exists, column_types, [], chunksize,
        )
    except Exception as e:
        success, msg = False, str(database_error(e))
    end = time.perf_counter()
    if success:
        msg = (
            f"It took {interval_str(timedelta(seconds=(end - start)))} "
            + f"to sync {len(df):,} row"
            + ('s' if len(df) != 1 else '')
            + f" to {name}."
        )
    elif not silent:
        warn(msg)
    stats.update({'start': start, 'end': end, 'duration': end - start, 'success': success, 'msg': msg})
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(msg)
    if as_tuple:
        return success, msg
    if as_dict:
        return stats
    return success


def sql_text(connectable: Any, query: str) -> Any:
    """
    `query` for `connectable.execute()`: the text itself for a `SQLConnector` whose raw SQL runs
    through the Rust connector (no SQLAlchemy needed), else `sqlalchemy.text(query)` (a
    SQLAlchemy session, engine or connection).
    """
    if (
        getattr(connectable, 'type', None) == 'sql'
        and hasattr(connectable, '_core_connector')
        and uses_native(connectable)
    ):
        return query
    from meerschaum.connectors.sql._create_engine import import_sqlalchemy
    return import_sqlalchemy().text(query)


def table_exists(connector: Any, table: str, schema: Optional[str] = None) -> Optional[bool]:
    """
    Whether `table` exists, asked through the Rust connector (`None` when the connector's raw
    SQL does not run there).
    """
    if not (getattr(connector, 'type', None) == 'sql' and uses_native(connector)):
        return None
    try:
        return connector._core_connector().sql_table_exists(table, schema)
    except Exception as e:
        raise database_error(e) from None
