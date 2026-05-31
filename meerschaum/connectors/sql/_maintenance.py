#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Run maintenance operations (VACUUM, ANALYZE) on pipes' target tables.

These methods are mixed into `SQLConnector` (see `meerschaum/connectors/sql/_SQLConnector.py`).
`vacuum` reclaims dead-tuple disk space; `analyze` refreshes the planner's statistics.
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import Union, Any, Optional, List, SuccessTuple
from meerschaum.utils.debug import dprint

### Flavors whose tables support reclaiming disk space (VACUUM / OPTIMIZE / REBUILD).
VACUUMABLE_FLAVORS = {
    'timescaledb',
    'timescaledb-ha',
    'postgresql',
    'postgis',
    'citus',
    'mysql',
    'mariadb',
    'mssql',
    'sqlite',
    'geopackage',
}

### Flavors whose `VACUUM` statement must run outside of a transaction block.
_AUTOCOMMIT_VACUUM_FLAVORS = {
    'timescaledb',
    'timescaledb-ha',
    'postgresql',
    'postgis',
    'citus',
    'sqlite',
    'geopackage',
}

### `VACUUM FULL` rewrites the table and reclaims space to the OS (PostgreSQL family only).
_FULL_VACUUM_FLAVORS = {
    'postgresql',
    'postgis',
    'citus',
}

### Flavors whose tables support refreshing planner statistics.
ANALYZABLE_FLAVORS = VACUUMABLE_FLAVORS | {'cockroachdb', 'duckdb'}


def _run_in_autocommit(
    self,
    queries: List[str],
    silent: bool = False,
    debug: bool = False,
) -> bool:
    """
    Execute statements with `AUTOCOMMIT` isolation (i.e. outside any transaction block).

    Required for `VACUUM`, which PostgreSQL and SQLite refuse to run inside a transaction.
    Returns `True` if every statement succeeded.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy', lazy=False)

    success = True
    try:
        connection = self.engine.connect().execution_options(isolation_level='AUTOCOMMIT')
    except Exception as e:
        if not silent:
            warn(f"Failed to open an autocommit connection:\n{e}")
        return False

    try:
        for query in queries:
            if debug:
                dprint(f"[{self}]\n{query}")
            try:
                connection.execute(sqlalchemy.text(query))
            except Exception as e:
                success = False
                msg = f"Encountered error while executing:\n{e}"
                if not silent:
                    warn(msg)
                elif debug:
                    dprint(f"[{self}]\n{msg}")
                break
    finally:
        connection.close()

    return success


def _get_vacuum_queries(
    self,
    pipe: mrsm.Pipe,
    pipe_name: str,
    full: bool = False,
) -> Optional[List[str]]:
    """
    Build the flavor-specific statements that reclaim disk space for a pipe's target table.

    Returns `None` if the flavor does not support vacuuming.
    """
    flavor = self.flavor

    if flavor in ('postgresql', 'postgis', 'citus'):
        options = 'FULL' if (full and flavor in _FULL_VACUUM_FLAVORS) else ''
        return [f"VACUUM {('(' + options + ') ') if options else ''}{pipe_name}".strip()]

    if flavor in ('timescaledb', 'timescaledb-ha'):
        ### `VACUUM` recurses into a hypertable's chunks. `VACUUM FULL` is rejected on compressed
        ### chunks, so the `full` flag is intentionally ignored here.
        return [f"VACUUM {pipe_name}"]

    if flavor in ('mysql', 'mariadb'):
        return [f"OPTIMIZE TABLE {pipe_name}"]

    if flavor == 'mssql':
        return [f"ALTER TABLE {pipe_name} REBUILD"]

    if flavor in ('sqlite', 'geopackage'):
        ### SQLite's `VACUUM` is database-wide; it cannot target a single table.
        return ["VACUUM"]

    return None


def _get_analyze_query(
    self,
    pipe: mrsm.Pipe,
    pipe_name: str,
) -> Optional[str]:
    """
    Build the flavor-specific statement that refreshes a pipe's planner statistics.

    Returns `None` if the flavor does not support analyzing.
    """
    flavor = self.flavor

    if flavor in (
        'timescaledb', 'timescaledb-ha', 'postgresql', 'postgis', 'citus', 'cockroachdb',
        'sqlite', 'geopackage',
    ):
        return f"ANALYZE {pipe_name}"

    if flavor in ('mysql', 'mariadb'):
        return f"ANALYZE TABLE {pipe_name}"

    if flavor == 'mssql':
        return f"UPDATE STATISTICS {pipe_name}"

    if flavor == 'duckdb':
        ### DuckDB's `ANALYZE` refreshes statistics for the whole database.
        return "ANALYZE"

    return None


def vacuum_pipe(
    self,
    pipe: mrsm.Pipe,
    full: bool = False,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Reclaim dead-tuple disk space from a pipe's target table.

    PostgreSQL-family tables run `VACUUM` (optionally `VACUUM FULL`); TimescaleDB hypertables
    recurse into their chunks; MySQL/MariaDB run `OPTIMIZE TABLE`; MSSQL rebuilds the table;
    SQLite vacuums the whole database file.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table to vacuum.

    full: bool, default False
        If `True` (PostgreSQL family only), run `VACUUM FULL`, which rewrites the table and
        returns freed space to the operating system at the cost of an exclusive lock.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` indicating success, including the amount of disk reclaimed.
    """
    from meerschaum.utils.sql import sql_item_name
    from meerschaum.utils.formatting import format_bytes

    if not pipe.exists(debug=debug):
        return False, f"{pipe} does not exist; nothing to vacuum."

    flavor = self.flavor
    if flavor not in VACUUMABLE_FLAVORS:
        return False, f"Vacuuming is not supported for flavor '{flavor}'."

    pipe_name = sql_item_name(pipe.target, flavor, self.get_pipe_schema(pipe))
    queries = self._get_vacuum_queries(pipe, pipe_name, full=full)
    if not queries:
        return False, f"Vacuuming is not supported for flavor '{flavor}'."

    size_before = pipe.get_size(debug=debug)

    try:
        if flavor in _AUTOCOMMIT_VACUUM_FLAVORS:
            success = self._run_in_autocommit(queries, silent=(not debug), debug=debug)
        else:
            success = all(self.exec_queries(
                queries, break_on_error=True, rollback=True, silent=(not debug), debug=debug,
            ))
    except Exception as e:
        return False, f"Failed to vacuum {pipe}:\n{e}"

    if not success:
        return False, f"Failed to vacuum {pipe}."

    pipe._clear_cache_key('_exists', debug=debug)
    size_after = pipe.get_size(debug=debug)

    reclaimed_msg = f"Vacuumed {pipe}."
    if size_before is not None and size_after is not None:
        reclaimed = size_before - size_after
        change_str = f"{format_bytes(size_before)} to {format_bytes(size_after)}"
        if reclaimed > 0:
            reclaimed_msg = f"Reclaimed {format_bytes(reclaimed)} ({change_str})."
        elif reclaimed < 0:
            reclaimed_msg = f"Size grew by {format_bytes(-reclaimed)} ({change_str})."
        else:
            reclaimed_msg = f"Size unchanged ({format_bytes(size_before)})."

    return True, reclaimed_msg


def analyze_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Refresh the database planner's statistics for a pipe's target table.

    This does not reclaim disk space; it helps the query planner choose better plans after
    large syncs. PostgreSQL/SQLite run `ANALYZE`, MySQL/MariaDB run `ANALYZE TABLE`, and MSSQL
    runs `UPDATE STATISTICS`.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table to analyze.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    from meerschaum.utils.sql import sql_item_name

    if not pipe.exists(debug=debug):
        return False, f"{pipe} does not exist; nothing to analyze."

    flavor = self.flavor
    if flavor not in ANALYZABLE_FLAVORS:
        return False, f"Analyzing is not supported for flavor '{flavor}'."

    pipe_name = sql_item_name(pipe.target, flavor, self.get_pipe_schema(pipe))
    query = self._get_analyze_query(pipe, pipe_name)
    if not query:
        return False, f"Analyzing is not supported for flavor '{flavor}'."

    try:
        success = all(self.exec_queries(
            [query], break_on_error=True, rollback=True, silent=(not debug), debug=debug,
        ))
    except Exception as e:
        return False, f"Failed to analyze {pipe}:\n{e}"

    if not success:
        return False, f"Failed to analyze {pipe}."

    return True, f"Analyzed {pipe}."
