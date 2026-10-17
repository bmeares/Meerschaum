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
from meerschaum.utils.typing import Any, SuccessTuple

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
    return self._rust_method('vacuum_pipe')(pipe, full=full)


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
    return self._rust_method('analyze_pipe')(pipe)
