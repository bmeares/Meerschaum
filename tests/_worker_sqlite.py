#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A SQLite instance of its own per test module and `pytest -n` worker, for tests that run the
Syncing Engine on SQLite.

Every sqlite-flavor test writes the shared `sql:test_sqlite` file. SQLite allows one writer at a
time (and with the rollback journal, which v4 and v5 both keep, readers block a writer), so under
`pytest -n 8` an engine sync could wait past the busy timeout (5 s, the same as Python's
`sqlite3`) behind another worker's write and fail with `database is locked` (code 5). A separate
file changes nothing about how SQLite behaves, only which tests share a writer lock.
"""

import os

import meerschaum as mrsm


def worker_sqlite(name: str):
    """
    Return `sql:<name>_<worker>`, a SQLite file under the test root's data directory. Subprocesses
    reach it with `MRSM_SQL_<NAME>_<WORKER>=sqlite:///<conn.database>`.
    """
    from meerschaum.config.paths import ROOT_DIR_PATH
    label = f"{name}_{os.environ.get('PYTEST_XDIST_WORKER', 'main')}"
    path = ROOT_DIR_PATH / 'data' / f'{label}.db'
    path.parent.mkdir(parents=True, exist_ok=True)
    return mrsm.get_connector('sql', label, flavor='sqlite', database=path.as_posix())
