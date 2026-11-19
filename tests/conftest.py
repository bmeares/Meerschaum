#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Settings for the whole test suite.

The suite encodes pandas results (Meerschaum 4 parity), so it pins the DataFrame library to
pandas (`MRSM_DATAFRAME=pandas`, which overrides `meerschaum:dataframe` and which every
subprocess and job started from here inherits). Export `MRSM_DATAFRAME=polars` to run it on
Polars instead. Tests of the setting itself (`tests/test_dataframe_library.py`) choose the library
per test, and the console suite (`tests/console`) starts its servers without any inherited
`MRSM_*` variable, so they run on the default (Polars) and prove that the console converts.

A run without `MRSM_ROOT_DIR` gets a fresh temporary root, so the test process never opens (and
never writes to) the developer's real root, e.g. through the one-time DataFrame pin.

`sql:main` is a SQLite file under that root unless the environment already sets `MRSM_SQL_MAIN`:
its default, `localhost:5432`, can be a live database on a developer's machine or a self-hosted
runner, and nothing in the suite needs it to be a network database (the flavor tests use the
connectors of `tests/connectors.py`; `tests/test_upgrade_postgres.py` makes its own). Subprocesses
and xdist workers inherit it; tests that need another `sql:main` strip `MRSM_*` or set their own.
"""

import json
import os
import pathlib
import tempfile

os.environ.setdefault('MRSM_DATAFRAME', 'pandas')

_TEMP_ROOT = None
if not os.environ.get('MRSM_ROOT_DIR'):
    _scratch = pathlib.Path.home() / '.cache' / 'mrsm-pytest-roots'
    _scratch.mkdir(parents=True, exist_ok=True)
    _TEMP_ROOT = tempfile.mkdtemp(prefix='root-', dir=_scratch)
    os.environ['MRSM_ROOT_DIR'] = _TEMP_ROOT

#: Whether this file pinned `sql:main` (the guard test checks the pin only then).
SQL_MAIN_PINNED = 'MRSM_SQL_MAIN' not in os.environ
if SQL_MAIN_PINNED:
    os.environ['MRSM_SQL_MAIN'] = json.dumps({
        'flavor': 'sqlite',
        'database': str(pathlib.Path(os.environ['MRSM_ROOT_DIR']) / 'sqlite' / 'pytest_sql_main.db'),
    })
    os.environ['MRSM_TEST_SQL_MAIN_PINNED'] = '1'


def pytest_unconfigure(config):
    """Remove the temporary root this process created (xdist workers inherit the variable)."""
    if _TEMP_ROOT and not hasattr(config, 'workerinput'):
        import shutil
        shutil.rmtree(_TEMP_ROOT, ignore_errors=True)
