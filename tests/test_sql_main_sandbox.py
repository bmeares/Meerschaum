#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Guard: the test process never reaches the default `sql:main` (`localhost:5432`), which can be a
live database on a developer's machine or a self-hosted runner (`tests/conftest.py`).
"""

import json
import os
import pathlib

import pytest


def test_sql_main_is_a_sqlite_file_under_the_test_root():
    if os.environ.get('MRSM_TEST_SQL_MAIN_PINNED') != '1':
        pytest.skip('MRSM_SQL_MAIN was set by the environment, not by tests/conftest.py.')
    attributes = json.loads(os.environ['MRSM_SQL_MAIN'])
    assert attributes['flavor'] == 'sqlite'
    root = pathlib.Path(os.environ['MRSM_ROOT_DIR']).resolve()
    assert pathlib.Path(attributes['database']).resolve().is_relative_to(root)

    from meerschaum.config import get_config
    main = get_config('meerschaum', 'connectors', 'sql', 'main')
    assert main['flavor'] == 'sqlite'

    import meerschaum as mrsm
    conn = mrsm.get_connector('sql:main')
    assert conn.flavor == 'sqlite'
    assert pathlib.Path(conn.database).resolve().is_relative_to(root)


def test_no_test_reaches_port_5432_through_sql_main():
    """The resolved URI of `sql:main` names no network host at all."""
    if os.environ.get('MRSM_TEST_SQL_MAIN_PINNED') != '1':
        pytest.skip('MRSM_SQL_MAIN was set by the environment, not by tests/conftest.py.')
    import meerschaum as mrsm
    conn = mrsm.get_connector('sql:main')
    uri = str(getattr(conn, 'URI', '') or '')
    assert uri.startswith('sqlite'), uri
    assert ':5432' not in uri
