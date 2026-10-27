#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
rc15 Q9 for Python: when nothing answers at the default `sql:main` as shipped, the connection
error says what to do next, once per process (the CLI adds its own wording per command).
The Rust connector is replaced by a stub that fails like a real first connection.
"""

import pytest

from meerschaum.connectors.sql import _SQLConnector as module

REFUSED = (
    "sql:main: cannot connect to localhost:5432 (connection refused). Is the database running?"
)
SHIPPED = {
    'flavor': 'timescaledb-ha',
    'host': 'localhost',
    'port': 5432,
    'database': 'meerschaum',
    'username': 'mrsm',
    'password': 'mrsm',
}


class _FailingCore:
    """A `_core.SqlConnector` whose every call fails with `message`."""

    def __init__(self, message: str):
        self.message = message

    def get_pipe_data(self, *args, **kwargs):
        raise RuntimeError(self.message)


def _connector(label: str = 'main', message: str = REFUSED, **attributes):
    conn = module.SQLConnector(label, **{**SHIPPED, **attributes})
    conn._core_connector = lambda: _FailingCore(message)
    return conn


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.delenv('MRSM_SQL_MAIN', raising=False)
    monkeypatch.setattr(module, '_instance_hint_shown', False)


def _error(conn) -> str:
    with pytest.raises(RuntimeError) as info:
        conn._rust_method('get_pipe_data')()
    return str(info.value)


def test_the_shipped_main_gets_the_next_step_once():
    conn = _connector()
    first = _error(conn)
    assert first == REFUSED + '\n' + module.DEFAULT_INSTANCE_HINT
    assert 'mrsm stack up -d db' in first and "instance='sql:local'" in first
    assert _error(conn) == REFUSED


@pytest.mark.parametrize(
    'label,message,attributes',
    [
        ('main', REFUSED, {'port': 5439}),
        ('main', REFUSED, {'host': 'db.example.com'}),
        (
            'main',
            "sql:main: cannot connect to localhost:5432 (authentication failed for user 'mrsm'). "
            "Check the connector's credentials.",
            {},
        ),
        ('other', REFUSED.replace('sql:main', 'sql:other'), {}),
    ],
)
def test_other_failures_and_connectors_keep_their_message(label, message, attributes):
    assert _error(_connector(label, message, **attributes)) == message


def test_an_environment_connector_is_configured(monkeypatch):
    monkeypatch.setenv('MRSM_SQL_MAIN', 'postgresql://mrsm:mrsm@localhost:5432/meerschaum')
    assert _error(_connector()) == REFUSED


def test_other_connectors_get_the_method_itself():
    conn = module.SQLConnector('e1_rc15_other', flavor='sqlite', database=':memory:')
    core = _FailingCore(REFUSED)
    conn._core_connector = lambda: core
    assert conn._rust_method('get_pipe_data') == core.get_pipe_data
