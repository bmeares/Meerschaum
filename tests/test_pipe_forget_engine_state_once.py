#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`pipe.drop()`, `pipe.delete()` and a full `pipe.clear()` forget the pipe's Syncing Engine state
once: a `sql:` or `valkey:` instance forgets it in its own drop, delete and clear, so the façade
does not do it again; other instances (`api:`, plugin instance connectors) leave it to the façade.
"""

import uuid
from types import SimpleNamespace

import pytest
import meerschaum as mrsm
import meerschaum.core.Pipeline._engine as engine_module
from meerschaum.core.Pipe._drop import _forget_engine_state

from tests.connectors import conns


@pytest.fixture
def forgets(monkeypatch):
    calls = []
    monkeypatch.setattr(
        engine_module,
        'forget_engine_state',
        lambda pipe, operation: calls.append(operation),
    )
    return calls


def test_sql_instance_forgets_engine_state_itself(forgets):
    conn = conns['sqlite']
    pipe = mrsm.Pipe(
        'test', 'forget_once_' + uuid.uuid4().hex[:8],
        instance=conn,
        columns={'datetime': 'dt'},
    )
    for _ in range(2):
        success, message = pipe.sync([{'dt': '2024-01-01'}])
        assert success, message
        success, message = pipe.clear()
        assert success, message
        success, message = pipe.drop()
        assert success, message
    success, message = pipe.sync([{'dt': '2024-01-01'}])
    assert success, message
    success, message = pipe.delete()
    assert success, message
    assert forgets == []


@pytest.mark.parametrize('instance_type', ['api', 'plugin_instance'])
def test_other_instances_leave_it_to_the_facade(forgets, instance_type):
    pipe = SimpleNamespace(instance_connector=SimpleNamespace(type=instance_type))
    _forget_engine_state(pipe, 'drop_pipe')
    assert forgets == ['drop_pipe']
