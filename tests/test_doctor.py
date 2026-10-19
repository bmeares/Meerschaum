#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`mrsm doctor` through the Python façade (wave 17c): `meerschaum.entry()` and
`meerschaum.actions.actions['doctor']` run the same native dispatcher in-process, on a throwaway
SQLite instance (the sqlite flavor needs no service). The binary-level behavior (shadowing
`meerschaum` copies, unreachable servers, read-only runs) is covered by
`crates/meerschaum-cli/tests/doctor.rs`.

Run with:
    PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \\
        python -m pytest tests/test_doctor.py -p no:cacheprovider
"""

import copy
import json
import uuid

import pytest

import meerschaum as mrsm
import meerschaum.config as mrsm_config
from meerschaum import _core
from meerschaum.config import set_config

pytestmark = pytest.mark.skipif(
    _core.load() is None,
    reason="The `_core` extension is not built (scripts/rust/build_py.sh).",
)

@pytest.fixture
def instance(tmp_path):
    """A SQLite instance defined in the live config (restored afterwards), one label per test."""
    label = f'doctor_py_{uuid.uuid4().hex[:8]}'
    keys = f'sql:{label}'
    original = copy.deepcopy(mrsm_config.config)
    try:
        set_config({
            'meerschaum': {
                'connectors': {'sql': {label: {'flavor': 'sqlite', 'database': str(tmp_path / 'doctor.db')}}},
            },
        })
        pipe = mrsm.Pipe(keys, 'doctor', instance=keys)
        success, message = pipe.register()
        assert success, message
        yield keys
    finally:
        set_config(original)


def _document(capsys) -> dict:
    return json.loads(capsys.readouterr().out)


def test_doctor_is_a_native_action_with_a_docstring():
    from meerschaum.actions import actions

    assert 'doctor' in actions
    assert 'read-only' in (actions['doctor'].__doc__ or '').lower()


def test_entry_prints_the_json_document_in_process(instance, capsys):
    success, message = mrsm.entry(['doctor', '--nopretty', '-i', instance])
    assert success, message
    doc = _document(capsys)
    assert doc['version'] == _core.__version__
    assert doc['summary']['ok'] is True
    by_name = {(c['group'], c['name']): c for c in doc['checks']}
    assert by_name[('instance', 'connection')]['status'] == 'PASS'
    assert by_name[('instance', 'tables')]['status'] == 'PASS'
    assert {c['group'] for c in doc['checks']} >= {
        'install', 'paths', 'config', 'instance', 'drivers', 'extras', 'plugins', 'jobs',
        'stack', 'extensions',
    }
    for check in doc['checks']:
        assert set(check) >= {'group', 'name', 'status', 'detail', 'fix'}


def test_the_action_function_returns_the_tuple(instance, capsys):
    from meerschaum.actions import actions

    success, message = actions['doctor'](nopretty=True, mrsm_instance=instance)
    assert success, message
    assert 'No problems found' in message
    assert _document(capsys)['summary']['fail'] == 0


def test_a_missing_instance_is_a_failed_tuple_with_a_fix(capsys):
    from meerschaum.actions import actions

    success, message = actions['doctor'](nopretty=True, mrsm_instance='sql:w17c_missing_instance')
    assert success is False
    assert 'instance: connection' in message
    doc = _document(capsys)
    connection = next(c for c in doc['checks'] if (c['group'], c['name']) == ('instance', 'connection'))
    assert connection['status'] == 'FAIL'
    assert 'W17C_MISSING_INSTANCE' in connection['fix']
