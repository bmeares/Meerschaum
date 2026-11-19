#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
5.1: for a connector the Rust core does not implement (a plugin's), `retry_connect()` starts a
child Python process to test the connection. The connector's attributes (its password among them)
must never be on that child's command line, where any local user can read them (`ps`,
`/proc/<pid>/cmdline`), nor in its environment.

The `sql`, `valkey` and `api` connectors are retried in process (`tests/test_retry_connect_native.py`
checks that nothing is spawned and no file is written); here a SQLite connector is sent down the
child-process path on purpose (`_native_target` answers `None`, as for a driver that cannot be
built), because the child can rebuild it from its attributes without a plugin.
"""

import pathlib
import subprocess

import pytest

SECRET = 'retry-connect-s3cret-value'


@pytest.mark.parametrize('dill_importable', [True, False])
def test_retry_connect_keeps_the_connector_off_the_command_line(
    tmp_path, monkeypatch, dill_importable,
):
    import sys
    import meerschaum as mrsm
    if not dill_importable:
        ### The base wheel has no runtime dependencies: `retry_connect` must not need `dill`.
        monkeypatch.setitem(sys.modules, 'dill', None)
    import meerschaum.connectors.poll as poll
    from meerschaum.connectors.poll import retry_connect
    ### The core implements `sql`: take the child-process path the other connectors take.
    monkeypatch.setattr(poll, '_native_target', lambda connector, debug=False: None)

    conn = mrsm.get_connector(
        'sql',
        'tpr_retry',
        flavor='sqlite',
        database=str(tmp_path / 'retry.db'),
        password=SECRET,
    )
    assert conn.meta.get('password') == SECRET

    seen = []
    real_call = subprocess.call

    def recording_call(cmd_list, *args, **kwargs):
        seen.append({
            'argv': list(cmd_list),
            'env': dict(kwargs.get('env') or {}),
        })
        return real_call(cmd_list, *args, **kwargs)

    monkeypatch.setattr(subprocess, 'call', recording_call)
    assert retry_connect(conn, max_retries=1, retry_wait=0, warn=False) is True
    assert seen, 'no child was started'
    for call in seen:
        assert not any(SECRET in word for word in call['argv']), call['argv']
        assert not any(SECRET in str(value) for value in call['env'].values())
    ### Nothing that carried the attributes is left behind.
    import tempfile
    leftovers = [
        path for path in pathlib.Path(tempfile.gettempdir()).glob('mrsm-retry-*')
    ]
    assert not leftovers, leftovers
