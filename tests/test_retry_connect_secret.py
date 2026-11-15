#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
5.1: `retry_connect()` starts a child Python process to test a connection. The connector's
attributes (its password among them) must never be on that child's command line, where any local
user can read them (`ps`, `/proc/<pid>/cmdline`), nor in its environment.
"""

import pathlib
import subprocess

SECRET = 'retry-connect-s3cret-value'


def test_retry_connect_keeps_the_connector_off_the_command_line(tmp_path, monkeypatch):
    import meerschaum as mrsm
    from meerschaum.connectors.poll import retry_connect

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
