#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Python session helpers share the Rust server's instance-backed session store
(`crates/meerschaum-web/src/sessions.rs`, `InstanceSessions`): table `mrsm_sessions` with
`session_id` (primary key), `data` (compact JSON string) and `expires` (Unix seconds).
The tests read and write the table with raw SQL the way the Rust code does.
"""

import json
import time
import sqlite3

from tests.api_host._helpers import run_python, sandbox  # noqa: F401

PRELUDE = """
import json, time
import meerschaum.api as api
from meerschaum.api.dash import sessions as s
"""


def _db(root):
    return sqlite3.connect(str(root / 'instance.db'))


def test_python_session_is_readable_as_rust_reads_it(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, PRELUDE + """
assert api._host_mode
s.set_session('py-sess-1', {'username': 'alice', 'admin': False, 'note': 'x y'})
print(json.dumps({
    'username': s.get_username_from_session('py-sess-1'),
    'active': s.is_session_active('py-sess-1'),
    'other': s.is_session_active('nope'),
    'expires_seconds': s.EXPIRES_SECONDS,
}))
""")
    assert result.returncode == 0, result.stderr[-3000:]
    out = json.loads(result.stdout.strip().splitlines()[-1])
    assert out['username'] == 'alice' and out['active'] is True and out['other'] is False
    assert out['expires_seconds'] == 43200 * 60

    conn = _db(root)
    try:
        rows = conn.execute(
            "SELECT session_id, data, expires FROM mrsm_sessions WHERE session_id = 'py-sess-1'"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 1
    session_id, data, expires = rows[0]
    ### Compact JSON (no spaces after separators), as the Rust store writes it.
    assert data == '{"username":"alice","admin":false,"note":"x y"}'
    assert json.loads(data)['username'] == 'alice'
    ### Expiry is Unix seconds `now + 30 days`.
    assert abs(int(expires) - (time.time() + 43200 * 60)) < 300


def test_rust_written_session_is_readable_in_python(sandbox):
    root, plugins = sandbox
    ### Make sure the table exists with the Python-created schema, then write as Rust does.
    run_python(root, plugins, PRELUDE + "s.set_session('seed', {'username': 'seed'})")
    conn = _db(root)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO mrsm_sessions (session_id, data, expires) VALUES (?, ?, ?)",
            ('rust-sess', '{"username":"bob","admin":true}', int(time.time()) + 600),
        )
        conn.execute(
            "INSERT OR REPLACE INTO mrsm_sessions (session_id, data, expires) VALUES (?, ?, ?)",
            ('rust-expired', '{"username":"carol"}', int(time.time()) - 5),
        )
        conn.commit()
    finally:
        conn.close()
    result = run_python(root, plugins, PRELUDE + """
print(json.dumps({
    'bob': s.get_username_from_session('rust-sess'),
    'bob_admin': s.session_is_admin('rust-sess'),
    'expired': s.get_username_from_session('rust-expired'),
    'expired_active': s.is_session_active('rust-expired'),
}))
""")
    assert result.returncode == 0, result.stderr[-3000:]
    out = json.loads(result.stdout.strip().splitlines()[-1])
    assert out == {'bob': 'bob', 'bob_admin': True, 'expired': None, 'expired_active': False}


def test_update_overwrites_and_delete_removes(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, PRELUDE + """
s.set_session('upd', {'username': 'dave'})
s.update_session('upd', {'admin': True})
first = s.get_session_data('upd')
s.set_session('upd', {'username': 'erin'})
second = s.get_session_data('upd')
s.delete_session('upd')
print(json.dumps({'first': first, 'second': second, 'after': s.get_session_data('upd')}))
""")
    assert result.returncode == 0, result.stderr[-3000:]
    out = json.loads(result.stdout.strip().splitlines()[-1])
    assert out['first'] == {'username': 'dave', 'admin': True}
    assert out['second'] == {'username': 'erin'}
    assert out['after'] is None
    conn = _db(root)
    try:
        count = conn.execute("SELECT COUNT(*) FROM mrsm_sessions WHERE session_id='upd'").fetchone()[0]
    finally:
        conn.close()
    assert count == 0
