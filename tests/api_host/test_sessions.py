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
s.set_session('00000000-0000-4000-8000-00000000a001', {'username': 'alice', 'admin': False, 'note': 'x y'})
print(json.dumps({
    'username': s.get_username_from_session('00000000-0000-4000-8000-00000000a001'),
    'active': s.is_session_active('00000000-0000-4000-8000-00000000a001'),
    'other': s.is_session_active('00000000-0000-4000-8000-00000000a0ff'),
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
            "SELECT session_id, data, expires FROM mrsm_sessions "
            "WHERE session_id = '00000000-0000-4000-8000-00000000a001'"
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
    run_python(
        root, plugins,
        PRELUDE + "s.set_session('00000000-0000-4000-8000-00000000a002', {'username': 'seed'})",
    )
    conn = _db(root)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO mrsm_sessions (session_id, data, expires) VALUES (?, ?, ?)",
            (
                '00000000-0000-4000-8000-00000000a003',
                '{"username":"bob","admin":true}',
                int(time.time()) + 600,
            ),
        )
        conn.execute(
            "INSERT OR REPLACE INTO mrsm_sessions (session_id, data, expires) VALUES (?, ?, ?)",
            ('00000000-0000-4000-8000-00000000a004', '{"username":"carol"}', int(time.time()) - 5),
        )
        conn.commit()
    finally:
        conn.close()
    result = run_python(root, plugins, PRELUDE + """
from meerschaum.core import User
from meerschaum.api import get_api_connector
conn = get_api_connector()
conn.register_user(User('bob', 'bob-password-1', type='admin', instance=conn))
conn.register_user(User('rust-claims', 'claims-password-1', instance=conn))
s.set_session('00000000-0000-4000-8000-00000000a005', {'username': 'rust-claims', 'admin': True})
print(json.dumps({
    'bob': s.get_username_from_session('00000000-0000-4000-8000-00000000a003'),
    'bob_admin': s.session_is_admin('00000000-0000-4000-8000-00000000a003'),
    'expired': s.get_username_from_session('00000000-0000-4000-8000-00000000a004'),
    'expired_active': s.is_session_active('00000000-0000-4000-8000-00000000a004'),
    'claims_admin': s.session_is_admin('00000000-0000-4000-8000-00000000a005'),
}))
""")
    assert result.returncode == 0, result.stderr[-3000:]
    out = json.loads(result.stdout.strip().splitlines()[-1])
    ### Whether a session is an administrator's is read from its user (their type now), never
    ### from a flag the session's data carries.
    assert out == {
        'bob': 'bob', 'bob_admin': True, 'expired': None, 'expired_active': False,
        'claims_admin': False,
    }


def test_update_overwrites_and_delete_removes(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, PRELUDE + """
UPD = '00000000-0000-4000-8000-00000000a005'
s.set_session(UPD, {'username': 'dave'})
s.update_session(UPD, {'admin': True})
first = s.get_session_data(UPD)
s.set_session(UPD, {'username': 'erin'})
second = s.get_session_data(UPD)
s.delete_session(UPD)
print(json.dumps({'first': first, 'second': second, 'after': s.get_session_data(UPD)}))
""")
    assert result.returncode == 0, result.stderr[-3000:]
    out = json.loads(result.stdout.strip().splitlines()[-1])
    assert out['first'] == {'username': 'dave', 'admin': True}
    assert out['second'] == {'username': 'erin'}
    assert out['after'] is None
    conn = _db(root)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM mrsm_sessions "
            "WHERE session_id = '00000000-0000-4000-8000-00000000a005'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert count == 0


def test_host_sessions_use_shared_instance_even_with_valkey_configured(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, PRELUDE + """
def unexpected_cache():
    raise AssertionError('Hosted sessions must use the same store as the Rust terminal')
s.get_cache_connector = unexpected_cache
SHARED = '00000000-0000-4000-8000-00000000a006'
s.set_session(SHARED, {'username': 'alice'})
assert s.get_session_data(SHARED, use_cache=False)['username'] == 'alice'
s.update_session(SHARED, {'admin': True})
assert s.get_session_data(SHARED, use_cache=False)['admin'] is True
s.delete_session(SHARED)
assert s.get_session_data(SHARED, use_cache=False) is None
""")
    assert result.returncode == 0, result.stderr[-3000:]


### What the instance store's `params` filter does not read literally (`'_None'` is `IS NOT NULL`,
### `'_x'` is `!= 'x'`, `'None'`/`'NA'`/`''` are `IS NULL`), and other spellings of a real ID.
NOT_SESSION_IDS = """[
    '_None', '_x', '__None', 'None', 'NA', 'nan', '', None, 'nope', '_' + ADMIN,
    ADMIN.upper(), '{' + ADMIN + '}', ADMIN.replace('-', ''), 'urn:uuid:' + ADMIN, ADMIN + ' ',
    ['_None'], {'session-id': ADMIN}, 7,
]"""


def test_only_uuid_session_ids_reach_the_store(sandbox):
    """
    A session ID from the browser is looked up with a `params` filter, so `'_None'` once named
    the first live session (an admin's) and `delete_session('_None')` signed everyone out: only
    the UUIDs the login mints are session IDs, and nothing else is read, written or deleted.
    """
    root, plugins = sandbox
    result = run_python(root, plugins, PRELUDE + """
from meerschaum.core import User
from meerschaum.api import get_api_connector
conn = get_api_connector()
conn.register_user(User('wild-admin', 'wild-password-1', type='admin', instance=conn))
ADMIN = '00000000-0000-4000-8000-00000000b001'
USER = '00000000-0000-4000-8000-00000000b002'
assert s.set_session(ADMIN, {'username': 'wild-admin'}) is True
assert s.set_session(USER, {'username': 'wild-user'}) is True
values = """ + NOT_SESSION_IDS + """
leaks = {}
for value in values:
    checks = {
        'valid': s.valid_session_id(value),
        'data': s.get_session_data(value),
        'data_uncached': s.get_session_data(value, use_cache=False),
        'active': s.is_session_active(value),
        'username': s.get_username_from_session(value),
        'user': s.get_user_from_session(value),
        'admin': s.session_is_admin(value),
        'authenticated': s.is_session_authenticated(value),
        'state': s.is_state_authenticated({'session-id': value}),
    }
    leaked = {key: repr(val) for key, val in checks.items() if val not in (None, False)}
    if leaked:
        leaks[repr(value)] = leaked
writes = {repr(value): [s.set_session(value, {'username': 'x'}), s.update_session(value, {'admin': True})]
          for value in ('_None', '_x', 'None', None, ADMIN.upper())}
for value in ('_None', '_x', '__None', 'None', '', None, '_' + ADMIN):
    s.delete_session(value)
print(json.dumps({
    'leaks': leaks,
    'writes': writes,
    'valid': [s.valid_session_id(ADMIN), s.valid_session_id(__import__('uuid').UUID(USER))],
    'still_signed_in': [s.get_username_from_session(ADMIN), s.get_username_from_session(USER)],
    'admin': s.session_is_admin(ADMIN),
}))
""")
    assert result.returncode == 0, result.stderr[-3000:]
    out = json.loads(result.stdout.strip().splitlines()[-1])
    assert out['leaks'] == {}, out['leaks']
    assert all(written == [False, False] for written in out['writes'].values()), out['writes']
    assert out['valid'] == [
        '00000000-0000-4000-8000-00000000b001', '00000000-0000-4000-8000-00000000b002',
    ]
    ### No lookup or delete touched the real sessions, and nothing else was written.
    assert out['still_signed_in'] == ['wild-admin', 'wild-user']
    assert out['admin'] is True
    conn = _db(root)
    try:
        rows = sorted(row[0] for row in conn.execute(
            "SELECT session_id FROM mrsm_sessions WHERE session_id LIKE '%b00%' "
            "OR session_id NOT LIKE '________-____-____-____-____________'"
        ))
    finally:
        conn.close()
    assert rows == ['00000000-0000-4000-8000-00000000b001', '00000000-0000-4000-8000-00000000b002']


def test_in_memory_sessions_need_uuid_ids_too(sandbox):
    """Outside the API host (no instance store, no cache connector) the same rule applies."""
    root, plugins = sandbox
    result = run_python(root, plugins, PRELUDE + """
ID = '00000000-0000-4000-8000-00000000c001'
print(json.dumps({
    'host_mode': bool(getattr(api, '_host_mode', False)),
    'custom': s.set_session('custom-id', {'username': 'x'}),
    'custom_read': s.get_session_data('custom-id'),
    'uuid': s.set_session(ID, {'username': 'mem-user'}),
    'uuid_read': s.get_username_from_session(ID),
    'wildcard': s.get_username_from_session('_None'),
}))
""", host_mode=False)
    assert result.returncode == 0, result.stderr[-3000:]
    out = json.loads(result.stdout.strip().splitlines()[-1])
    assert out == {
        'host_mode': False,
        'custom': False,
        'custom_read': None,
        'uuid': True,
        'uuid_read': 'mem-user',
        'wildcard': None,
    }
