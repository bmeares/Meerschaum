#! /usr/bin/env python3
# vim:fenc=utf-8

"""
rc15 wave 2, the access model through an `api:` instance (decisions AC2, AC3, AC4, AC5, AC12,
AC13): the Python API reaches teams, a pipe's access and locks, users' roles and permissions
through a real `mrsm start api` (a SQLite instance behind it), and the server decides who may
see and change what.
"""

from __future__ import annotations

import random
import socket
import uuid

import pytest

import meerschaum as mrsm
from meerschaum.core import User

pytestmark = pytest.mark.skipif(
    not __import__('meerschaum._core', fromlist=['x']).AVAILABLE,
    reason="meerschaum._core is not built",
)


def _free_port() -> int:
    """A free loopback port in 19000-19999 (the console tests own 18400-18999)."""
    ports = list(range(19000, 20000))
    random.shuffle(ports)
    for port in ports:
        with socket.socket() as probe:
            try:
                probe.bind(('127.0.0.1', port))
            except OSError:
                continue
        return port
    raise RuntimeError("No free port for the test server.")


@pytest.fixture(scope='module')
def server():
    from tests._binary import binary_or_skip
    binary_or_skip()
    from tests.console.conftest import Server
    instance = Server('p3-access', native=False)
    instance.port = _free_port()
    instance.base_url = f"http://127.0.0.1:{instance.port}"
    try:
        instance.start()
    except Exception as e:
        instance.stop()
        pytest.skip(f"cannot start a loopback API server: {e}")
    yield instance
    instance.stop()


def _conn(server, username: str, password: str):
    label = 'p3' + uuid.uuid4().hex[:8]
    return mrsm.get_connector(
        'api', label, host='127.0.0.1', port=server.port, protocol='http',
        username=username, password=password,
    )


### A pipe the caller cannot see answers like a missing one (`404`), which the façade reports.
@pytest.mark.filterwarnings('ignore:.*Failed to load attributes')
def test_teams_access_locks_and_roles_through_an_api_instance(server):
    from tests.console.conftest import USERNAME, PASSWORD
    admin = _conn(server, USERNAME, PASSWORD)
    tag = uuid.uuid4().hex[:6]
    viewer, editor, other = f"pv{tag}", f"pe{tag}", f"po{tag}"
    for name, attributes in ((viewer, {'role': 'viewer'}), (editor, {}), (other, {})):
        success, msg = admin.register_user(User(
            name, 'pw-' + name, email=f"{name}@example.com", attributes=attributes,
            instance=admin,
        ))
        assert success, msg
    vi = _conn(server, viewer, 'pw-' + viewer)
    ed = _conn(server, editor, 'pw-' + editor)
    ot = _conn(server, other, 'pw-' + other)

    ### Teams: the administrator creates them; members are users of the instance.
    data = mrsm.Team(f"Data{tag}", instance=admin, members=[viewer, editor])
    success, msg = data.register()
    assert success, msg
    ops = mrsm.Team(f"Ops{tag}", instance=admin, members=[other])
    assert ops.register()[0]
    names = [team.name for team in mrsm.get_teams(admin)]
    assert names[0] == 'Everyone' and data.name in names and ops.name in names
    success, msg = mrsm.Team(f"Nope{tag}", instance=ed).register()
    assert not success and 'administrators' in msg, msg
    assert dict(mrsm.Team(data.name, instance=ed).roles) == {viewer: 'member', editor: 'member'}

    ### A pipe registered through the API is its creator's, in their primary team.
    data_key, ops_key = f"data{tag}", f"ops{tag}"
    success, msg = mrsm.Pipe('p3src', data_key, instance=ed, columns={'datetime': 'ts'}).register()
    assert success, msg
    success, msg = mrsm.Pipe('p3src', ops_key, instance=ot, columns={'datetime': 'ts'}).register()
    assert success, msg
    stored = mrsm.Pipe('p3src', data_key, instance=admin)
    assert (stored.owner, stored.team, stored.visibility) == (editor, data.name, 'team')

    def visible(conn) -> set:
        return {
            pipe.metric_key
            for pipe in mrsm.get_pipes('p3src', instance=conn, as_list=True)
        }

    assert visible(ed) == {data_key}
    assert visible(vi) == {data_key}
    assert visible(ot) == {ops_key}
    assert {data_key, ops_key} <= visible(admin)

    ### Locks: the owner (or a manager, or an administrator) locks; a lock refuses everyone.
    mine = mrsm.Pipe('p3src', data_key, instance=ed)
    success, msg = mine.lock('sync')
    assert success, msg
    assert mrsm.Pipe('p3src', data_key, instance=admin).locks == ['sync']
    success, msg = mrsm.Pipe('p3src', data_key, instance=vi).unlock()
    assert not success, msg
    success, msg = mine.sync([{'ts': '2026-01-01'}])
    assert not success and 'locked' in msg, msg
    success, msg = mine.unlock()
    assert success, msg
    assert mrsm.Pipe('p3src', data_key, instance=admin).locks == []

    ### Access: the owner makes the pipe private; the other members no longer see it.
    success, msg = mine.set_access(visibility='private')
    assert success, msg
    assert visible(vi) == set()
    assert visible(ed) == {data_key}
    ### Nobody changes the access of a pipe they cannot see: it is not there for them (the
    ### change stays in memory, as for any pipe that is not registered).
    mrsm.Pipe('p3src', ops_key, instance=ed).set_access(visibility='everyone')
    assert mrsm.Pipe('p3src', ops_key, instance=admin).visibility == 'team'
    assert visible(ed) == {data_key}
    access = admin.get_pipe_access(mrsm.Pipe('p3src', data_key, instance=admin))
    assert access['access']['visibility'] == 'private'
    assert access['may'] == {'edit': True, 'manage_access': True, 'lock': True}

    ### Roles and permissions: the server's answer.
    assert User(editor, instance=admin).role == 'editor'
    assert 'pipes:register' in User(editor, instance=admin).permissions
    assert User(viewer, instance=admin).role == 'viewer'
    success, msg = User(editor, instance=ed).set_role('operator')
    assert not success, msg
    success, msg = User(editor, instance=admin).set_role('viewer')
    assert success, msg
    assert User(editor, instance=admin).role == 'viewer'
    success, msg = mrsm.Pipe('p3src', data_key, instance=ed).lock('drop')
    assert not success, msg
    assert dict(User(editor, instance=admin).teams).get(data.name) == 'member'

    ### Emails: your own, and everyone's for administrators.
    emails = {row['username']: row['email'] for row in vi.get_users_permissions()}
    assert emails[viewer] == f"{viewer}@example.com"
    assert emails[editor] is None and emails[other] is None
    emails = {row['username']: row['email'] for row in admin.get_users_permissions()}
    assert emails[editor] == f"{editor}@example.com"

    ### Renaming a team renames it in its pipes; deleting one gives them to Everyone.
    assert admin.get_team_pipes_count(data.name) == 1
    success, msg = mrsm.Team(data.name, instance=admin).rename(f"Lab{tag}")
    assert success, msg
    assert mrsm.Pipe('p3src', data_key, instance=admin).team == f"Lab{tag}"
    success, msg = mrsm.Team(f"Lab{tag}", instance=admin).delete()
    assert success, msg
    assert mrsm.Pipe('p3src', data_key, instance=admin).team == 'Everyone'
    assert ops.delete()[0]

    for conn in (ed, ot, admin):
        for key in (data_key, ops_key):
            pipe = mrsm.Pipe('p3src', key, instance=conn)
            if pipe.locks:
                pipe.unlock()
    for key in (data_key, ops_key):
        mrsm.Pipe('p3src', key, instance=admin).delete()
