#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The access model through the Python API (rc15 wave 2: AC3, AC4, AC5, AC12, FX1), on
throwaway SQLite instances (and Valkey when the `valkey` flavor is selected):

- the rules of `meerschaum.core.access` (the core's: permissions, roles, teams, visibility,
  locks);
- locks refuse every write they name, through every Python call, whatever the allowances
  (`allow_protected()`), and unlocking restores them; a `parameters` lock allows changing the
  locks and syncing;
- a pipe's access (`set_access()`, `owner`, `team`, `visibility`, `edit_mode`);
- teams (`mrsm.Team`, `mrsm.get_teams()`): CRUD, members and managers, renames and deletions
  carried into the pipes' access, a deleted user leaving their teams and pipes;
- `user.role`, `user.permissions`, `user.teams`, `user.primary_team`;
- FX1: users are looked up by their exact username (`_alice` never reaches `alice`'s row), on
  SQL instances and on the default (pipe-backed) store of a custom instance connector;
- a Pipeline run skips a sync-locked member as `locked` without failing.
"""

from __future__ import annotations

from tests._ports import service_port

import os
import uuid

import pytest

import meerschaum as mrsm
from meerschaum.connectors import allow_protected
from meerschaum.core import User
from meerschaum.core.access import (
    ALL_PERMISSIONS,
    EVERYONE,
    Principal,
    access_for_new_pipe,
    can_edit,
    can_lock,
    can_manage_access,
    can_manage_team,
    can_administer_teams,
    can_see,
    can_see_email,
    check_access_change,
    check_lock,
    effective_permissions,
    filter_visible_pipes,
    get_principal,
    is_lock_refusal,
    lock_refusal,
    locks_with,
    locks_without,
    parse_access,
    parse_locks,
    primary_team,
    resolve_default_role,
    role_permissions,
    token_permissions,
    user_permissions,
    user_role,
    valid_team_name,
)

VALKEY_SELECTED = 'valkey' in os.environ.get('MRSM_TEST_FLAVORS', 'sqlite').split(',')


def _label(prefix: str = 'p1') -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def conn(tmp_path):
    return mrsm.get_connector(
        'sql', _label(),
        flavor='sqlite', database=str(tmp_path / 'p1.db'),
    )


def _pipe(conn, metric: str = 'temps', **kw) -> mrsm.Pipe:
    return mrsm.Pipe(
        'p1', metric,
        instance=conn,
        columns={'datetime': 'ts', 'id': 'id'},
        **kw
    )


def _docs(*days: int):
    return [{'ts': f'2024-01-{day:02d}', 'id': 1, 'v': float(day)} for day in days]


def _register_user(conn, username: str, password: str = 'password123', **kw) -> User:
    user = User(username, password, email=f'{username.strip("_")}@example.com', instance=conn, **kw)
    success, msg = conn.register_user(user)
    assert success, msg
    return User(username, instance=conn)


def _refused(result, lock: str) -> str:
    success, msg = result
    assert not success, msg
    assert is_lock_refusal(msg), msg
    assert f"it is locked ({lock}" in msg, msg
    assert 'mrsm unlock pipes' in msg, msg
    return msg


# ------------------------------------------------------------------------------------------
# The rules
# ------------------------------------------------------------------------------------------

def test_permissions_and_roles_follow_the_core():
    assert ALL_PERMISSIONS[:3] == ('pipes:read', 'pipes:write', 'pipes:drop')
    for new in ('pipes:register', 'pipes:define_sql', 'webterm:use'):
        assert new in ALL_PERMISSIONS
    viewer = set(role_permissions('viewer'))
    editor = set(role_permissions('editor'))
    operator = set(role_permissions('operator'))
    assert viewer < editor < operator < set(role_permissions('admin'))
    assert 'pipes:write' not in viewer
    assert {'pipes:register', 'pipes:define_sql', 'sql:read', 'jobs:execute'} <= editor
    assert 'actions:execute' not in editor and 'webterm:use' not in editor
    assert {'actions:execute', 'webterm:use', 'jobs:stop', 'jobs:delete'} <= operator
    assert 'users:register' not in operator

    assert resolve_default_role() == 'editor'
    assert resolve_default_role(None, True) == 'operator'
    assert resolve_default_role('viewer', True) == 'viewer'
    assert resolve_default_role('nonsense') == 'viewer'

    assert set(effective_permissions('admin')) == set(ALL_PERMISSIONS)
    assert set(effective_permissions(None, None, 'viewer')) == viewer
    ### A scope list written before rc15 keeps what it allowed.
    legacy = set(user_permissions(None, {'scopes': ['pipes:read', 'pipes:write', 'actions:execute']}, 'viewer'))
    assert {'pipes:register', 'pipes:define_sql', 'webterm:use'} <= legacy
    assert set(user_permissions(None, {'scopes': ['*']}, 'viewer')) == set(ALL_PERMISSIONS)
    assert set(user_permissions(None, {'role': 'operator'}, 'viewer')) == operator
    assert user_role(None, {'scopes': ['pipes:read']}, 'editor') == 'custom'
    assert user_role(None, {'scopes': sorted(viewer)}, 'editor') == 'viewer'
    assert user_role('admin', {}, 'viewer') == 'admin'
    assert effective_permissions(None, [], 'operator') == []
    assert user_permissions(None, {'role': 'operator', 'scopes': []}, 'operator') == []
    assert user_role(None, {'scopes': []}, 'operator') == 'custom'

    ### A token never holds more than its user.
    assert set(token_permissions(['*'], sorted(viewer))) == viewer
    assert token_permissions(['pipes:write'], sorted(viewer)) == []


def test_visibility_edit_and_team_rules():
    owner = Principal('owen', permissions=['pipes:read'], teams=[('ops', 'member')])
    member = Principal('mo', permissions=['pipes:read'], teams=[('Ops', 'member')])
    manager = Principal('mia', permissions=['pipes:read'], teams=[('ops', 'manager')])
    outsider = Principal('otto', permissions=['pipes:read'], teams=[('data', 'member')])
    admin = Principal('ada', is_admin=True)

    default = {}
    team = {'owner': 'owen', 'team': 'ops', 'visibility': 'team'}
    private = {'owner': 'owen', 'team': 'ops', 'visibility': 'private'}
    everyone = {'owner': 'owen', 'team': 'ops', 'visibility': 'everyone', 'edit': 'owner'}

    for who in (owner, member, manager, outsider, admin):
        assert can_see(who, default) and can_edit(who, default)
    assert [can_see(p, team) for p in (owner, member, manager, outsider, admin)] == [
        True, True, True, False, True,
    ]
    assert [can_see(p, private) for p in (owner, member, manager, outsider, admin)] == [
        True, False, True, False, True,
    ]
    assert [can_see(p, everyone) for p in (owner, member, manager, outsider, admin)] == [
        True, True, True, True, True,
    ]
    assert [can_edit(p, everyone) for p in (owner, member, manager, outsider, admin)] == [
        True, False, True, False, True,
    ]
    assert [can_manage_access(p, team) for p in (owner, member, manager, outsider, admin)] == [
        True, False, True, False, True,
    ]
    assert can_lock(manager, team) and not can_lock(member, team)

    ### Emails: admins, managers of a team the user is in, and the user themself.
    assert can_see_email(admin, [('data', 'member')])
    assert can_see_email(manager, [('ops', 'member')])
    assert not can_see_email(member, [('ops', 'member')])
    assert can_see_email(member, [], target_username='mo')

    assert can_manage_team(manager, 'OPS') and not can_manage_team(member, 'ops')
    assert can_administer_teams(admin) and not can_administer_teams(manager)

    ### Access changes and new pipes.
    assert check_access_change(owner, team, {**team, 'visibility': 'private'}) is None
    assert check_access_change(member, team, {**team, 'visibility': 'private'})
    assert check_access_change(owner, team, {**team, 'team': 'data'})
    assert check_access_change(owner, team, {**team, 'owner': None}) is None
    assert check_access_change(owner, team, {**team, 'owner': 'mo'})
    assert check_access_change(manager, team, {**team, 'owner': 'mo'}) is None
    new = access_for_new_pipe(member, {'owner': 'someone-else', 'visibility': 'private'}, 'ops')
    assert new == {'owner': 'mo', 'team': 'ops', 'visibility': 'private', 'edit': 'team'}
    assert access_for_new_pipe(admin, {'owner': 'mo'}, EVERYONE)['owner'] == 'mo'
    assert primary_team({}, [('Everyone', 'member'), ('ops', 'member')]) == 'ops'
    assert primary_team({'primary_team': 'data'}, [('ops', 'member'), ('data', 'member')]) == 'data'
    assert primary_team({'primary_team': 'gone'}, [('Everyone', 'member')]) == EVERYONE

    assert valid_team_name('ops-team.1')[0]
    assert not valid_team_name('bad name')[0]
    assert not valid_team_name('everyone')[0]

    with pytest.raises(ValueError):
        parse_access({'visibility': 'nobody'})
    with pytest.raises(ValueError):
        parse_access({'teams': 'ops'})


def test_lock_rules():
    assert parse_locks('drop, sync') == ['sync', 'drop']
    assert parse_locks([]) == ['all']
    with pytest.raises(ValueError):
        parse_locks('dorp')
    assert locks_with(['sync'], ['all']) == ['all']
    assert locks_without(['all'], ['sync']) == ['parameters', 'clear', 'drop', 'delete']
    assert locks_without(['sync', 'drop'], ['all']) == []
    assert check_lock(['drop'], 'sync') is None
    assert check_lock(['drop'], 'rebuild') == 'it is locked (drop)'
    assert check_lock(['clear'], 'replace') == 'it is locked (clear)'
    assert check_lock(['drop'], 'schema') == 'it is locked (drop)'
    assert check_lock(['all'], 'edit') == 'it is locked (all)'
    pipe = mrsm.Pipe('sql:main', 'weather', instance='sql:main')
    msg = lock_refusal(pipe, ['sync'], 'sync')
    assert msg.startswith("Cannot sync Pipe('sql:main', 'weather'): it is locked (sync).")
    assert '`mrsm unlock pipes -c sql:main -m weather --locks sync`' in msg
    assert is_lock_refusal(msg) and not is_lock_refusal('Failed to sync.')


# ------------------------------------------------------------------------------------------
# Locks on an instance
# ------------------------------------------------------------------------------------------

def test_locks_refuse_every_write_they_name(conn):
    pipe = _pipe(conn)
    assert pipe.register()[0]
    assert pipe.sync(_docs(1, 2))[0]
    assert pipe.locks == []

    success, msg = pipe.lock()
    assert success, msg
    assert pipe.locks == ['all']
    assert _pipe(conn).locks == ['all'], "the lock is registered"

    with allow_protected():
        _refused(pipe.sync(_docs(3)), 'all')
        _refused(pipe.sync(_docs(3), replace=True, begin='2024-01-03', end='2024-01-04'), 'all')
        _refused(pipe.clear(), 'all')
        _refused(pipe.drop(), 'all')
        _refused(pipe.delete(), 'all')
        _refused(pipe.deduplicate(), 'all')
        _refused(pipe.verify(), 'all')
        pipe.parameters['tags'] = ['edited']
        _refused(pipe.edit(), 'all')
        _refused(pipe.update_parameters({'tags': ['x']}, persist=True), 'all')
        _refused(pipe.set_access(visibility='private'), 'all')
    assert pipe.get_rowcount() == 2
    assert pipe.get_id() is not None

    ### One kind at a time.
    pipe = _pipe(conn)
    assert pipe.unlock('sync')[0]
    assert pipe.locks == ['parameters', 'clear', 'drop', 'delete']
    assert pipe.sync(_docs(3))[0]
    _refused(pipe.sync(_docs(3), replace=True, begin='2024-01-03', end='2024-01-04'), 'clear')
    _refused(pipe.clear(begin='2024-01-03'), 'clear')
    _refused(pipe.drop(), 'drop')
    _refused(pipe.delete(drop=False), 'delete')

    assert pipe.unlock()[0]
    assert pipe.lock('drop')[0]
    pipe = _pipe(conn, dtypes={'extra': 'int'})
    _refused(pipe.verify_schema(apply=True), 'drop')
    _refused(pipe.drop_indices(), 'drop')
    ### Deleting drops the table first.
    _refused(pipe.delete(), 'drop')
    assert pipe.clear(begin='2024-01-03')[0]
    assert pipe.get_rowcount() == 2

    assert pipe.unlock()[0]
    assert pipe.lock('delete')[0]
    _refused(mrsm.Pipe('p1', 'temps', instance=conn).delete(), 'delete')
    assert pipe.unlock()[0]
    assert pipe.drop()[0]
    assert pipe.delete()[0]
    assert mrsm.Pipe('p1', 'temps', instance=conn).get_id() is None


def test_a_parameters_lock_allows_lock_changes_and_syncs(conn):
    pipe = _pipe(conn)
    assert pipe.register()[0]
    assert pipe.lock('parameters')[0]

    edited = _pipe(conn)
    edited.parameters['tags'] = ['nope']
    _refused(edited.edit(), 'parameters')
    assert _pipe(conn).tags in (None, [])

    ### Changing the locks is the one edit allowed.
    assert edited.lock('clear')[0]
    assert _pipe(conn).locks == ['parameters', 'clear']

    ### A sync writes; what it learns (a new JSON column's dtype) is not recorded.
    docs = [{'ts': '2024-01-01', 'id': 1, 'meta': {'a': 1}}]
    success, msg = _pipe(conn).sync(docs)
    assert success, msg
    assert _pipe(conn).get_rowcount() == 1
    assert 'meta' not in (_pipe(conn).parameters.get('dtypes', None) or {})
    assert _pipe(conn).locks == ['parameters', 'clear']
    assert pipe.unlock()[0]
    assert _pipe(conn).locks == []


def test_locks_of_an_unregistered_pipe_are_saved_with_it(conn):
    pipe = _pipe(conn, metric='later')
    success, msg = pipe.lock('drop', 'delete')
    assert success and 'saved when it is registered' in msg, msg
    success, msg = pipe.set_access(team='ops', visibility='private')
    assert success, msg
    assert pipe.register()[0]
    fresh = mrsm.Pipe('p1', 'later', instance=conn)
    assert fresh.locks == ['drop', 'delete']
    assert fresh.access == {'owner': None, 'team': 'ops', 'visibility': 'private', 'edit': 'team'}
    _refused(fresh.drop(), 'drop')
    assert fresh.unlock()[0]
    assert fresh.drop()[0]
    assert fresh.delete()[0]


# ------------------------------------------------------------------------------------------
# Access, teams and users
# ------------------------------------------------------------------------------------------

def test_pipe_access_properties(conn):
    pipe = _pipe(conn)
    assert pipe.register()[0]
    assert pipe.access == {'owner': None, 'team': None, 'visibility': 'team', 'edit': 'team'}
    assert (pipe.owner, pipe.team, pipe.visibility, pipe.edit_mode) == (None, EVERYONE, 'team', 'team')

    success, msg = pipe.set_access(team='ops', visibility='private', edit='owner', owner='alice')
    assert success, msg
    fresh = _pipe(conn)
    assert (fresh.owner, fresh.team, fresh.visibility, fresh.edit_mode) == ('alice', 'ops', 'private', 'owner')

    ### Arguments not given keep their values; `None` clears.
    assert fresh.set_access(owner=None)[0]
    assert _pipe(conn).access == {'owner': None, 'team': 'ops', 'visibility': 'private', 'edit': 'owner'}
    assert fresh.set_access(team=None, visibility='everyone')[0]
    assert _pipe(conn).team == EVERYONE

    success, msg = fresh.set_access(visibility='nobody')
    assert not success and 'visibility' in msg, msg

    ### Rules over pipes.
    viewer = Principal('vic', permissions=['pipes:read'], teams=[('data', 'member')])
    assert fresh.set_access(team='ops', visibility='team')[0]
    assert filter_visible_pipes(viewer, [_pipe(conn)]) == []
    assert can_see(Principal('ops-member', teams=[('ops', 'member')]), _pipe(conn))


def test_teams_crud_and_renames(conn):
    alice = _register_user(conn, 'alice')
    bob = _register_user(conn, 'bob')
    mia = _register_user(conn, 'mia', type='admin')

    teams = mrsm.get_teams(conn)
    assert [team.name for team in teams] == [EVERYONE]
    assert mrsm.Team('Everyone', instance=conn).exists()
    assert set(mrsm.Team('Everyone', instance=conn).members) == {'alice', 'bob', 'mia'}

    ops = mrsm.Team('ops', instance=conn, members=['bob'], managers=['alice'])
    success, msg = ops.register()
    assert success, msg
    assert ops.id
    assert not mrsm.Team('OPS', instance=conn).register()[0], "names are unique without case"
    assert not mrsm.Team('bad name', instance=conn).register()[0]
    assert not mrsm.Team('Everyone', instance=conn).register()[0]
    success, msg = mrsm.Team('strangers', instance=conn, members=['nobody']).register()
    assert not success and 'not registered' in msg, msg

    found = mrsm.Team('Ops', instance=conn)
    assert found.exists()
    assert found.name == 'ops', "the stored spelling wins"
    assert sorted(found.members) == ['alice', 'bob']
    assert found.managers == ['alice']
    assert [team.name for team in mrsm.get_teams(conn)] == [EVERYONE, 'ops']

    assert found.add_members('mia')[0]
    assert sorted(mrsm.Team('ops', instance=conn).members) == ['alice', 'bob', 'mia']
    assert found.remove_members('mia')[0]
    assert not found.remove_members('nobody')[0]
    assert found.add_managers('bob')[0]
    assert sorted(mrsm.Team('ops', instance=conn).managers) == ['alice', 'bob']

    assert alice.teams == [(EVERYONE, 'member'), ('ops', 'manager')]
    assert alice.primary_team == 'ops'
    assert mia.teams == [(EVERYONE, 'member')]
    assert mia.primary_team == EVERYONE

    ### Everyone's record may name managers; nobody leaves Everyone.
    everyone = mrsm.Team('Everyone', instance=conn)
    assert everyone.add_managers('bob')[0]
    assert User('bob', instance=conn).teams[0] == (EVERYONE, 'manager')
    assert everyone.remove_members('bob')[0]
    assert User('bob', instance=conn).teams[0] == (EVERYONE, 'member')
    assert not everyone.delete()[0]
    assert not everyone.rename('all')[0]

    ### A pipe of the team, locked against parameter edits: renames still reach it.
    pipe = _pipe(conn)
    assert pipe.register()[0]
    assert pipe.set_access(team='ops', visibility='private', owner='bob')[0]
    assert pipe.lock('parameters')[0]
    assert conn.get_team_pipes_count('ops') == 1

    success, msg = found.rename('ops2')
    assert success and '1 pipe updated' in msg, msg
    assert found.name == 'ops2'
    assert not mrsm.Team('ops', instance=conn).exists()
    assert _pipe(conn).team == 'ops2'
    assert _pipe(conn).locks == ['parameters']
    assert User('alice', instance=conn).primary_team == 'ops2'

    ### Deleting a user: they leave the team and their pipe keeps its team, without an owner.
    assert conn.delete_user(User('bob', instance=conn))[0]
    assert 'bob' not in mrsm.Team('ops2', instance=conn).members
    assert _pipe(conn).owner is None
    assert _pipe(conn).team == 'ops2'

    ### Deleting the team: its pipe moves to Everyone.
    success, msg = mrsm.Team('OPS2', instance=conn).delete()
    assert success and '1 pipe moved to Everyone' in msg, msg
    assert [team.name for team in mrsm.get_teams(conn)] == [EVERYONE]
    assert _pipe(conn).team == EVERYONE
    assert _pipe(conn).visibility == 'private'
    assert User('alice', instance=conn).primary_team == EVERYONE


def test_user_roles_and_permissions(conn):
    alice = _register_user(conn, 'alice')
    admin = _register_user(conn, 'ada', type='admin')
    assert alice.role == 'editor'
    assert set(alice.permissions) == set(role_permissions('editor'))
    assert admin.role == 'admin'
    assert set(admin.permissions) == set(ALL_PERMISSIONS)

    success, msg = alice.set_role('viewer')
    assert success, msg
    alice = User('alice', instance=conn)
    assert alice.role == 'viewer'
    assert set(alice.permissions) == set(role_permissions('viewer'))
    assert not alice.set_role('admin')[0]
    assert not admin.set_role('viewer')[0]

    assert alice.set_permissions(['pipes:read', 'logs:read'])[0]
    alice = User('alice', instance=conn)
    assert alice.role == 'custom'
    assert alice.permissions == ['pipes:read', 'logs:read']
    assert not alice.set_permissions(['pipes:fly'])[0]

    ### Only the attributes changed: the password still works.
    from meerschaum.core.User._User import verify_password
    assert verify_password('password123', conn.get_user_password_hash(User('alice', instance=conn)))

    principal = get_principal('alice', instance=conn)
    assert principal.username == 'alice' and not principal.is_admin
    assert principal.permissions == ['pipes:read', 'logs:read']
    assert get_principal('ada', instance=conn).is_admin

    # Clearing a custom grid must revoke rights on the stored user, including later reads.
    assert alice.set_permissions([])[0]
    fresh = User('alice', instance=conn)
    assert fresh.role == 'custom'
    assert fresh.permissions == []
    assert fresh.get_scopes(refresh=True) == []
    assert get_principal('alice', instance=conn).permissions == []
    for permissions in (['pipes:write'], ['actions:execute']):
        assert alice.set_permissions(permissions)[0]
        fresh = User('alice', instance=conn)
        assert fresh.permissions == permissions
        assert fresh.role == 'custom'


def _check_exact_user_lookups(conn) -> None:
    """FX1: `_alice` is not `alice`, and an unknown user has nothing."""
    from meerschaum.core.User._User import verify_password
    alice = _register_user(conn, 'alice', password='alice-pass', type='admin', attributes={'who': 'alice'})
    under = _register_user(conn, '_alice', password='under-pass', attributes={'who': 'under'})

    alice_id = conn.get_user_id(alice)
    under_id = conn.get_user_id(under)
    assert alice_id is not None and under_id is not None
    assert str(alice_id) != str(under_id)
    assert conn.get_username(under_id) == '_alice'
    assert conn.get_user_type(under) != 'admin'
    assert conn.get_user_type(alice) == 'admin'
    assert verify_password('under-pass', conn.get_user_password_hash(under))
    assert not verify_password('alice-pass', conn.get_user_password_hash(under))
    assert conn.get_user_attributes(under)['who'] == 'under'
    assert conn.get_user_attributes(alice)['who'] == 'alice'

    nobody = User('_nobody', instance=conn)
    assert conn.get_user_id(nobody) is None
    assert conn.get_user_type(nobody) is None
    assert conn.get_user_attributes(nobody) is None
    assert conn.get_user_password_hash(nobody) is None
    assert not conn.edit_user(User('_nobody', attributes={'who': 'mallory'}, instance=conn))[0]
    conn.delete_user(nobody)
    assert conn.get_user_id(User('alice', instance=conn)) is not None
    assert conn.get_user_id(User('_alice', instance=conn)) is not None

    assert conn.delete_user(User('_alice', instance=conn))[0]
    assert conn.get_user_id(User('_alice', instance=conn)) is None
    assert conn.get_user_id(User('alice', instance=conn)) is not None
    assert conn.get_user_attributes(User('alice', instance=conn))['who'] == 'alice'


def test_users_are_looked_up_by_their_exact_username(conn):
    _check_exact_user_lookups(conn)


def test_a_custom_instance_looks_users_up_exactly(tmp_path):
    """The default users store of a custom instance connector (`InstanceConnector`'s)."""
    from meerschaum.connectors.instance._users import _exact_row, _is_null

    class FakeUsersPipe:
        """A users pipe whose `params` filter reads a leading `_` as a negation (v4's grammar)."""

        rows = [
            {'user_id': 'id-alice', 'username': 'alice', 'user_type': 'admin'},
            {'user_id': 'id-under', 'username': '_alice', 'user_type': None},
        ]

        def get_data(self, columns, params=None, debug=False):
            import pandas as pd
            rows = self.rows
            for key, value in (params or {}).items():
                if str(value).startswith('_'):
                    rows = [r for r in rows if r[key] != value[1:]]
                else:
                    rows = [r for r in rows if r[key] == value]
            return pd.DataFrame(rows)[list(columns)] if rows else pd.DataFrame(columns=columns)

    users = FakeUsersPipe()
    assert _exact_row(users, 'username', '_alice', ['user_id'])['user_id'] == 'id-under'
    assert _exact_row(users, 'username', 'alice', ['user_id'])['user_id'] == 'id-alice'
    assert _exact_row(users, 'username', '_bob', ['user_id']) is None
    assert _exact_row(users, 'user_id', None, ['username']) is None
    assert _is_null(float('nan')) and _is_null(None) and not _is_null('x')


@pytest.fixture
def custom_instance():
    """
    A `@make_connector` instance (`w15lmem`, `tests/plugins/w15l_custom.py`): its methods run in
    Python here and in the worker the core drives.
    """
    from meerschaum.plugins import _plugins_import_errors, reload_plugins
    from meerschaum.utils.venv import init_venv
    names = ['w15l_custom', 'w15l2_custom']
    for name in names:
        init_venv(name, verify=False)
        _plugins_import_errors.pop(name, None)
    reload_plugins(names)
    conn = mrsm.get_connector('w15lmem', _label('p1mem'))
    assert conn is not None, "the `w15l_custom` test plugin is not importable"
    return conn


def test_locks_on_a_custom_instance(custom_instance):
    pipe = mrsm.Pipe('p1', 'custom', instance=custom_instance, columns={'datetime': 'ts'})
    assert pipe.register()[0]
    assert pipe.lock('sync', 'clear', 'drop')[0]
    assert mrsm.Pipe('p1', 'custom', instance=custom_instance).locks == ['sync', 'clear', 'drop']
    _refused(pipe.sync([{'ts': '2024-01-01', 'v': 1}]), 'sync')
    _refused(pipe.clear(), 'clear')
    _refused(pipe.drop(), 'drop')
    _refused(pipe.delete(), 'drop')
    pipe.parameters['tags'] = ['edited']
    assert pipe.edit()[0], "no `parameters` lock"
    assert pipe.lock('parameters')[0]
    edited = mrsm.Pipe('p1', 'custom', instance=custom_instance)
    edited.parameters['tags'] = ['again']
    _refused(edited.edit(), 'parameters')
    assert pipe.unlock()[0]
    assert mrsm.Pipe('p1', 'custom', instance=custom_instance).locks == []
    assert pipe.sync([{'ts': '2024-01-01', 'v': 1}])[0]
    assert pipe.drop()[0]
    assert pipe.delete()[0]


@pytest.mark.skipif(not VALKEY_SELECTED, reason="the `valkey` flavor is not selected")
def test_valkey_teams_locks_and_users():
    conn = mrsm.get_connector('valkey', _label('p1vk'), uri=f"redis://localhost:{service_port('valkey')}/12")
    prefix = uuid.uuid4().hex[:6]
    usernames = [f'p1{prefix}a', f'_p1{prefix}a']
    team_name = f'p1-{prefix}'
    for username in usernames:
        conn.delete_user(User(username, instance=conn))
    try:
        alice = _register_user(conn, usernames[0])
        under = _register_user(conn, usernames[1])
        assert str(conn.get_user_id(alice)) != str(conn.get_user_id(under))

        team = mrsm.Team(team_name, instance=conn, members=[usernames[0]])
        assert team.register()[0]
        assert mrsm.Team(team_name.upper(), instance=conn).members == [usernames[0]]

        pipe = mrsm.Pipe('p1', f'vk_{prefix}', instance=conn, columns={'datetime': 'ts', 'id': 'id'})
        assert pipe.register()[0]
        assert pipe.set_access(team=team_name, owner=usernames[0])[0]
        assert pipe.lock('sync')[0]
        _refused(pipe.sync(_docs(1)), 'sync')
        assert mrsm.Team(team_name, instance=conn).rename(team_name + 'x')[0]
        assert mrsm.Pipe('p1', f'vk_{prefix}', instance=conn).team == team_name + 'x'
        assert mrsm.Team(team_name + 'x', instance=conn).delete()[0]
        assert mrsm.Pipe('p1', f'vk_{prefix}', instance=conn).team == EVERYONE
        assert pipe.unlock()[0]
        assert pipe.delete()[0]
    finally:
        for username in usernames:
            conn.delete_user(User(username, instance=conn))
        for name in (team_name, team_name + 'x'):
            mrsm.Team(name, instance=conn).delete()


# ------------------------------------------------------------------------------------------
# The Syncing Engine
# ------------------------------------------------------------------------------------------

def test_a_pipeline_run_skips_a_locked_member(conn):
    conn.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
    conn.exec("INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0)")
    keys = str(conn)
    raw = mrsm.Pipe(
        keys, 'raw', instance=conn,
        parameters={
            'sql': 'SELECT ts, id, v FROM src',
            'columns': {'datetime': 'ts', 'id': 'id'},
            'dtypes': {'ts': 'datetime', 'id': 'int', 'v': 'float'},
        },
    )
    clean = mrsm.Pipe(
        keys, 'clean', instance=conn,
        parameters={
            'sql': f"SELECT ts, id, v * 2 AS v FROM {{{{ Pipe('{keys}', 'raw', instance='{keys}') }}}}",
            'columns': {'datetime': 'ts', 'id': 'id'},
            'dtypes': {'ts': 'datetime', 'id': 'int', 'v': 'float'},
        },
    )
    assert raw.register()[0] and clean.register()[0]
    run = clean.sync(parents=True)
    assert run.success, run.message
    assert raw.get_rowcount() == 1

    conn.exec("INSERT INTO src VALUES ('2026-01-01 01:00:00', 1, 2.0)")
    assert raw.lock('sync')[0]
    run = mrsm.Pipe(keys, 'clean', instance=conn).sync(parents=True)
    assert run.success, run.message
    assert 'locked' in run.message, run.message
    nodes = {node['pipe_ref']: node for node in (run.record or {}).get('nodes', [])}
    raw_node = next(node for ref, node in nodes.items() if ref.endswith('|raw|None'))
    assert raw_node['status'] == 'locked', raw_node
    assert 'it is locked (sync)' in raw_node['reason']
    assert any('it is locked (sync)' in warning for warning in run.warnings), run.warnings
    assert mrsm.Pipe(keys, 'raw', instance=conn).get_rowcount() == 1
