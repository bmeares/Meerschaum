#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The web console's access model (rc15 wave 2: AC1, AC2, AC4, AC5, AC7, AC11, AC13), in process on
the sandbox's SQLite instance, as real users with real console sessions (`SIGN_IN`):

- each role sees the pages, buttons and pipes it may (Viewer, Editor, Operator, a team's manager,
  a team's member, another team's member, an administrator), and a hidden pipe's keys never reach
  a page or a store;
- Access… and Locks… are checked again on the server side of their callbacks; a lock refuses
  Sync and says how to unlock;
- New pipe gives the pipe the viewer's access (owner, primary team) and refuses a team the
  viewer is not in;
- Users & Teams: the Email column and addresses follow AC13; teams are an administrator's to
  create, rename and delete, and their managers' to fill;
- a new instance's login page creates the first account as an administrator, once;
- Pipelines and runs show only what the viewer may see (hidden members are a count).

`tests/console/test_access_console.py` drives the same rules in Chromium.
"""

import json

from tests.api_host._helpers import SIGN_IN, run_python, sandbox  # noqa: F401


def _json(result) -> dict:
    assert result.returncode == 0, result.stderr[-6000:]
    line = [line for line in result.stdout.splitlines() if line.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


#: The users and pipes of these tests (registered once per sandbox; every test may run alone).
SEED = SIGN_IN + '''
import json
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from dash._callback_context import context_value
from dash._utils import AttributeDict
from dash.exceptions import PreventUpdate
conn = get_api_connector()
INSTANCE = str(conn)
### An administrator, one user per role preset, the manager and a member of `data`, and a member
### of `ops` (all but the administrator and the presets have the default role, Editor).
SESSIONS = {
    'admin': sign_in('ac-admin', user_type='admin'),
    'viewer': sign_in('ac-viewer', role='viewer'),
    'editor': sign_in('ac-editor'),
    'operator': sign_in('ac-operator', role='operator'),
    'manager': sign_in('ac-manager', teams={'data': 'manager'}),
    'member': sign_in('ac-member', teams={'data': 'member'}),
    'other': sign_in('ac-other', teams={'ops': 'member'}),
}

def make(mk, access=None, **params):
    pipe = mrsm.Pipe('ac:src', mk, instance=conn)
    if pipe.get_id() is None:
        if access is not None:
            params['access'] = access
        assert mrsm.Pipe('ac:src', mk, instance=conn, parameters=params).register()[0]
    return mrsm.Pipe('ac:src', mk, instance=conn)

PIPES = {
    'every': make('ac_every', columns={'datetime': 'ts'}),
    'data': make('ac_data', {'team': 'data'}),
    'secret': make('ac_secret', {'team': 'data', 'visibility': 'private', 'owner': 'ac-member'}),
    'ops': make('ac_ops', {'team': 'ops'}),
    'shared': make('ac_shared', {'team': 'ops', 'visibility': 'everyone'}),
}

def trigger(prop_id, value=1):
    context_value.set(AttributeDict(**{'triggered_inputs': [{'prop_id': prop_id, 'value': value}]}))

from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash import access, pipes_table, pipe_detail, components
'''


def test_each_viewer_sees_the_pipes_and_pages_its_role_and_teams_allow(sandbox):
    """
    Roles set permissions (the default role is Editor); teams and visibility decide which pipes
    each viewer sees in the Pipes table, its filters, the Overview's counts and a pipe's page; the
    navigation leaves out what a viewer may not use.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from unittest import mock
import plotly.utils
from meerschaum.api.dash import overview
from meerschaum.api.dash.callbacks import pipes as callbacks
import meerschaum.api.dash.webterm as webterm

out = {'permissions': {}, 'rows': {}, 'options': {}, 'overview': {}, 'nav': {}, 'leaks': {},
       'team_column': {}}
for who, session in SESSIONS.items():
    viewer = access.get_viewer(session)
    out['permissions'][who] = sorted(viewer.permissions)
    trigger('pipes-location.search', '?connector_keys=ac%3Asrc')
    meta = {'instance': INSTANCE, 'global': INSTANCE}
    store, _ = callbacks.load_pipes_rows('?connector_keys=ac%3Asrc', 0, '/dash/pipes', meta, session, None)
    out['rows'][who] = sorted(row['mk'] for row in store['rows'])
    out['team_column'][who] = store['teams']
    options = pipes_table.filter_options(conn, viewer=viewer)
    out['options'][who] = sorted(options['metric_keys'])
    out['overview'][who] = overview.pipes_summary(conn, viewer=viewer)['total']
    with mock.patch.object(webterm, 'webterm_enabled', lambda: True):
        with components.console_nav_context('/dash', session['session-id']):
            nav = json.dumps(components.build_pages_navbar(), cls=plotly.utils.PlotlyJSONEncoder)
    out['nav'][who] = [section for section in ('pipes', 'jobs', 'connectors', 'plugins', 'terminal')
                       if f'console-nav-{section}' in nav]
    ### A hidden pipe's keys are nowhere in what the page gets.
    text = json.dumps(store, default=str) + json.dumps(options)
    out['leaks'][who] = sorted(name for name, pipe in PIPES.items()
                               if pipe.metric_key in text and pipe.metric_key not in out['rows'][who])
print('JSON:' + json.dumps({**out, 'roles': {role: sorted(access.role_permissions(role)) for role in ('viewer', 'editor', 'operator')}}))
'''))
    roles = out['roles']
    perms = out['permissions']
    assert perms['viewer'] == roles['viewer']
    ### No role of their own: the default role, Editor.
    assert perms['editor'] == roles['editor'] == perms['member'] == perms['manager']
    assert perms['operator'] == roles['operator']
    assert 'actions:execute' not in perms['editor'] and 'webterm:use' not in perms['editor']
    assert 'actions:execute' in perms['operator'] and 'webterm:use' in perms['operator']
    assert {'pipes:register', 'pipes:define_sql', 'pipes:write'} <= set(perms['editor'])
    assert 'pipes:write' not in perms['viewer'] and 'pipes:read' in perms['viewer']

    rows = out['rows']
    everyone = ['ac_every', 'ac_shared']
    assert rows['admin'] == ['ac_data', 'ac_every', 'ac_ops', 'ac_secret', 'ac_shared']
    assert rows['viewer'] == everyone and rows['editor'] == everyone and rows['operator'] == everyone
    ### A team's member sees its team's pipes but not a private one they do not own; its manager does.
    assert rows['member'] == ['ac_data', 'ac_every', 'ac_secret', 'ac_shared']
    assert rows['manager'] == ['ac_data', 'ac_every', 'ac_secret', 'ac_shared']
    assert rows['other'] == ['ac_every', 'ac_ops', 'ac_shared']
    ### The filters and the Overview count only what each viewer sees.
    for who, metrics in rows.items():
        assert set(metrics) <= set(out['options'][who]), who
        assert not (set(out['options'][who]) & {'ac_data', 'ac_secret', 'ac_ops'} - set(metrics)), who
        assert out['leaks'][who] == [], who
    assert out['overview']['other'] < out['overview']['admin']
    assert out['overview']['admin'] - out['overview']['other'] == 2
    ### The Team column shows once a viewer has a team or sees a team's pipe.
    assert out['team_column']['member'] is True and out['team_column']['other'] is True

    nav = out['nav']
    ### The Terminal (with the terminal on) is for `webterm:use`: Operators and administrators.
    assert 'terminal' in nav['admin'] and 'terminal' in nav['operator']
    assert all('terminal' not in nav[who] for who in ('viewer', 'editor', 'member', 'manager', 'other'))
    assert all({'pipes', 'jobs', 'connectors', 'plugins'} <= set(nav[who]) for who in nav)


def test_a_pipe_page_shows_what_the_viewer_may_do_and_hides_another_teams_pipe(sandbox):
    """
    A pipe's page: its owner, team and visibility badges and lock icon; the actions of the
    viewer's permissions and edit rights; Access… and Locks… for its owner, its team's managers
    and administrators; a pipe the viewer may not see reads as not registered (no actions).
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.api.dash.callbacks import pipes as callbacks

ACTIONS = ('pipe-action-sync', 'pipe-action-verify', 'pipe-edit-toggle', 'pipe-manage-copy',
           'pipe-manage-access', 'pipe-manage-locks', 'pipe-manage-clear', 'pipe-manage-drop',
           'pipe-manage-delete')

def ids(component):
    found = set()
    def walk(node):
        if isinstance(node, (list, tuple)):
            for child in node:
                walk(child)
            return
        if node is None or isinstance(node, (str, int, float)):
            return
        node_id = getattr(node, 'id', None)
        if isinstance(node_id, str):
            found.add(node_id)
        for attr in ('children', 'label'):
            walk(getattr(node, attr, None))
    walk(component)
    return found

def page(who, name):
    pipe = PIPES[name]
    viewer = access.get_viewer(SESSIONS[who])
    layout = pipe_detail.layout(pipe.connector_keys, pipe.metric_key, pipe.location_key, INSTANCE,
                                INSTANCE, viewer=viewer)
    return layout

out = {'actions': {}, 'badges': {}, 'tab': {}}
for who, name in (('admin', 'secret'), ('viewer', 'every'), ('editor', 'every'), ('member', 'secret'),
                  ('member', 'data'), ('manager', 'secret'), ('other', 'data')):
    layout = page(who, name)
    found = ids(layout)
    out['actions'][who + ':' + name] = sorted(i for i in found if i in ACTIONS)
    out['badges'][who + ':' + name] = sorted(i for i in found if i.startswith('pipe-detail-') and
                                             i in ('pipe-detail-owner', 'pipe-detail-team',
                                                   'pipe-detail-visibility', 'pipe-detail-locks'))
    trigger('pipe-detail-tabs.active_tab', 'overview')
    result = callbacks.render_pipe_detail_tab('overview', False, 0, pipe_detail.meta_of(PIPES[name]), SESSIONS[who])
    out['tab'][who + ':' + name] = result['registered']
print('JSON:' + json.dumps(out))
'''))
    actions = out['actions']
    assert actions['admin:secret'] == sorted([
        'pipe-action-sync', 'pipe-action-verify', 'pipe-edit-toggle', 'pipe-manage-copy',
        'pipe-manage-access', 'pipe-manage-locks', 'pipe-manage-clear', 'pipe-manage-drop',
        'pipe-manage-delete',
    ])
    ### A Viewer reads: no actions at all (not disabled: absent).
    assert actions['viewer:every'] == []
    ### An Editor syncs, edits and copies an Everyone pipe; Clear, Drop and Delete need
    ### `pipes:delete` / `pipes:drop`, and Access and Locks its owner, team managers or admins.
    assert actions['editor:every'] == sorted([
        'pipe-action-sync', 'pipe-action-verify', 'pipe-edit-toggle', 'pipe-manage-copy',
    ])
    ### The owner (a member) manages the access and locks of their private pipe.
    assert {'pipe-manage-access', 'pipe-manage-locks'} <= set(actions['member:secret'])
    assert not {'pipe-manage-access', 'pipe-manage-locks'} & set(actions['member:data'])
    assert {'pipe-manage-access', 'pipe-manage-locks'} <= set(actions['manager:secret'])
    ### Another team's pipe: no actions, and its tabs read as not registered.
    assert actions['other:data'] == [] and out['tab']['other:data'] is False
    assert out['tab']['member:data'] is True and out['tab']['admin:secret'] is True
    assert out['badges']['admin:secret'] == ['pipe-detail-owner', 'pipe-detail-team', 'pipe-detail-visibility']
    assert out['badges']['member:data'] == ['pipe-detail-team']
    assert out['badges']['viewer:every'] == []


def test_access_and_locks_are_checked_on_the_server_and_a_lock_says_how_to_unlock(sandbox):
    """
    Access… and Locks… open only for the pipe's owner, team managers and administrators, and
    their Save refuses anyone else whatever the browser sends; a `sync` lock refuses Sync (whoever
    asks, the administrator included) with the pipe, the lock and Manage > Locks.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.api.dash.callbacks import pipes as callbacks
pipe = make('ac_dialogs', {'team': 'data'})
meta = pipe_detail.meta_of(pipe)
out = {}

def call(fn, *args):
    try:
        return fn(*args)
    except PreventUpdate:
        return 'prevented'

trigger('pipe-manage-access.n_clicks')
out['open_member'] = call(callbacks.open_pipe_access, 1, None, meta, SESSIONS['member'])
opened = call(callbacks.open_pipe_access, 1, None, meta, SESSIONS['manager'])
out['open_manager'] = [opened[0], opened[4], opened[5], [o['value'] for o in opened[3]]]
trigger('pipe-access-save.n_clicks')
refused = callbacks.save_pipe_access(1, 'ac-member', 'data', 'private', 'owner', meta, SESSIONS['member'])
out['save_member'] = str(refused[1])
saved = callbacks.save_pipe_access(1, 'ac-member', 'data', 'private', 'owner', meta, SESSIONS['manager'])
out['save_manager'] = [saved[0], str(saved[3])]
out['access'] = mrsm.Pipe('ac:src', 'ac_dialogs', instance=conn).get_parameters(apply_symlinks=False, refresh=True).get('access')
### A manager may not give the pipe to a team they are not in.
moved = callbacks.save_pipe_access(1, 'ac-member', 'ops', 'team', 'team', meta, SESSIONS['manager'])
out['move_manager'] = str(moved[1])

### Locks…: the owner locks it against syncs; another member of the team sees it but may not
### (the browser asks anyway); a viewer outside the team does not even see it.
pipe = make('ac_lockable', {'team': 'data', 'owner': 'ac-member'})
meta = pipe_detail.meta_of(pipe)
member2 = sign_in('ac-member2', teams={'data': 'member'})
trigger('pipe-manage-locks.n_clicks')
out['locks_open_member2'] = call(callbacks.open_pipe_locks, 1, None, meta, member2)
out['locks_open_outsider'] = call(callbacks.open_pipe_locks, 1, None, meta, SESSIONS['editor'])
trigger('pipe-locks-save.n_clicks')
out['locks_save_member2'] = str(callbacks.save_pipe_locks(1, ['sync'], meta, member2)[1])
locked = callbacks.save_pipe_locks(1, ['sync'], meta, SESSIONS['member'])
out['locks_save_owner'] = [locked[0], str(locked[3])]
out['locks'] = access.pipe_locks(mrsm.Pipe('ac:src', 'ac_lockable', instance=conn))
### Sync is refused while it is locked, for everyone.
started = []
import meerschaum.api.dash.execute as execute
execute.run_action = lambda *a, **k: started.append(a) or {'success': True, 'job': 'x', 'stream': '/x', 'msg': ''}
trigger('pipe-action-sync.n_clicks')
for who in ('member', 'admin'):
    store, alert, _ = callbacks.run_pipe_action(1, None, None, None, None, None, None, meta, SESSIONS[who])
    out['sync_' + who] = [store is callbacks.dash.no_update, str(alert)]
out['started'] = len(started)
### The team's manager may unlock it.
unlocked = callbacks.save_pipe_locks(1, [], meta, SESSIONS['manager'])
out['unlocked'] = [unlocked[0], access.pipe_locks(mrsm.Pipe('ac:src', 'ac_lockable', instance=conn))]
store, alert, _ = callbacks.run_pipe_action(1, None, None, None, None, None, None, meta, SESSIONS['member'])
out['sync_after'] = store is not callbacks.dash.no_update
print('JSON:' + json.dumps(out, default=str))
'''))
    assert out['open_member'] == 'prevented'
    assert out['open_manager'][0] is True and out['open_manager'][1] == 'data'
    assert out['open_manager'][2] == 'team' and 'data' in out['open_manager'][3]
    assert "Only the pipe's owner, its team's managers and administrators" in out['save_member']
    assert out['save_manager'][0] is False and 'Saved the access' in out['save_manager'][1]
    assert out['access']['owner'] == 'ac-member' and out['access']['visibility'] == 'private'
    assert out['access']['edit'] == 'owner' and out['access']['team'] == 'data'
    assert 'ops' in out['move_manager'] or 'team' in out['move_manager'].lower()
    assert out['locks_open_member2'] == 'prevented' and out['locks_open_outsider'] == 'prevented'
    assert 'lock or unlock' in out['locks_save_member2']
    assert out['locks_save_owner'][0] is False and out['locks'] == ['sync']
    for who in ('member', 'admin'):
        refused, message = out['sync_' + who]
        assert refused is True, who
        assert 'locked' in message and 'Manage > Locks' in message and 'unlock pipes' in message, message
    assert out['started'] == 0
    assert out['unlocked'] == [False, []]
    assert out['sync_after'] is True


def test_new_pipe_belongs_to_its_viewer_and_their_team(sandbox):
    """
    New pipe: the Team select starts at the viewer's primary team and offers only their teams; the
    registered pipe is owned by the viewer; a team they are not in is refused; a connector
    reserved for other teams (`teams:`) is not offered and refused.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
import os
from meerschaum.api.dash import new_pipe
from meerschaum.api.dash.callbacks import pipes as callbacks
out = {}
member = access.get_viewer(SESSIONS['member'])
options, value = new_pipe.team_options(member)
out['team_options'] = [[o['value'] for o in options], value]
out['team_options_editor'] = [o['value'] for o in new_pipe.team_options(access.get_viewer(SESSIONS['editor']))[0]]

def register(who, mk, team, visibility='team', connector='ac:src'):
    trigger('new-pipe-register.n_clicks')
    return callbacks.register_new_pipe(1, None, connector, mk, None, INSTANCE, '', '{}', None, None,
                                       SESSIONS[who], team, visibility)

result = register('member', 'ac_new_member', 'data', 'private')
out['registered'] = [result[0], result[4]]
out['access'] = mrsm.Pipe('ac:src', 'ac_new_member', instance=conn).get_parameters(apply_symlinks=False, refresh=True).get('access')
refused = register('member', 'ac_new_refused', 'ops')
out['refused'] = str(refused[0])
out['refused_exists'] = mrsm.Pipe('ac:src', 'ac_new_refused', instance=conn).get_id() is not None
viewer_refused = register('viewer', 'ac_new_viewer', 'Everyone')
out['viewer'] = str(viewer_refused[0])
### A connector only the `ops` team may use.
from meerschaum.connectors import get_connector
get_connector('sql', 'acopsonly', flavor='sqlite', database=os.path.join(os.getcwd(), 'acops.db'), teams=['ops'])
out['usable'] = [
    'sql:acopsonly' in new_pipe.usable_connectors(['sql:acopsonly', 'ac:src'], member),
    'sql:acopsonly' in new_pipe.usable_connectors(['sql:acopsonly', 'ac:src'], access.get_viewer(SESSIONS['other'])),
]
out['teams_refused'] = str(register('member', 'ac_new_teams', 'data', connector='sql:acopsonly')[0])
print('JSON:' + json.dumps(out, default=str))
'''))
    assert out['team_options'] == [['Everyone', 'data'], 'data']
    assert out['team_options_editor'] == ['Everyone']
    assert out['registered'][0] == [] and out['registered'][1].startswith('/dash/pipes/')
    assert out['access']['owner'] == 'ac-member' and out['access']['team'] == 'data'
    assert out['access']['visibility'] == 'private'
    assert 'ops' in out['refused'] and out['refused_exists'] is False
    assert 'may not register' in out['viewer']
    assert out['usable'] == [False, True]
    assert 'acopsonly' in out['teams_refused'] and 'data' in out['teams_refused']


def test_users_and_teams_follow_the_email_rule_and_team_roles(sandbox):
    """
    Users & Teams (AC11, AC13): an administrator sees every user with Type, Role and Email; a
    team's manager sees its members with their emails (and nobody else's); a member sees the
    members of their teams without emails; only administrators create, rename and delete teams;
    managers change their team's members; deleting a team says what moves to Everyone.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.core import User
from meerschaum.api.dash import users
from meerschaum.api.dash.callbacks import users as cu
for name, email in (('ac-member', 'member@example.com'), ('ac-other', 'other@example.com'),
                    ('ac-manager', 'manager@example.com')):
    conn.edit_user(User(name, '', email=email, type='', instance=conn))

import plotly.utils
def listing(who):
    info = users.viewer(SESSIONS[who])
    table, _, total = users.build_users_listing('', 1, viewer_info=info)
    return json.dumps(table, cls=plotly.utils.PlotlyJSONEncoder)

out = {}
for who in ('admin', 'manager', 'member', 'other'):
    text = listing(who)
    out[who] = {
        'users': sorted(name for name in ('ac-admin', 'ac-member', 'ac-other', 'ac-manager', 'ac-viewer') if name in text),
        'emails': sorted(email for email in ('member@example.com', 'other@example.com', 'manager@example.com') if email in text),
        'email_column': '"Email"' in text,
        'role_column': '"Role"' in text,
    }

def call(fn, *args):
    try:
        return fn(*args)
    except PreventUpdate:
        return 'prevented'

### Teams: an administrator creates one; a manager may not; a manager fills their own team.
trigger('teams-new-submit.n_clicks')
out['create_admin'] = call(cu.submit_new_team, 1, 'ac-crew', 0, SESSIONS['admin'])[0]
out['create_manager'] = str(call(cu.submit_new_team, 1, 'ac-crew2', 0, SESSIONS['manager'])[1])
out['teams'] = access.team_names(conn)
trigger('teams-new-submit.n_clicks')
out['create_everyone'] = str(call(cu.submit_new_team, 1, 'everyone', 0, SESSIONS['admin'])[1])
### Members: the manager of `data` adds a member there, not to `ac-crew`.
import dash
from types import SimpleNamespace
target = {'team': 'data', 'action': 'members'}
context_value.set(AttributeDict(**{'triggered_inputs': [{'prop_id': 'team-add-button.n_clicks', 'value': 1}]}))
added = call(cu.edit_team_members, 1, [], [], 'ac-viewer', 'member', target, 0, SESSIONS['manager'])
out['add_by_manager'] = added != 'prevented' and not added[1]
out['data_members'] = mrsm.Team('data', instance=conn).roles
crew = {'team': 'ac-crew', 'action': 'members'}
out['add_elsewhere'] = str(call(cu.edit_team_members, 1, [], [], 'ac-viewer', 'member', crew, 0, SESSIONS['manager'])[1])
### Delete says what moves to Everyone.
out['consequence'] = cu.delete_consequence('data')
print('JSON:' + json.dumps(out, default=str))
'''))
    admin = out['admin']
    assert set(admin['users']) == {'ac-admin', 'ac-member', 'ac-other', 'ac-manager', 'ac-viewer'}
    assert admin['email_column'] and admin['role_column']
    assert set(admin['emails']) == {'member@example.com', 'other@example.com', 'manager@example.com'}
    manager = out['manager']
    ### The members of their team (and themself), with the emails of their team's members only.
    assert 'ac-member' in manager['users'] and 'ac-other' not in manager['users']
    assert manager['email_column'] and 'member@example.com' in manager['emails']
    assert 'other@example.com' not in manager['emails'] and not manager['role_column']
    member = out['member']
    assert 'ac-manager' in member['users'] and 'ac-other' not in member['users']
    assert member['email_column'] is False and member['emails'] == []
    assert out['other']['emails'] == [] and 'ac-member' not in out['other']['users']
    assert out['create_admin'] is False and 'ac-crew' in out['teams']
    assert 'Only administrators' in out['create_manager'] and 'ac-crew2' not in out['teams']
    assert 'built-in team' in out['create_everyone']
    assert out['add_by_manager'] is True and out['data_members'].get('ac-viewer') == 'member'
    assert 'may not change' in out['add_elsewhere']
    assert 'move to Everyone' in out['consequence'] and 'pipe' in out['consequence']


def test_a_new_instance_creates_its_first_admin_from_the_login_page_once(sandbox):
    """
    AC1: while the API's user store has no users, the login page is Create admin account, which
    makes the first account an administrator and signs it in; then the page is the login form,
    the form refuses a second first admin, and registration stays closed (no Create account).
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, extra_env={
        'MRSM_SQL_ACFIRST': 'sqlite:///' + str(root / 'acfirst.db'),
        'MRSM_API_HOST_CONFIG': json.dumps({'mrsm_instance': 'sql:acfirst'}),
    }, code='''
import json
from meerschaum.api import get_api_connector
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.pages import login, register
from meerschaum.api.dash import registration
conn = get_api_connector()
assert str(conn) == 'sql:acfirst', conn

import plotly.utils
def ids(component):
    text = json.dumps(component, cls=plotly.utils.PlotlyJSONEncoder)
    return {name for name in ('first-admin-button', 'login-button', 'login-register-link', 'first-admin-title') if name in text}

out = {'before': sorted(ids(login.layout('/dash/login', 'http://x/dash/login'))), 'first_run': registration.first_run()}
out['short'] = registration.create_first_admin('first', 'ab', 'ab')
out['created'] = registration.create_first_admin('ac-first', 'correct-horse-9', 'correct-horse-9', 'first@example.com')
out['type'] = conn.get_user_type(__import__('meerschaum').core.User('ac-first', instance=conn))
out['again'] = registration.create_first_admin('ac-second', 'correct-horse-9', 'correct-horse-9')
out['after'] = sorted(ids(login.layout('/dash/login', 'http://x/dash/login')))
out['registration_open'] = registration.self_registration_open()
out['register_account'] = registration.register_account('ac-visitor', 'correct-horse-9')[0]
print('JSON:' + json.dumps(out, default=str))
'''))
    assert out['first_run'] is True
    assert out['before'] == ['first-admin-button', 'first-admin-title']
    assert out['short'][0] is False
    assert out['created'][0] is True, out['created'] and out['type'] == 'admin'
    assert out['again'][0] is False and 'already has users' in out['again'][1]
    assert out['after'] == ['login-button']
    assert out['registration_open'] is False
    assert out['register_account'][0] is False and 'closed' in out['register_account'][1]


def test_pipelines_and_runs_show_hidden_members_as_a_count(sandbox):
    """
    AC7: a saved Pipeline with a member the viewer may not see lists that member only as a count,
    may not be run by them, and is hidden altogether when its registry pipe is another team's; the
    engine's views leave out or anonymize the pipes they may not see.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.core.Pipeline._saved import save_pipeline, registration as registry_pipe
from meerschaum.api.dash import engine
refs = [access._ref(INSTANCE, 'ac:src', 'ac_every', None), access._ref(INSTANCE, 'ac:src', 'ac_ops', None)]
if 'ac-mixed' not in access.visible_pipelines(conn, viewer=access.SYSTEM):
    save_pipeline('ac-mixed', refs, INSTANCE)
if 'ac-ops-only' not in access.visible_pipelines(conn, viewer=access.SYSTEM):
    save_pipeline('ac-ops-only', [refs[1]], INSTANCE)
    pipe = registry_pipe('ac-ops-only', INSTANCE)
    assert pipe.set_access(team='ops')[0]
child = make('ac_ops_child', {'team': 'ops'})
parent = make('ac_parent', children=["Pipe('ac:src', 'ac_ops_child')"])
out = {}
for who in ('member', 'other', 'admin'):
    viewer = access.get_viewer(SESSIONS[who])
    pipelines = access.visible_pipelines(conn, viewer=viewer)
    out[who] = {
        name: [definition.get('targets'), definition.get('hidden_members', 0)]
        for name, definition in pipelines.items() if name.startswith('ac-')
    }
    out[who + '_may_sync'] = access.may_sync_pipeline(viewer, 'ac-mixed', conn)[0]
    graph = engine.plan_graph(conn, None, viewer=viewer)
    out[who + '_graph'] = sorted(node['pipe_ref'].split('|')[2] for node in graph['nodes'] if node['pipe_ref'].startswith(INSTANCE + '|ac:src'))
    ### A child they may not see is counted, not named, and no edge leads to it.
    lineage = engine.plan_graph(conn, [parent], children=1, viewer=viewer)
    out[who + '_lineage'] = [sorted(node['pipe_ref'].split('|')[2] for node in lineage['nodes']),
                             lineage['hidden'], len(lineage['edges'])]
print('JSON:' + json.dumps(out, default=str))
'''))
    every = 'sql:apihosttest|ac:src|ac_every|None'
    ops = 'sql:apihosttest|ac:src|ac_ops|None'
    ### The member of `data` sees the Pipeline with one hidden member, and not the `ops` one.
    assert out['member'] == {'ac-mixed': [[every], 1]}
    assert out['member_may_sync'] is False
    assert out['other']['ac-mixed'] == [sorted([every, ops]), 0]
    assert out['other']['ac-ops-only'] == [[ops], 0]
    assert out['other_may_sync'] is True
    assert out['admin']['ac-mixed'][1] == 0 and out['admin_may_sync'] is True
    ### The canvas and plans leave hidden pipes out (and count them).
    assert 'ac_ops' not in out['member_graph'] and 'ac_ops' in out['admin_graph']
    assert out['member_lineage'] == [['ac_parent'], 1, 0]
    assert out['other_lineage'] == [['ac_ops_child', 'ac_parent'], 0, 1]


def test_the_console_sql_guard_refuses_what_the_server_refuses(sandbox):
    """
    AC8 in the console (`meerschaum.api.dash.sql_guard`, the server's rules): Meerschaum's own
    tables, text-query functions, config references and the tables of pipes the viewer cannot
    see (bare, quoted, qualified or in a string) are refused for non-administrators; catalog reads
    only while the instance has pipes they cannot see.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.api.dash import sql_guard
member = access.get_viewer(SESSIONS['member'])
other_target = str(PIPES['ops'].target)
cases = {
    'plain': 'SELECT * FROM my_table',
    'users': 'SELECT * FROM mrsm_users',
    'quoted_users': 'SELECT * FROM "mrsm_users"',
    'text_query': "SELECT query_to_xml('select 1', true, true, '')",
    'config': "SELECT 'MRSM{meerschaum:connectors:sql:main:password}'",
    'hidden': f'SELECT * FROM {other_target}',
    'hidden_quoted': f'SELECT * FROM "{other_target}"',
    'hidden_string': f"SELECT * FROM t WHERE name = '{other_target}'",
    'catalog': 'SELECT * FROM information_schema.tables',
    'query_column': 'SELECT query, count(*) FROM logs GROUP BY query',
}
pipe = mrsm.Pipe(str(conn), 'ac_guarded', instance=conn)
print('JSON:' + json.dumps({
    name: sql_guard.check_query(member, sql, pipe) for name, sql in cases.items()
}, default=str))
'''))
    assert out['plain'] is None and out['query_column'] is None
    for name in ('users', 'quoted_users', 'text_query', 'config', 'hidden', 'hidden_quoted',
                 'hidden_string', 'catalog'):
        assert out[name], name
