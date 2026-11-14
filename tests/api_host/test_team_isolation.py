#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Isolated teams in the web console (5.1), in process on the sandbox's SQLite instance, as real
users with real console sessions (`SIGN_IN`):

- a member of an isolated team is walled off from Everyone: its pipes, jobs, connectors, plugins,
  teams and accounts, and they never hold the permissions that act with the server's credentials;
- a member of an isolated team and an open team keeps the open team's view;
- New team creates the team with its settings (isolation, default instance, grants), and with a
  database of its own it writes the team's connector and makes it the default instance;
- Settings edits them, keeping the team's database;
- a connector another process wrote to the configuration is picked up.
"""

import json

from tests.api_host._helpers import SIGN_IN, run_python, sandbox  # noqa: F401


def _json(result) -> dict:
    assert result.returncode == 0, result.stderr[-6000:]
    line = [line for line in result.stdout.splitlines() if line.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


SEED = SIGN_IN + '''
import json
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from dash._callback_context import context_value
from dash._utils import AttributeDict
conn = get_api_connector()
INSTANCE = str(conn)

def trigger(prop_id, value=1):
    context_value.set(AttributeDict(**{'triggered_inputs': [{'prop_id': prop_id, 'value': value}]}))

def team(name, **attributes):
    record = mrsm.Team(name, instance=conn, attributes=attributes)
    if not record.exists():
        assert record.register()[0]
    return record

def make(mk, access=None):
    params = {'access': access} if access is not None else {}
    pipe = mrsm.Pipe('ti:src', mk, instance=conn, parameters=params)
    if pipe.get_id() is None:
        assert pipe.register()[0]
    return pipe

from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash import access, team_setup
'''


def test_members_of_an_isolated_team_are_walled_off_from_everyone(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.api.dash import overview, pipes_table, connector_listing, components
from meerschaum.api.dash.users import visible_teams, viewer as page_viewer
from meerschaum.connectors import get_connector
team('wetlands', isolated=True, instance='sql:wetdb', connectors=['sql:wetdb'], plugins=['noaa'])
team('open')
SESSIONS = {
    'admin': sign_in('ti-admin', user_type='admin'),
    'ann': sign_in('ti-ann', role='operator', teams={'wetlands': 'member'}),
    'mixed': sign_in('ti-mixed', teams={'wetlands': 'member', 'open': 'member'}),
    'bob': sign_in('ti-bob'),
}
make('legacy')
make('everyone_public', {'team': 'open', 'visibility': 'everyone'})
make('wet', {'team': 'wetlands'})
make('open_only', {'team': 'open'})
get_connector('sql', 'wetdb', flavor='sqlite', database=':memory:', teams=['wetlands'])
get_connector('sql', 'shared', flavor='sqlite', database=':memory:')

out = {}
for who, session in SESSIONS.items():
    viewer = access.get_viewer(session)
    rows = pipes_table.list_rows(conn, viewer=viewer, connector_keys=['ti:src']) if hasattr(pipes_table, 'list_rows') else []
    seen = sorted(
        mk for ck, mk, lk, parameters in access.registered_pipes(conn)
        if ck == 'ti:src' and access.can_see_pipe(viewer, parameters)
    )
    out[who] = {
        'isolated': viewer.is_isolated,
        'pipes': seen,
        'overview': overview.pipes_summary(conn, viewer=viewer)['total'],
        'everyone': viewer.in_team('Everyone'),
        'withheld': sorted(p for p in ('actions:execute', 'webterm:use', 'instance:chain') if viewer.can(p)),
        'instance': viewer.default_instance,
        'plugins': [viewer.can_use_plugin('noaa'), viewer.can_use_plugin('usgs')],
        'connectors': [
            connector_listing.connector_team_allows(keys, viewer)
            for keys in ('sql:wetdb', 'sql:shared', INSTANCE)
        ],
        'teams': sorted(t['name'] for t in visible_teams(access.list_teams(), page_viewer(session))),
        'team_names': access._principal(viewer).team_names if viewer.principal is None else viewer.principal.team_names,
    }

### Jobs: a local job is Everyone's; a team's job is its team's.
from meerschaum.jobs import filter_visible_jobs
class FakeJob:
    def __init__(self, name, properties):
        self.name = name
        self._properties = properties
    def get_daemon_properties(self):
        return self._properties
jobs = {
    'local': FakeJob('local', {}),
    'wet': FakeJob('wet', {'owner': 'ti-mixed', 'team': 'wetlands'}),
}
for who, session in SESSIONS.items():
    out[who]['jobs'] = sorted(filter_visible_jobs(access.get_viewer(session), jobs))
print('JSON:' + json.dumps(out, default=str))
'''))
    ann, mixed, bob, admin = out['ann'], out['mixed'], out['bob'], out['admin']
    assert ann['isolated'] is True and ann['everyone'] is False
    assert ann['pipes'] == ['wet'] and ann['overview'] == 1
    ### An Operator loses what acts with the server's credentials.
    assert ann['withheld'] == []
    assert ann['instance'] == 'sql:wetdb'
    assert ann['plugins'] == [True, False]
    ### Their team's connector, not an unrestricted one, nor the API's own instance.
    assert ann['connectors'] == [True, False, False]
    assert ann['teams'] == ['wetlands'] and ann['team_names'] == ['wetlands']
    assert ann['jobs'] == ['wet']

    assert mixed['isolated'] is False and mixed['everyone'] is True
    assert mixed['pipes'] == ['everyone_public', 'legacy', 'open_only', 'wet']
    assert mixed['connectors'] == [True, True, True]
    assert mixed['jobs'] == ['local', 'wet']
    assert mixed['plugins'] == [True, True]

    assert bob['isolated'] is False
    assert bob['pipes'] == ['everyone_public', 'legacy']
    assert bob['connectors'] == [False, True, True]
    assert bob['jobs'] == ['local'] and bob['teams'] == ['Everyone']

    assert admin['isolated'] is False
    assert admin['pipes'] == ['everyone_public', 'legacy', 'open_only', 'wet']
    assert admin['jobs'] == ['local', 'wet']


def test_new_team_and_settings_dialogs(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.api.dash.callbacks import users as callbacks
from meerschaum.api.dash.users import build_users_modals
from meerschaum.config import get_config
admin = sign_in('ts-admin', user_type='admin')
editor = sign_in('ts-editor')
out = {}

def ids(component, found=None):
    found = set() if found is None else found
    identifier = getattr(component, 'id', None)
    if isinstance(identifier, str):
        found.add(identifier)
    children = getattr(component, 'children', None)
    for child in (children if isinstance(children, (list, tuple)) else [children]):
        if child is not None and not isinstance(child, (str, int, float)):
            ids(child, found)
    return found

found = set()
for modal in build_users_modals():
    ids(modal, found)
out['fields'] = sorted(i for i in found if i.startswith(('teams-new-', 'teams-settings-')))

def submit(session, name, storage='instance', base=None, database=None, instance=None,
           isolated=True, connectors=None, plugins=None, plugins_write=False):
    trigger('teams-new-submit.n_clicks')
    return callbacks.submit_new_team(
        1, name, storage, base, database, instance, isolated, connectors, plugins, plugins_write,
        0, session,
    )

### Only administrators.
out['editor'] = str(submit(editor, 'nope')[1])
out['editor_exists'] = mrsm.Team('nope', instance=conn).exists()

### An isolated team on an existing instance, with grants.
result = submit(admin, 'rivers', instance=INSTANCE, connectors=['plugin:noaa'], plugins=['noaa'],
                plugins_write=True)
out['rivers_closed'] = result[0]
out['rivers'] = team_setup.get_team_attributes('rivers')

### A team with a database of its own: the provisioning is the extension's (faked here).
calls = []
class FakeCore:
    def team_database_slug(self, name):
        return str(name).lower().replace('-', '_')
    def team_database_valid_name(self, name):
        return (name.isidentifier(), 'bad name')
    def team_database_create(self, base, base_attributes, team, kind, keys, name=None, grafana=None):
        calls.append([base, sorted(base_attributes), team, kind, keys, name, grafana])
        return True, 'ok', {
            'attributes': {'flavor': 'postgresql', 'host': 'db', 'port': 5432,
                           'username': 'mrsm_' + name, 'password': 'generated-secret',
                           'database': name, 'teams': [team]},
            'summary': {'kind': kind, 'connector': keys, 'base': base, 'database': name,
                        'role': 'mrsm_' + name, 'readonly_role': 'mrsm_' + name + '_ro'},
            'notes': [],
            'grafana': None,
            'grafana_error': None,
        }
    def grafana_ensure_team(self, section, team):
        return True, 'ok', None
team_setup._core = lambda: FakeCore()
get_config('meerschaum', 'connectors').setdefault('sql', {})['pgbase'] = {
    'flavor': 'postgresql', 'host': 'db', 'port': 5432, 'username': 'u', 'password': 'p',
    'database': 'main',
}
out['bases'] = team_setup.database_bases()
result = submit(admin, 'Wet-Lands', storage='database', base='sql:pgbase')
out['wet_closed'] = result[0]
out['wet_alert'] = str(result[2])
out['calls'] = calls
out['wet'] = team_setup.get_team_attributes('Wet-Lands')
out['connector_live'] = dict(get_config('meerschaum', 'connectors', 'sql').get('wet_lands') or {})
from meerschaum.config._read_config import read_config
disk = read_config(keys=['meerschaum'], write_missing=False, substitute=False)
out['connector_disk'] = dict(disk['meerschaum']['connectors']['sql'].get('wet_lands') or {})
out['no_secret_in_team'] = 'generated-secret' not in json.dumps(out['wet'])
### A team's own connector is not a base for another; the same name twice is refused.
out['bases_after'] = team_setup.database_bases()
out['duplicate'] = str(submit(admin, 'Wet-Lands', storage='database', base='sql:pgbase')[1])

### Settings: what is stored, then edited (the database is kept).
trigger(json.dumps({'action': 'settings', 'index': 'Wet-Lands', 'type': 'team-action'}, sort_keys=True) + '.n_clicks')
from dash._callback_context import context_value
from dash._utils import AttributeDict
context_value.set(AttributeDict(**{'triggered_inputs': [{
    'prop_id': '{"action":"settings","index":"Wet-Lands","type":"team-action"}.n_clicks', 'value': 1,
}]}))
opened = callbacks.open_team_settings([1], None, admin)
out['settings_open'] = [opened[0], opened[2], opened[6], opened[7], opened[9]]
trigger('teams-settings-submit.n_clicks')
saved = callbacks.submit_team_settings(
    1, {'team': 'Wet-Lands'}, 'sql:wet_lands', False, ['sql:wet_lands', 'plugin:usgs'], ['usgs'],
    True, 0, admin,
)
out['settings_saved'] = saved[0]
out['wet_after'] = team_setup.get_team_attributes('Wet-Lands')
refused = callbacks.submit_team_settings(
    1, {'team': 'Wet-Lands'}, None, True, [], [], False, 0, editor,
)
out['settings_refused'] = str(refused[1])

### A connector another process wrote to the configuration is picked up.
import os, time
section = disk['meerschaum']
section['connectors']['sql']['elsewhere'] = {'flavor': 'sqlite', 'database': 'x.db'}
team_setup.refresh_connectors()
from meerschaum.config._edit import write_config
time.sleep(0.05)
assert write_config({'meerschaum': section})
path = team_setup._connectors_file()
os.utime(path, (time.time() + 5, time.time() + 5))
out['refreshed'] = team_setup.refresh_connectors()
out['elsewhere'] = 'elsewhere' in get_config('meerschaum', 'connectors', 'sql')
print('JSON:' + json.dumps(out, default=str))
'''))
    for field in (
        'teams-new-name', 'teams-new-storage', 'teams-new-base', 'teams-new-database',
        'teams-new-instance', 'teams-new-isolated', 'teams-new-connectors', 'teams-new-plugins',
        'teams-new-plugins-write', 'teams-settings-instance', 'teams-settings-isolated',
        'teams-settings-connectors', 'teams-settings-plugins', 'teams-settings-plugins-write',
    ):
        assert field in out['fields'], field
    assert 'administrators' in out['editor'] and out['editor_exists'] is False

    assert out['rivers_closed'] is False
    rivers = out['rivers']
    assert rivers['isolated'] is True and rivers['plugins_write'] is True
    assert rivers['plugins'] == ['noaa']
    assert rivers['connectors'][0] == rivers['instance'] and 'plugin:noaa' in rivers['connectors']
    assert 'database' not in rivers

    assert out['bases'] == ['sql:main', 'sql:pgbase']
    assert out['wet_closed'] is False, out['wet_alert']
    assert len(out['calls']) == 1
    base, _, team, kind, keys, name, grafana = out['calls'][0]
    assert (base, team, kind, keys, name, grafana) == (
        'sql:pgbase', 'Wet-Lands', 'database', 'sql:wet_lands', 'wet_lands', None,
    )
    wet = out['wet']
    assert wet['instance'] == 'sql:wet_lands' and wet['connectors'] == ['sql:wet_lands']
    assert wet['isolated'] is True
    assert wet['database']['kind'] == 'database' and wet['database']['connector'] == 'sql:wet_lands'
    assert out['no_secret_in_team'] is True
    for where in ('connector_live', 'connector_disk'):
        assert out[where]['password'] == 'generated-secret', where
        assert out[where]['teams'] == ['Wet-Lands'], where
    assert out['bases_after'] == ['sql:main', 'sql:pgbase']
    assert 'exists' in out['duplicate']

    assert out['settings_open'] == [True, {'team': 'Wet-Lands'}, 'sql:wet_lands', True, ['sql:wet_lands']]
    assert out['settings_saved'] is False
    after = out['wet_after']
    assert 'isolated' not in after and after['plugins_write'] is True
    assert after['connectors'] == ['sql:wet_lands', 'plugin:usgs'] and after['plugins'] == ['usgs']
    assert after['database'] == wet['database']
    assert 'administrators' in out['settings_refused']
    assert out['refreshed'] is True and out['elsewhere'] is True
