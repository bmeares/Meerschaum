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
from meerschaum.api.dash.users import visible_teams
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
        'teams': sorted(t['name'] for t in visible_teams(access.list_teams(), access.resolve_viewer(session, fresh=True))),
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
    def team_database_create(self, base, base_attributes, team, kind, keys, name=None, grafana=None,
                             grafana_recorded=None):
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
    def grafana_ensure_team(self, section, team, recorded=None):
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

    ### `sql:main` is the sandbox's SQLite file: not a base.
    assert out['bases'] == ['sql:pgbase']
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
    assert out['bases_after'] == ['sql:pgbase']
    assert 'exists' in out['duplicate']

    assert out['settings_open'] == [True, {'team': 'Wet-Lands'}, 'sql:wet_lands', True, ['sql:wet_lands']]
    assert out['settings_saved'] is False
    after = out['wet_after']
    assert 'isolated' not in after and after['plugins_write'] is True
    assert after['connectors'] == ['sql:wet_lands', 'plugin:usgs'] and after['plugins'] == ['usgs']
    assert after['database'] == wet['database']
    assert 'administrators' in out['settings_refused']
    assert out['refreshed'] is True and out['elsewhere'] is True


def test_grafana_follows_console_renames_and_deletions_by_saved_id(sandbox):
    """
    Renaming or deleting a team in the console renames or retires its Grafana organization by
    the id saved in its attributes, and a Grafana failure is a warning, not a success.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.api.dash.callbacks import users as callbacks
admin = sign_in('gf-admin', user_type='admin')
calls = []
class FakeCore:
    def grafana_rename_team(self, section, org_id, new):
        calls.append(['rename', org_id, new])
        return False, 'Grafana: renaming an organization failed: 409'
    def grafana_retire_team(self, section, team, org_id):
        calls.append(['retire', team, org_id])
        return True, 'Success'
    def grafana_ensure_team(self, section, team, recorded=None):
        calls.append(['ensure', team, [list(r) for r in (recorded or [])]])
        return True, 'Success', 41
team_setup._core = lambda: FakeCore()
team_setup.grafana_enabled = lambda: True
team_setup.grafana_config = lambda: {'enabled': True}
team('gf-data', grafana={'org_id': 7})
team('gf-other', grafana={'org_id': 8})
team('gf-plain')
out = {}

trigger('teams-rename-submit.n_clicks')
renamed = callbacks.submit_rename_team(
    1, {'action': 'rename', 'team': 'gf-data'}, 'gf-analytics', 0, admin,
)
out['rename_alert'] = renamed[2].to_plotly_json()['props']
out['renamed_exists'] = mrsm.Team('gf-analytics', instance=conn).exists()

trigger('teams-delete-submit.n_clicks')
deleted = callbacks.submit_delete_team(
    1, {'action': 'delete', 'team': 'gf-analytics'}, 'gf-analytics', 0, admin,
)
out['delete_alert'] = deleted[2].to_plotly_json()['props']

### A team without an organization gets one when an administrator saves its settings.
trigger('teams-settings-submit.n_clicks')
saved = callbacks.submit_team_settings(
    1, {'team': 'gf-plain'}, None, False, [], [], False, 0, admin,
)
out['plain'] = team_setup.get_team_attributes('gf-plain')
out['calls'] = calls
print('JSON:' + json.dumps(out, default=str))
'''))
    assert out['renamed_exists'] is True
    assert out['calls'][0] == ['rename', 7, 'gf-analytics']
    assert out['rename_alert']['color'] == 'warning', out['rename_alert']
    assert '409' in json.dumps(out['rename_alert']['children'])
    assert out['calls'][1] == ['retire', 'gf-analytics', 7]
    assert out['delete_alert']['color'] == 'success', out['delete_alert']
    ensure = out['calls'][2]
    assert ensure[0] == 'ensure' and ensure[1] == 'gf-plain'
    assert ['gf-other', 8] in ensure[2]
    assert out['plain']['grafana'] == {'org_id': 41}


def test_a_deleted_teams_connector_is_not_inherited_by_name(sandbox):
    """
    Deleting a team in the console keeps its connector (and its database) but retires the
    connector's reservation, so a new team of that name gets nothing of the old one; renaming a
    team moves its reservation; and a name a connector is still reserved for is refused.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.config._read_config import read_config
attributes = {'flavor': 'sqlite', 'database': 'old.db'}
assert team_setup.write_connector('sql:res_old', dict(attributes, teams=['ti-res']))[0]
assert team_setup.write_connector('sql:res_move', dict(attributes, teams=['ti-move']))[0]
assert team_setup.write_connector('sql:res_left', dict(attributes, teams=['ti-left']))[0]
out = {}
### A name a connector is still reserved for (no such team) is refused, by both dialogs.
out['create_refused'] = team_setup.create_team('ti-left', isolated=True)
team('ti-other')
out['rename_refused'] = access.rename_team('ti-other', 'ti-left')

### Delete: the connector stays, its reservation names no team.
old = team('ti-res')
old_id = old.id
out['deleted'] = access.delete_team('ti-res')
disk = read_config(keys=['meerschaum'], write_missing=False, substitute=False)
out['old_teams'] = disk['meerschaum']['connectors']['sql']['res_old']['teams']
out['old_kept'] = disk['meerschaum']['connectors']['sql']['res_old']['database']
out['old_id'] = old_id
out['reserved_after_delete'] = team_setup.reserved_connectors('ti-res')
### A new team of the deleted team's name is created and gets nothing.
out['recreated'] = team_setup.create_team('ti-res', isolated=True)

### Rename: the reservation follows the team.
team('ti-move')
out['renamed'] = access.rename_team('ti-move', 'ti-moved')
disk = read_config(keys=['meerschaum'], write_missing=False, substitute=False)
out['moved_teams'] = disk['meerschaum']['connectors']['sql']['res_move']['teams']
print('JSON:' + json.dumps(out, default=str))
'''))
    assert out['create_refused'][0] is False, out
    assert 'sql:res_left' in out['create_refused'][1]
    assert out['rename_refused'][0] is False and 'sql:res_left' in out['rename_refused'][1]
    assert out['deleted'][0] is True, out
    assert out['old_teams'] == [f"Deleted team ti-res #{out['old_id']}"], out
    assert out['old_kept'] == 'old.db'
    assert out['reserved_after_delete'] == []
    assert out['recreated'][0] is True, out
    assert out['renamed'][0] is True, out
    assert out['moved_teams'] == ['ti-moved']


def test_a_team_connector_is_the_only_change_to_the_configuration_file(sandbox):
    """
    New team's connector (and a rename's reservations) are written to `meerschaum.yaml` as the
    file holds them: references stay references, and `sql:main` from `MRSM_SQL_MAIN` or an
    `MRSM_CONFIG` patch is never written. The file holding the team's password is owner-only.
    """
    import os
    import stat
    root, plugins = sandbox
    config_dir = root / 'config'
    config_dir.mkdir(parents=True, exist_ok=True)
    path = config_dir / 'meerschaum.yaml'
    path.write_text(
        "connectors:\n"
        "  sql:\n"
        "    shared:\n"
        "      flavor: sqlite\n"
        "      database: MRSM{meerschaum:connectors:sql:local:database}\n"
        "  api:\n"
        "    upstream:\n"
        "      host: api.example.com\n"
    )
    os.chmod(path, 0o644)
    out = _json(run_python(root, plugins, SEED + '''
from meerschaum.config._read_config import read_config
from meerschaum.config import get_config
out = {}
out['main_live'] = get_config('meerschaum', 'connectors', 'sql', 'main', 'uri')
out['written'] = team_setup.write_connector(
    'sql:wetlands', {'flavor': 'sqlite', 'database': 'w.db', 'password': 'team-generated-pw',
                     'teams': ['ti-cfg']},
)
team('ti-cfg')
out['renamed'] = access.rename_team('ti-cfg', 'ti-cfg2')
print('JSON:' + json.dumps(out, default=str))
''', extra_env={
        'MRSM_SQL_MAIN': 'timescaledb://mrsm:env-only-pw-4242@127.0.0.1:1/meerschaum',
        'MRSM_CONFIG': json.dumps({'meerschaum': {'connectors': {'api': {'upstream': {
            'password': 'patch-only-k3y-31337',
        }}}}}),
    }))
    assert 'env-only-pw-4242' in out['main_live'], out
    assert out['written'][0] is True, out
    assert out['renamed'][0] is True, out
    text = path.read_text()
    assert 'env-only-pw-4242' not in text, text
    assert 'patch-only-k3y-31337' not in text, text
    assert 'MRSM{meerschaum:connectors:sql:local:database}' in text, text
    assert 'team-generated-pw' in text, text
    assert '- ti-cfg2' in text, text
    assert 'main:' not in text, text
    assert stat.S_IMODE(os.stat(path).st_mode) & 0o077 == 0, oct(os.stat(path).st_mode)


def test_isolated_team_instance_policy_refusal_never_falls_back_to_api_default(sandbox):
    """
    When an isolated team's default instance is blocked by `api:permissions:instances`, Dash
    never falls back to showing or querying the API's own instance (which the isolated viewer
    cannot access), distinguishes policy denial from team isolation, and surfaces one clear alert.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
import flask
import meerschaum.api as api
from meerschaum.connectors import get_connector
from meerschaum.api.dash import engine
from meerschaum.api.dash.pages import pipes as pipes_page
from meerschaum.api.dash.callbacks import pipes as pipes_cb

team('whitedl', isolated=True, instance='sql:whitedl', connectors=['sql:whitedl'])
get_connector('sql', 'whitedl', flavor='sqlite', database=':memory:', teams=['whitedl'])
session = sign_in('whitedl-mgr', role='operator', teams={'whitedl': 'manager'})
viewer = access.get_viewer(session)
default = str(api.get_api_connector())

app = flask.Flask('t')
out = {'default': default}

orig_in_request = access._in_request
orig_current_viewer = access.current_viewer
access._in_request = lambda: True
access.current_viewer = lambda: viewer
try:
    ### 1. API forbids additional instances (`allow_multiple_instances=False`, narrow allowlist).
    api.permissions_config.setdefault('instances', {})['allow_multiple_instances'] = False
    api.permissions_config['instances']['allowed_instance_keys'] = [default, 'sql:sra']

    with app.test_request_context('/dash/pipes'):
        out['fresh_problem'] = engine.instance_problem()
        out['fresh_display'] = engine.display_instance_keys()
        out['fresh_page'] = list(engine.page_instance())
        out['req_problem'] = engine.instance_problem('sql:whitedl')
        out['req_page'] = list(engine.page_instance('sql:whitedl'))
        try:
            engine.get_instance('sql:whitedl', strict=True)
            out['strict_whitedl'] = None
        except PermissionError as e:
            out['strict_whitedl'] = str(e)
        try:
            engine.get_instance(default, strict=True)
            out['strict_default'] = None
        except PermissionError as e:
            out['strict_default'] = str(e)
        out['default_problem'] = engine.instance_problem(default)

        layout = pipes_page.layout('/dash/pipes', 'http://localhost/dash/pipes')
        meta = next(
            node.data for node in pipes_page._walk(layout)
            if getattr(node, 'id', None) == 'pipes-page-meta'
        )
        alert = next(
            node.children for node in pipes_page._walk(layout)
            if getattr(node, 'id', None) == 'pipes-instance-alert'
        )
        out['pipes_meta'] = meta
        out['pipes_instance_alert'] = str(alert)
        cb_token = context_value.set(AttributeDict(**{
            'triggered_inputs': [{'prop_id': 'pipes-location.search', 'value': ''}],
        }))
        try:
            store, cb_alert = pipes_cb.load_pipes_rows('', 0, '/dash/pipes', meta, session, None)
        finally:
            context_value.reset(cb_token)
        out['cb_store_instance'] = store['instance']
        out['cb_rows'] = store['rows']
        out['cb_alert'] = cb_alert

    ### 2. API allows multiple instances, but the browser still holds a stale `sql:main` cookie.
    api.permissions_config['instances']['allow_multiple_instances'] = True
    api.permissions_config['instances']['allowed_instance_keys'] = ['*']
    with app.test_request_context(
        '/dash/pipes', headers={'Cookie': f'mrsm-instance={default.replace(":", "%3A")}'}
    ):
        out['stale_cookie_page'] = list(engine.page_instance())
    with app.test_request_context('/dash/pipes'):
        out['allowed_page'] = list(engine.page_instance())
finally:
    access._in_request = orig_in_request
    access.current_viewer = orig_current_viewer

print('JSON:' + json.dumps(out, default=str))
'''))
    default = out['default']
    for key in ('fresh_problem', 'req_problem', 'pipes_instance_alert'):
        msg = out[key]
        assert msg and "'sql:whitedl'" in msg, (key, msg)
        assert 'allow_multiple_instances' in msg and 'allowed_instance_keys' in msg, (key, msg)
        assert f'showing {default} instead' not in msg, (key, msg)
    assert out['fresh_display'] == 'sql:whitedl'
    assert out['fresh_page'][0] == 'sql:whitedl' and out['fresh_page'][2] is True
    assert out['req_page'][0] == 'sql:whitedl' and out['req_page'][2] is True
    assert 'allow_multiple_instances' in (out['strict_whitedl'] or '')
    assert 'not available to your teams' in (out['strict_default'] or '')
    assert 'not available to this API' not in (out['strict_default'] or '')
    assert 'not available to your teams' in (out['default_problem'] or '')
    assert "'sql:whitedl'" in (out['default_problem'] or '')
    assert f'showing {default} instead' not in (out['default_problem'] or '')

    assert out['pipes_meta']['instance'] == 'sql:whitedl'
    assert out['pipes_meta']['instance_blocked'] is True
    assert out['cb_store_instance'] == 'sql:whitedl'
    assert out['cb_rows'] == []
    assert out['cb_alert'] == []

    stale_keys, stale_problem, stale_blocked = out['stale_cookie_page']
    assert stale_keys == 'sql:whitedl' and stale_blocked is False
    assert 'not available to your teams' in stale_problem and 'showing sql:whitedl instead' in stale_problem
    assert out['allowed_page'] == ['sql:whitedl', None, False]


def test_team_setup_warns_when_api_instance_policy_blocks_team_instance(sandbox):
    """
    Creating or editing a team with a default instance blocked by `api:permissions:instances`
    succeeds but appends a warning note and renders a warning alert in the Users & Teams dialogs.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
import meerschaum.api as api
from meerschaum.api.dash.callbacks import users as callbacks

admin = sign_in('pol-admin', user_type='admin')
default = str(api.get_api_connector())
api.permissions_config.setdefault('instances', {})['allow_multiple_instances'] = False
api.permissions_config['instances']['allowed_instance_keys'] = [default]

out = {}
trigger('teams-new-submit.n_clicks')
created = callbacks.submit_new_team(
    1, 'pol-team', 'instance', None, None, 'sql:whitedl', True, [], [], False, 0, admin,
)
out['create_closed'] = created[0]
out['create_alert'] = created[2].to_plotly_json()['props']

trigger('teams-settings-submit.n_clicks')
edited = callbacks.submit_team_settings(
    1, {'team': 'pol-team'}, 'sql:whitedl', True, ['sql:whitedl'], [], False, 0, admin,
)
out['edit_closed'] = edited[0]
out['edit_alert'] = edited[2].to_plotly_json()['props']

### Once the API policy allows it, saving again returns a normal success alert without a warning.
api.permissions_config['instances']['allow_multiple_instances'] = True
api.permissions_config['instances']['allowed_instance_keys'] = ['*']
trigger('teams-settings-submit.n_clicks')
edited_ok = callbacks.submit_team_settings(
    1, {'team': 'pol-team'}, 'sql:whitedl', True, ['sql:whitedl'], [], False, 0, admin,
)
out['edit_ok_alert'] = edited_ok[2].to_plotly_json()['props']
print('JSON:' + json.dumps(out, default=str))
'''))
    assert out['create_closed'] is False
    assert out['create_alert']['color'] == 'warning', out['create_alert']
    create_text = json.dumps(out['create_alert']['children'])
    assert 'allow_multiple_instances' in create_text and 'allowed_instance_keys' in create_text
    assert 'sql:whitedl' in create_text

    assert out['edit_closed'] is False
    assert out['edit_alert']['color'] == 'warning', out['edit_alert']
    edit_text = json.dumps(out['edit_alert']['children'])
    assert 'allow_multiple_instances' in edit_text and 'sql:whitedl' in edit_text

    assert out['edit_ok_alert']['color'] == 'success', out['edit_ok_alert']


def test_pages_dropdown_nests_large_plugin_groups_and_respects_team_plugins(sandbox):
    """
    The navbar's Pages dropdown nests multi-page groups in collapsible `<details>` (collapsed by
    default when a group has > 3 pages, expanded when small or active), strips redundant group
    prefixes, nests sub-paths, and hides plugins disallowed for an isolated viewer.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
import plotly.utils
from meerschaum.api.dash import components
from meerschaum.api.dash.callbacks import dashboard as dash_cb
from meerschaum.plugins import _plugin_endpoints_to_pages

team('pg-iso-team', isolated=True, instance='sql:whitedl', connectors=['sql:whitedl'], plugins=['solo'])
admin = sign_in('pg-admin', user_type='admin')
isolated = sign_in('pg-iso', role='operator', teams={'pg-iso-team': 'member'})

def _dummy():
    return None

orig_pages = dict(_plugin_endpoints_to_pages)
orig_paths = dict(dash_cb._paths)
try:
    _plugin_endpoints_to_pages.clear()
    _plugin_endpoints_to_pages['Anchored'] = {
        'anchored': {'function': _dummy, 'page_key': 'anchored'},
        'anchored/login': {'function': _dummy, 'page_key': 'anchored/login'},
        'anchored/crew': {'function': _dummy, 'page_key': 'anchored/crew'},
        'anchored/calendar': {'function': _dummy, 'page_key': 'anchored/calendar'},
        'anchored/admin/users': {'function': _dummy, 'page_key': 'anchored/admin/users'},
        'anchored/admin/audit': {'function': _dummy, 'page_key': 'anchored/admin/audit'},
    }
    _plugin_endpoints_to_pages['Reports'] = {
        'reports/daily': {'function': _dummy, 'page_key': 'reports/daily'},
        'reports/weekly': {'function': _dummy, 'page_key': 'reports/weekly'},
    }
    _plugin_endpoints_to_pages['Solo'] = {
        'solo/home': {'function': _dummy, 'page_key': 'solo/home'},
    }
    _plugin_endpoints_to_pages['Weather'] = {
        'forecast': {'function': _dummy, 'page_key': 'forecast'},
    }
    for ep in (
        '/dash/anchored', '/dash/anchored/login', '/dash/anchored/crew',
        '/dash/anchored/calendar', '/dash/anchored/admin/users', '/dash/anchored/admin/audit',
        '/dash/reports/daily', '/dash/reports/weekly', '/dash/solo/home', '/dash/forecast',
    ):
        dash_cb._paths[ep] = True

    def plain(comp):
        return json.loads(json.dumps(comp, cls=plotly.utils.PlotlyJSONEncoder))

    def walk(node):
        if isinstance(node, list):
            for child in node:
                yield from walk(child)
        elif isinstance(node, dict):
            yield node
            yield from walk(node.get('props', {}).get('children'))

    def summarize(menu):
        p = plain(menu)
        details = []
        top_items = []
        for child in p.get('props', {}).get('children') or []:
            if child.get('type') == 'Details':
                props = child.get('props', {})
                labels = [
                    n.get('props', {}).get('children')
                    for n in walk(child)
                    if n.get('type') == 'DropdownMenuItem'
                ]
                subgroups = [
                    n.get('props', {}).get('open')
                    for n in walk(props.get('children'))
                    if n.get('type') == 'Details'
                ]
                details.append({
                    'group': props.get('data-mrsm-pages-group'),
                    'open': bool(props.get('open', False)),
                    'labels': labels,
                    'subgroups': subgroups,
                })
            elif child.get('type') == 'DropdownMenuItem':
                top_items.append(child.get('props', {}).get('children'))
        return {'details': details, 'top_items': top_items}

    admin_viewer = access.get_viewer(admin)
    iso_viewer = access.get_viewer(isolated)
    out = {
        'pipes': summarize(components._build_pages_menu('/dash/pipes', viewer=admin_viewer)),
        'active_anchored': summarize(components._build_pages_menu('/dash/anchored/login', viewer=admin_viewer)),
        'iso_groups': sorted(components.get_plugin_nav_pages(viewer=iso_viewer).keys()),
    }
finally:
    _plugin_endpoints_to_pages.clear()
    _plugin_endpoints_to_pages.update(orig_pages)
    dash_cb._paths.clear()
    dash_cb._paths.update(orig_paths)

print('JSON:' + json.dumps(out, default=str))
'''))
    pipes_groups = {g['group']: g for g in out['pipes']['details']}
    assert pipes_groups['Anchored']['open'] is False
    assert pipes_groups['Anchored']['labels'] == ['Anchored', 'Login', 'Crew', 'Calendar', 'Users', 'Audit']
    assert len(pipes_groups['Anchored']['subgroups']) == 1
    assert pipes_groups['Reports']['open'] is True
    assert pipes_groups['Reports']['labels'] == ['Daily', 'Weekly']
    assert out['pipes']['top_items'] == ['Solo / Home', 'Weather', 'Forecast']

    active_groups = {g['group']: g for g in out['active_anchored']['details']}
    assert active_groups['Anchored']['open'] is True
    assert out['iso_groups'] == ['Solo']


def test_team_switcher_narrows_instances_pipes_connectors_plugins_and_jobs(sandbox):
    """
    Choosing a team in the navbar's Team switcher (`mrsm-team` cookie) narrows an admin or
    multi-team user to acting as that team across instances, pipes, connectors, plugins, jobs,
    and permissions, while keeping the switcher able to return to All teams and rejecting forged
    cookies.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
import flask
import plotly.utils
from meerschaum.connectors import get_connector
from meerschaum.jobs import filter_visible_jobs
from meerschaum.api.dash import components, connector_listing, engine

team('sw-alpha', isolated=True, instance='sql:swalpha', connectors=['sql:swalpha'], plugins=['noaa'])
team('sw-beta', isolated=True, instance='sql:swbeta', connectors=['sql:swbeta'], plugins=['usgs'])
team('sw-open')

get_connector('sql', 'swalpha', flavor='sqlite', database=':memory:', teams=['sw-alpha'])
get_connector('sql', 'swbeta', flavor='sqlite', database=':memory:', teams=['sw-beta'])
get_connector('sql', 'swshared', flavor='sqlite', database=':memory:')

make('sw_everyone')
make('sw_alpha', {'team': 'sw-alpha'})
make('sw_beta', {'team': 'sw-beta'})
make('sw_open', {'team': 'sw-open'})

class FakeJob:
    def __init__(self, name, properties):
        self.name = name
        self._properties = properties
    def get_daemon_properties(self):
        return self._properties

jobs = {
    'j_local': FakeJob('j_local', {}),
    'j_alpha': FakeJob('j_alpha', {'owner': 'u', 'team': 'sw-alpha'}),
    'j_beta': FakeJob('j_beta', {'owner': 'u', 'team': 'sw-beta'}),
}

admin = sign_in('sw-admin', user_type='admin')
multi = sign_in('sw-multi', role='operator', teams={'sw-alpha': 'manager', 'sw-beta': 'member'})
solo = sign_in('sw-solo', role='operator', teams={'sw-alpha': 'member'})
plain_user = sign_in('sw-plain', role='operator')

app = flask.Flask('t')

def plain(comp):
    return json.loads(json.dumps(comp, cls=plotly.utils.PlotlyJSONEncoder))

def find_id(node, target_id):
    if isinstance(node, list):
        for child in node:
            found = find_id(child, target_id)
            if found is not None:
                return found
    elif isinstance(node, dict):
        if node.get('props', {}).get('id') == target_id:
            return node
        return find_id(node.get('props', {}).get('children'), target_id)
    return None

def snapshot(session, team_cookie=None):
    headers = {}
    cookies = [f"mrsm-session-id={session['session-id']}"]
    if team_cookie is not None:
        cookies.append(f"mrsm-team={team_cookie}")
    headers['Cookie'] = '; '.join(cookies)
    with app.test_request_context('/dash/pipes', headers=headers):
        viewer = access.get_viewer(session)
        nav = plain(components.build_pages_navbar(pathname='/dash/pipes', session_id=session['session-id']))
        team_sel = find_id(nav, 'pages-team-select')
        inst_sel = find_id(nav, 'pages-instance-select')
        pipes = sorted(
            mk for ck, mk, lk, parameters in access.registered_pipes(conn)
            if ck == 'ti:src' and mk.startswith('sw_') and access.can_see_pipe(viewer, parameters)
        )
        return {
            'is_admin': viewer.is_admin,
            'is_real_admin': viewer.is_real_admin,
            'acting_team': viewer.acting_team,
            'isolated': viewer.is_isolated,
            'default_instance': viewer.default_instance,
            'display_instance': engine.display_instance_keys(),
            'manages_beta': viewer.manages('sw-beta'),
            'webterm': viewer.can('webterm:use'),
            'actions': viewer.can('actions:execute'),
            'pipes': pipes,
            'connectors': [
                connector_listing.connector_team_allows(k, viewer)
                for k in ('sql:swalpha', 'sql:swbeta', 'sql:swshared', INSTANCE)
            ],
            'plugins': [viewer.can_use_plugin('noaa'), viewer.can_use_plugin('usgs')],
            'jobs': sorted(filter_visible_jobs(viewer, jobs)),
            'team_select_options': (
                [o['value'] for o in team_sel['props']['options']] if team_sel else None
            ),
            'team_select_value': (team_sel['props']['value'] if team_sel else None),
            'team_select_acting_class': (
                'team-select-acting' in (team_sel['props'].get('class_name') or '')
                if team_sel else False
            ),
            'instance_select_options': (
                [o['value'] for o in inst_sel['props']['options']] if inst_sel else None
            ),
        }

out = {
    'admin_all': snapshot(admin),
    'admin_alpha': snapshot(admin, 'sw-alpha'),
    'admin_everyone': snapshot(admin, 'Everyone'),
    'multi_all': snapshot(multi),
    'multi_beta': snapshot(multi, 'sw-beta'),
    'multi_forged_open': snapshot(multi, 'sw-open'),
    'multi_forged_everyone': snapshot(multi, 'Everyone'),
    'solo': snapshot(solo),
    'plain_user': snapshot(plain_user),
}
print('JSON:' + json.dumps(out, default=str))
'''))
    # Admin: All teams -> sees everything and has switcher with all teams
    a_all = out['admin_all']
    assert a_all['is_admin'] is True and a_all['acting_team'] is None
    assert '' == a_all['team_select_options'][0] and 'Everyone' == a_all['team_select_options'][-1]
    for name in ('sw-alpha', 'sw-beta', 'sw-open'):
        assert name in a_all['team_select_options']
    assert a_all['team_select_value'] == '' and a_all['team_select_acting_class'] is False
    assert a_all['pipes'] == ['sw_alpha', 'sw_beta', 'sw_everyone', 'sw_open']

    # Admin acting as isolated team `sw-alpha`
    a_w = out['admin_alpha']
    assert a_w['is_admin'] is False and a_w['is_real_admin'] is True
    assert a_w['acting_team'] == 'sw-alpha' and a_w['isolated'] is True
    assert a_w['default_instance'] == 'sql:swalpha' and a_w['display_instance'] == 'sql:swalpha'
    assert a_w['webterm'] is False and a_w['actions'] is False
    assert a_all['actions'] is True
    assert a_w['pipes'] == ['sw_alpha']
    assert a_w['connectors'] == [True, False, False, False]
    assert a_w['plugins'] == [True, False]
    assert a_w['jobs'] == ['j_alpha']
    # Admin can still see all teams in the switcher to return to All teams
    assert '' == a_w['team_select_options'][0] and 'sw-alpha' in a_w['team_select_options']
    assert a_w['team_select_value'] == 'sw-alpha' and a_w['team_select_acting_class'] is True
    assert a_w['instance_select_options'] == ['sql:swalpha']

    # Admin acting as Everyone
    a_ev = out['admin_everyone']
    assert a_ev['is_admin'] is False and a_ev['acting_team'] == 'Everyone'
    assert a_ev['actions'] is False and a_ev['webterm'] is False
    assert a_ev['pipes'] == ['sw_everyone']
    assert a_ev['connectors'] == [False, False, True, True]
    assert a_ev['jobs'] == ['j_local']

    # Multi-team isolated user (`sw-alpha` manager + `sw-beta` member)
    m_all = out['multi_all']
    assert m_all['team_select_options'] == ['', 'sw-alpha', 'sw-beta']
    assert m_all['pipes'] == ['sw_alpha', 'sw_beta']
    assert m_all['jobs'] == ['j_alpha', 'j_beta']

    m_beta = out['multi_beta']
    assert m_beta['acting_team'] == 'sw-beta' and m_beta['isolated'] is True
    assert m_beta['manages_beta'] is False  # keeps real member role, never escalated
    assert m_beta['default_instance'] == 'sql:swbeta' and m_beta['display_instance'] == 'sql:swbeta'
    assert m_beta['pipes'] == ['sw_beta']
    assert m_beta['connectors'] == [False, True, False, False]
    assert m_beta['plugins'] == [False, True]
    assert m_beta['jobs'] == ['j_beta']
    assert m_beta['instance_select_options'] == ['sql:swbeta']

    # Forged cookies for unauthorized teams / Everyone are safely ignored
    assert out['multi_forged_open']['acting_team'] is None
    assert out['multi_forged_open']['pipes'] == ['sw_alpha', 'sw_beta']
    assert out['multi_forged_everyone']['acting_team'] is None
    assert out['multi_forged_everyone']['pipes'] == ['sw_alpha', 'sw_beta']

    # Single-team isolated user & Everyone-only user do not see the team switcher
    assert out['solo']['team_select_options'] is None
    assert out['plain_user']['team_select_options'] is None


def test_acting_team_fails_closed_when_team_lookup_unavailable(sandbox):
    """
    If team records cannot be loaded while evaluating `mrsm-team`, `apply_acting_team` must fail
    closed (returning the unchanged base viewer) instead of dropping isolation or escalating grants.
    """
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SEED + '''
import os
from meerschaum.api.dash import team_setup

team('fc-alpha', isolated=True, instance='sql:fcalpha', connectors=['sql:fcalpha'], plugins=['noaa'])
team('fc-beta', isolated=True, instance='sql:fcbeta', connectors=['sql:fcbeta'], plugins=['usgs'])
multi = sign_in('fc-multi', role='operator', teams={'fc-alpha': 'member', 'fc-beta': 'member'})
base = access.get_viewer(multi)

orig_cached = access._cached_teams
try:
    access._cached_teams = lambda conn: []
    when_empty = access.apply_acting_team(base, 'fc-beta')
    def _boom(conn):
        raise RuntimeError('db down')
    access._cached_teams = _boom
    when_error = access.apply_acting_team(base, 'fc-beta')
finally:
    access._cached_teams = orig_cached

# Also verify `team_setup.create_team` aborts before `team_database_create` when config is unwritable.
called = {'db_create': False}
orig_access = os.access
orig_core = team_setup._core
real_core = orig_core()
class _FakeCore:
    def __getattr__(self, name):
        return getattr(real_core, name)
    def team_database_create(self, *args, **kwargs):
        called['db_create'] = True
        return True, 'created', {}

try:
    team_setup._core = lambda: _FakeCore()
    os.access = lambda p, mode: False if 'config' in str(p) else orig_access(p, mode)
    ok_unwritable, msg_unwritable = team_setup.create_team(
        'fc-new', isolated=True, storage='database', base='sql:main', conn=conn,
    )
finally:
    os.access = orig_access
    team_setup._core = orig_core

print('JSON:' + json.dumps({
    'empty_acting': when_empty.acting_team,
    'empty_isolated': when_empty.is_isolated,
    'error_acting': when_error.acting_team,
    'error_isolated': when_error.is_isolated,
    'ok_unwritable': ok_unwritable,
    'msg_unwritable': msg_unwritable,
    'db_create_called': called['db_create'],
}))
'''))
    assert out['empty_acting'] is None and out['empty_isolated'] is True
    assert out['error_acting'] is None and out['error_isolated'] is True
    assert out['ok_unwritable'] is False
    assert 'chown -R meerschaum:meerschaum /meerschaum/config' in out['msg_unwritable']
    assert out['db_create_called'] is False
