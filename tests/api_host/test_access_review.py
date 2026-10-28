"""Console access stays closed on incomplete reads and invalid creation requests."""

import json

from tests.api_host._helpers import SIGN_IN, run_python, sandbox  # noqa: F401


def _json(result):
    assert result.returncode == 0, result.stderr[-6000:]
    return json.loads(next(line[5:] for line in result.stdout.splitlines() if line.startswith('JSON:')))


def test_unreadable_access_and_missing_pipeline_snapshots_are_not_public(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from unittest import mock
from meerschaum.api.dash import access
viewer = access.make_viewer('reviewer', role='editor')
class Unreadable:
    parameters = {}  # A stale cache must not replace an unreadable registration.
    def get_parameters(self, **kw):
        raise RuntimeError('storage unavailable')
with mock.patch.object(access, '_pipeline_registry', return_value=[]):
    definitions = access.visible_pipelines('sql:apihosttest', viewer=viewer,
        definitions={'private-pipeline': {'targets': []}})
print('JSON:' + json.dumps({
    'read': access.can_see_pipe(viewer, Unreadable()),
    'write': access.can_edit_pipe(viewer, Unreadable()),
    'manage': access.can_manage_access(viewer, Unreadable()),
    'missing': access.can_edit_pipe(viewer, None),
    'legacy': access.can_see_pipe(viewer, {}),
    'pipelines': definitions,
    'explicit_empty': access.user_rights(None, {'scopes': []})['explicit'],
}))
'''))
    assert not any(out[key] for key in ('read', 'write', 'manage', 'missing'))
    assert out['legacy'] and out['explicit_empty']
    assert out['pipelines'] == {}


def test_new_pipeline_validates_its_team_before_writing_and_starts_with_access(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SIGN_IN + '''
import json
from copy import deepcopy
import meerschaum as mrsm
from dash._callback_context import context_value
from dash._utils import AttributeDict
from meerschaum.api import get_api_connector
from meerschaum.api.dash import build_dash_app, access
from meerschaum.core.Pipeline._saved import registration, list_pipelines
build_dash_app()
from meerschaum.api.dash.callbacks.pipeline_pages import create_pipeline
conn = get_api_connector()
session = sign_in('review-owner', teams={'review-team': 'member'})
member = mrsm.Pipe('review:src', 'member', instance=conn)
assert member.register()[0]
draft = {'name': 'review-invalid', 'members': [access._ref(str(conn), 'review:src', 'member', None)]}
context_value.set(AttributeDict(triggered_inputs=[{'prop_id': 'pipeline-new-create.n_clicks', 'value': 1}]))
create_pipeline(1, draft, False, {'registry': str(conn)}, {}, session, 'another-team', 'private')
invalid_written = 'review-invalid' in list_pipelines(conn)
captured = []
original = mrsm.Pipe.register
def record(pipe, *args, **kw):
    if pipe.connector_keys == 'mrsm' and pipe.metric_key == 'pipeline':
        captured.append(deepcopy(dict(pipe.parameters)))
    return original(pipe, *args, **kw)
mrsm.Pipe.register = record
draft['name'] = 'review-owned'
create_pipeline(1, draft, False, {'registry': str(conn)}, {}, session, 'review-team', 'private')
parameters = registration('review-owned', conn).get_parameters(apply_symlinks=False, refresh=True)
print('JSON:' + json.dumps({'invalid_written': invalid_written, 'first': captured,
                          'stored': dict(parameters)}, default=str))
'''))
    assert out['invalid_written'] is False
    assert len(out['first']) == 1
    expected = {'owner': 'review-owner', 'team': 'review-team', 'visibility': 'private', 'edit': 'team'}
    assert out['first'][0]['access'] == expected == out['stored']['access']


def test_run_redaction_covers_mapping_keys_and_escaped_pipe_labels(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from unittest import mock
from meerschaum.api.dash import access, engine
viewer = access.make_viewer('reviewer', role='viewer')
visible = 'sql:apihosttest|source:local|public|None'
hidden = 'sql:apihosttest|source:local|secret|None'
with mock.patch.object(access, 'visible_refs', return_value={visible}):
    redactor = engine.Anonymizer(viewer, 'sql:apihosttest')
    payload = redactor.value({hidden: {'pipe_ref': hidden}, 'visible': visible})
    label = redactor.text('Failed Pipe(' + repr('source:local') + ', ' + repr("secret's (details)") + ')')
    empty = redactor.run({'run': {'targets': []}})
    only_hidden = redactor.run({'run': {'targets': [hidden]}})
    mixed = redactor.run({'run': {'targets': [visible, hidden]}})
print('JSON:' + json.dumps({'payload': payload, 'label': label, 'empty': empty,
                          'only_hidden': only_hidden, 'mixed': mixed}))
'''))
    assert 'secret' not in json.dumps(out['payload'])
    assert out['payload']['visible'] == 'sql:apihosttest|source:local|public|None'
    assert out['label'] == 'Failed a hidden pipe'
    assert out['empty'] is None and out['only_hidden'] is None
    assert out['mixed']['run']['targets'] == ['sql:apihosttest|source:local|public|None', '__hidden__:1']


def test_registration_uses_the_server_lock_and_never_falls_back_on_failure(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import io, json
import urllib.error
from urllib.parse import parse_qs
from unittest import mock
from meerschaum.api import get_api_connector
from meerschaum.api.dash import registration, execute
requests = []
def refused(request, **kwargs):
    requests.append({'url': request.full_url, 'form': parse_qs(request.data.decode())})
    return io.BytesIO(json.dumps([False, 'Registration is closed.']).encode())
with mock.patch.object(execute, 'server_url', return_value='http://127.0.0.1:19997'), \
     mock.patch.object(registration, '_users', return_value=[]):
    with mock.patch('urllib.request.urlopen', side_effect=refused):
        first = registration.create_first_admin('review-first', 'correct-horse-9', 'correct-horse-9')
        with mock.patch.object(registration, 'first_run', return_value=False), \
             mock.patch.object(registration, 'self_registration_open', return_value=True):
            visitor = registration.register_account('review-visitor', 'correct-horse-9', email='visitor@example.com')
    with mock.patch('urllib.request.urlopen', side_effect=urllib.error.URLError('offline')):
        unavailable = registration.create_first_admin('review-offline', 'correct-horse-9', 'correct-horse-9')
print('JSON:' + json.dumps({'requests': requests, 'first': first, 'visitor': visitor,
                          'unavailable': unavailable, 'users': get_api_connector().get_users()}))
'''))
    assert out['first'] == [False, 'Registration is closed.']
    assert out['visitor'] == [[False, 'Registration is closed.'], False]
    assert out['unavailable'][0] is False and 'offline' in out['unavailable'][1]
    assert not ({'review-first', 'review-visitor', 'review-offline'} & set(out['users']))
    assert len(out['requests']) == 2
    assert all(item['url'].endswith('/users/register') for item in out['requests'])
    assert all('type' not in item['form'] for item in out['requests'])
    assert out['requests'][1]['form']['email'] == ['visitor@example.com']


def test_everyone_can_be_the_explicit_primary_team_of_a_team_member(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SIGN_IN + '''
import json
from meerschaum.api.dash import access, users
session = sign_in('review-primary', teams={'review-team': 'member'})
assert users.set_user_rights('review-primary', 'editor', primary_team='Everyone')[0]
access.forget()
viewer = access.get_viewer(session)
row = next(row for row in users.get_user_rows() if row['username'] == 'review-primary')
print('JSON:' + json.dumps({'viewer': viewer.primary_team, 'row': row['primary_team']}))
'''))
    assert out == {'viewer': 'Everyone', 'row': 'Everyone'}


def test_everyone_managers_keep_their_management_rights_in_the_principal(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, '''
import json
from meerschaum.api.dash import access
manager = access.make_viewer('manager', teams={'Everyone': 'manager'})
member = access.make_viewer('member')
private = {'access': {'owner': 'owner', 'visibility': 'private'}}
print('JSON:' + json.dumps({
    'manager': [access.can_see_pipe(manager, private), access.can_manage_access(manager, private),
                access.can_see_email(manager, 'another-user', [])],
    'member': [access.can_see_pipe(member, private), access.can_manage_access(member, private),
               access.can_see_email(member, 'another-user', [])],
}))
'''))
    assert out == {'manager': [True, True, True], 'member': [False, False, False]}
