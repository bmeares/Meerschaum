"""Saved Pipeline identities in history follow the current registration's access."""

from tests.api_host._helpers import run_python, sandbox  # noqa: F401


def test_public_member_does_not_expose_private_or_recreated_pipeline_history(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
import json
import meerschaum as mrsm
from meerschaum.api.dash import access, engine
conn = mrsm.get_connector('sql:apihosttest')
member = mrsm.Pipe('fixture', 'public-history-member', instance=conn,
                   parameters={'access': {'visibility': 'everyone'}})
assert member.register()[0]
ref = f'{conn}|fixture|public-history-member|None'
registration = mrsm.Pipe('mrsm', 'pipeline', 'restricted-history', instance=conn,
    parameters={'access': {'visibility': 'private', 'owner': 'bob'},
                'pipeline': {'id': 'original-definition', 'targets': [ref]}})
assert registration.register()[0]
summary = {'run': {'run_id': 'restricted-history-fixture', 'targets': [ref],
    'selection': json.dumps({'pipeline_definitions': [{'name': 'restricted-history',
        'id': 'original-definition', 'registry': str(conn)}]})},
    'status': 'succeeded', 'duration_seconds': 1, 'counts': {'succeeded': 1}}
detail = {'summary': summary, 'nodes': [{'pipe_ref': ref}], 'bars': []}
captured_generations = {'restricted-history-fixture': True}
engine._call = lambda name, request: (
    {run_id: captured_generations.get(run_id, False) for run_id in request['run_ids']}
    if name == 'engine_run_generations' else detail if name == 'engine_run_detail' else [summary]
)
alice = access.make_viewer('alice', role='viewer')
bob = access.make_viewer('bob', role='viewer')
assert engine.get_runs(conn, viewer=alice) == [], 'Private Pipeline identity leaked via public member'
assert engine.get_run_detail(conn, 'restricted-history-fixture', viewer=alice) is None
assert engine.get_run_events(conn, 'restricted-history-fixture', viewer=alice) == []
assert len(engine.get_runs(conn, viewer=bob)) == 1
# A fresh request sees both a grant and a subsequent revocation for the same viewer.
registration.parameters['access'] = {'visibility': 'everyone'}
assert registration.edit(interactive=False)[0]
assert len(engine.get_runs(conn, viewer=alice)) == 1
registration.parameters['access'] = {'visibility': 'private', 'owner': 'bob'}
assert registration.edit(interactive=False)[0]
assert engine.get_runs(conn, viewer=alice) == []
# Reusing a public name must not attach an old private definition to the new registration.
registration.parameters['pipeline']['id'] = 'replacement-definition'
registration.parameters['access'] = {'visibility': 'everyone'}
assert registration.edit(interactive=False)[0]
assert engine.get_runs(conn, viewer=alice) == []
assert engine.get_run_detail(conn, 'restricted-history-fixture', viewer=alice) is None
''')
    assert result.returncode == 0, result.stderr[-6000:]


def test_history_uses_only_explicit_canonical_registry_proof_and_reads_it_once(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
import copy, json
import meerschaum as mrsm
from meerschaum.api.dash import access, engine, overview
conn = mrsm.get_connector('sql:apihosttest')
member = mrsm.Pipe('fixture', 'alias-history-member', instance=conn)
assert member.register()[0]
ref = f'{conn}|fixture|alias-history-member|None'
registration = mrsm.Pipe('mrsm', 'pipeline', 'alias-history', instance=conn,
    parameters={'pipeline': {'id': 'alias-definition', 'targets': [ref]}})
assert registration.register()[0]
snapshot = {'name': 'alias-history', 'id': 'alias-definition', 'registry': 'api:client',
            'registration_registry': str(conn)}
summary = {'run': {'run_id': 'alias-history-fixture', 'targets': [ref],
                  'selection': json.dumps({'pipeline_definitions': [snapshot]})},
           'status': 'succeeded', 'counts': {'succeeded': 1}}
captured_generations = {'alias-history-fixture': True}
engine._call = lambda name, request: (
    {run_id: captured_generations.get(run_id, False) for run_id in request['run_ids']}
    if name == 'engine_run_generations' else [copy.deepcopy(summary), copy.deepcopy(summary)]
)
alice = access.make_viewer('alice', role='viewer')
read = access.visible_pipelines
calls = []
def snapshot_read(*a, **kw):
    calls.append(1)
    assert kw.get('include_members') is False
    return read(*a, **kw)
access.visible_pipelines = snapshot_read
rows = engine.get_runs(conn, viewer=alice)
assert len(rows) == 2 and len(calls) == 1, (rows, calls)
assert json.loads(rows[0]['run']['selection'])['pipeline_definitions'][0]['registry'] == 'api:client'
assert 'registration_registry' not in json.dumps(rows)
assert '_snapshot_registries' not in json.dumps(rows)
definitions = read(conn, viewer=alice, include_members=False)
assert overview.run_pipeline_name(rows[0], definitions) == 'alias-history'
# Losing the private carrier cannot create authority from the original API alias.
assert overview.run_pipeline_name(dict(rows[0]), definitions) is None
assert overview.run_pipeline_name(json.loads(json.dumps(rows[0])), definitions) is None
assert engine.Anonymizer(alice, conn).run(rows[0]) is None
admin = access.make_viewer('admin', admin=True)
admin_rows = engine.get_runs(conn, viewer=admin)
assert 'registration_registry' not in json.dumps(admin_rows)
assert overview.run_pipeline_name(admin_rows[0], definitions) == 'alias-history'
anonymizer = engine.Anonymizer(admin, conn)
admin_detail = anonymizer.detail({'summary': summary, 'nodes': []})
assert 'registration_registry' not in json.dumps(admin_detail)
assert overview.run_pipeline_name(admin_detail['summary'], definitions) == 'alias-history'
assert 'registration_registry' not in json.dumps(anonymizer.events([{'selection': summary['run']['selection']}]))
# A bare API alias is never inferred to mean the served instance.
snapshot.pop('registration_registry')
summary['run']['selection'] = json.dumps({'pipeline_definitions': [snapshot]})
assert engine.get_runs(conn, viewer=alice) == []
snapshot['registration_registry'] = 'sql:unrelated'
summary['run']['selection'] = json.dumps({'pipeline_definitions': [snapshot]})
assert engine.get_runs(conn, viewer=alice) == []
# Checking any matching same-name snapshot is insufficient: every saved identity must match.
snapshot['registration_registry'] = str(conn)
old = {**snapshot, 'id': 'old-private-definition'}
summary['run']['selection'] = json.dumps({'pipeline_definitions': [snapshot, old]})
assert engine.get_runs(conn, viewer=alice) == []
''')
    assert result.returncode == 0, result.stderr[-6000:]


def test_name_derived_legacy_pipeline_identity_is_not_history_authority(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
import json
import meerschaum as mrsm
from meerschaum.api.dash import access, engine
conn = mrsm.get_connector('sql:apihosttest')
member = mrsm.Pipe('fixture', 'legacy-pipeline-member', instance=conn)
assert member.register()[0]
ref = f'{conn}|fixture|legacy-pipeline-member|None'
registration = mrsm.Pipe('mrsm', 'pipeline', 'legacy-identity', instance=conn,
    parameters={'pipeline': {'targets': [ref]}})
assert registration.register()[0]
run = {'run': {'run_id': 'legacy-identity-fixture', 'targets': [ref],
    'selection': json.dumps({'pipeline_definitions': [{'name': 'legacy-identity',
        'id': f'legacy:{conn}:legacy-identity', 'registry': str(conn)}]})}, 'status': 'succeeded'}
captured_generations = {'legacy-identity-fixture': True}
engine._call = lambda name, request: captured_generations if name == 'engine_run_generations' else [run]
assert engine.get_runs(conn, viewer=access.make_viewer('alice', role='viewer')) == []
assert len(engine.get_runs(conn, viewer=access.make_viewer('admin', admin=True))) == 1
# Persisting the same predictable normalization string cannot turn it into continuity proof.
registration.parameters['pipeline']['id'] = f'legacy:{conn}:legacy-identity'
assert registration.edit(interactive=False)[0]
assert engine.get_runs(conn, viewer=access.make_viewer('alice', role='viewer')) == []
assert len(engine.get_runs(conn, viewer=access.make_viewer('admin', admin=True))) == 1
''')
    assert result.returncode == 0, result.stderr[-6000:]


def test_recreated_public_pipe_cannot_inherit_retained_private_history(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
import json, sqlite3
from pathlib import Path
import meerschaum as mrsm
from meerschaum import _core
from meerschaum.api.dash import access, engine
from meerschaum.config import paths
conn = mrsm.get_connector('sql:apihosttest')
private = mrsm.Pipe('fixture', 'retained-history-member', instance=conn,
    columns={'id': 'id'}, parameters={'access': {'visibility': 'private', 'owner': 'bob'}})
assert private.register()[0]
old_id = private.get_id()
assert private.sync([{'id': 1}])[0]
_core.flush_sync_statuses(timeout=10)
mrsm.initialize_engine_metrics(instance=conn)
ref = f'{conn}|fixture|retained-history-member|None'
database = Path(paths.ROOT_DIR_PATH) / 'instance.db'
def captured_stamp():
    with sqlite3.connect(database) as db:
        return db.execute("SELECT registration_id FROM mrsm_engine_metrics WHERE metric_key='retained-history-member' ORDER BY ts_us DESC LIMIT 1").fetchone()[0]
def persist(run_id, stamp, message):
    # Evidence belongs to the actual native outcome just captured, never to a later read.
    with sqlite3.connect(database) as db:
        db.execute('INSERT INTO mrsm_engine_runs (run_id,started,finished,success,message,targets,workers,num_nodes,holder) VALUES (?,?,?,?,?,?,?,?,?)',
            (run_id,'2026-01-01 00:00:00.000000','2026-01-01 00:00:01.000000',0,message,json.dumps([ref]),1,1,'fixture'))
        db.execute('INSERT INTO mrsm_engine_nodes (run_id,pipe_ref,action,status,started,actual_started,finished,message,registration_id,registration_ref) VALUES (?,?,?,?,?,?,?,?,?,?)',
            (run_id,ref,'incremental','failed','2026-01-01 00:00:00.000000','2026-01-01 00:00:00.000000','2026-01-01 00:00:01.000000',message,stamp,ref))
old_stamp = captured_stamp()
assert old_stamp
persist('retained-private-history', old_stamp, 'private retained failure')
alice = access.make_viewer('alice', role='viewer')
bob = access.make_viewer('bob', role='viewer')
assert engine.get_runs(conn, viewer=alice) == []
assert 'private retained failure' in json.dumps(engine.get_runs(conn, viewer=bob))
assert private.delete()[0]
public = mrsm.Pipe('fixture', 'retained-history-member', instance=conn, columns={'id': 'id'})
assert public.register()[0]
assert public.get_id() == old_id, 'The SQLite fixture must exercise reused numeric IDs'
assert engine.get_runs(conn, viewer=alice) == []
assert engine.get_run_detail(conn, 'retained-private-history', viewer=alice) is None
assert engine.get_run_events(conn, 'retained-private-history', viewer=alice) == []
admin = access.make_viewer('admin', admin=True)
assert 'private retained failure' in json.dumps(engine.get_run_detail(conn, 'retained-private-history', viewer=admin))
assert public.sync([{'id': 2}])[0]
_core.flush_sync_statuses(timeout=10)
new_stamp = captured_stamp()
assert new_stamp and new_stamp != old_stamp
persist('current-public-history', new_stamp, 'current public failure')
visible = engine.get_runs(conn, viewer=alice)
assert [row['run']['run_id'] for row in visible] == ['current-public-history'], visible
assert 'current public failure' in json.dumps(engine.get_run_detail(conn, 'current-public-history', viewer=alice))
assert 'private retained failure' not in json.dumps(visible)
''')
    assert result.returncode == 0, result.stderr[-6000:]


def test_current_mixed_history_keeps_hidden_node_payloads_private(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
import json
from unittest import mock
from meerschaum.api.dash import access, engine
visible = 'sql:apihosttest|fixture|public-message-member|None'
hidden = 'sql:apihosttest|fixture|private-message-member|None'
summary = {'run': {'run_id': 'current-mixed-fixture', 'targets': [visible, hidden],
                   'message': 'private aggregate failure'}, 'status': 'failed'}
captured_generations = {'current-mixed-fixture': True}
with mock.patch.object(access, 'visible_refs', return_value={visible}), mock.patch.object(
    engine, '_run_generations', return_value=captured_generations,
):
    guard = engine.Anonymizer(access.make_viewer('reader', role='viewer'), 'sql:apihosttest')
    detail = guard.detail({'summary': summary, 'nodes': [
        {'pipe_ref': visible, 'message': 'public failure', 'status': 'failed'},
        {'pipe_ref': hidden, 'message': 'private node failure', 'reason': 'private SQL',
         'bounds': {'begin': 'private bound'}, 'status': 'failed'},
    ], 'bars': [{'pipe_ref': hidden, 'message': 'private bar failure', 'status': 'failed'}]})
    assert detail is not None
    assert detail['nodes'][0]['message'] == 'public failure'
    assert detail['nodes'][1]['pipe_ref'].startswith('__hidden__:')
    assert detail['nodes'][1]['status'] == 'failed'
    events = guard.events([
        {'event': 'node_failed', 'pipe_ref': hidden, 'payload': {'error': 'private event failure'}},
        {'event': 'run_finished', 'payload': {'message': 'private run failure'}},
        {'event': 'node_failed', 'pipe_ref': visible, 'payload': {'error': 'public event failure'}},
    ])
    public = json.dumps([detail, events])
    assert 'private' not in public, public
    assert 'public event failure' in public
''')
    assert result.returncode == 0, result.stderr[-6000:]


def test_authorized_rename_keeps_history_on_its_live_uuid_and_current_acl(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
import json
import meerschaum as mrsm
from meerschaum.api.dash import access, engine, overview
from meerschaum.core.Pipeline._saved import registration
conn = mrsm.get_connector('sql:apihosttest')
member = mrsm.Pipe('fixture', 'rename-history-member', instance=conn)
assert member.register()[0]
ref = f'{conn}|fixture|rename-history-member|None'
saved = registration('original-private-name', conn)
saved.parameters = {'access': {'owner': 'bob', 'visibility': 'private'},
    'pipeline': {'id': 'stable-rename-identity', 'targets': [ref]}}
assert saved.register()[0]
summary = {'run': {'run_id': 'original-private-name-fixture', 'targets': [ref],
    'selection': json.dumps({'pipeline_definitions': [{'name': 'original-private-name',
        'id': 'stable-rename-identity', 'registry': str(conn)}]})}, 'status': 'succeeded'}
captured_generations = {'original-private-name-fixture': True}
engine._call = lambda name, request: captured_generations if name == 'engine_run_generations' else [summary]
bob = access.make_viewer('bob', role='viewer')
alice = access.make_viewer('alice', role='viewer')
pipeline = mrsm.Pipeline.load('original-private-name', instance=conn)
assert pipeline.rename('renamed-private-name')[0]
assert pipeline.definition['id'] == 'stable-rename-identity'
rows = engine.get_runs(conn, viewer=bob)
assert len(rows) == 1
definitions = access.visible_pipelines(conn, viewer=bob, include_members=False)
assert overview.run_pipeline_name(rows[0], definitions) == 'renamed-private-name'
assert json.loads(rows[0]['run']['selection'])['pipeline_definitions'][0]['name'] == 'original-private-name'
assert engine.get_runs(conn, viewer=alice) == []
# A duplicate raw UUID cannot provide an alternate public authority for private history.
duplicate = registration('duplicate-public-name', conn)
duplicate.parameters = {'pipeline': {'id': 'stable-rename-identity', 'targets': [ref]}}
assert duplicate.register()[0]
assert engine.get_runs(conn, viewer=alice) == []
assert engine.get_runs(conn, viewer=bob) == []
''')
    assert result.returncode == 0, result.stderr[-6000:]


def test_api_history_uses_one_fresh_canonical_map_for_client_and_server_origins(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
import copy, json
from unittest import mock
from meerschaum.api.dash import access, engine, overview
api_ref = 'api:canonical-home|fixture|member|None'
server_ref = 'sql:declared-server|fixture|member|None'
parameters = {'access': {'visibility': 'everyone'}, 'pipeline': {
    'id': 'current-api-identity', 'name': 'renamed', 'registry': 'sql:declared-server',
    'targets': [server_ref],
}}
duplicates = {}
class Response:
    ok = True
    def json(self):
        return {'success': True, 'keys': {
            '1': ['mrsm', 'pipeline', 'renamed', copy.deepcopy(parameters)], **duplicates,
        }}
class Home:
    instance_keys = None
    calls = 0
    def __str__(self): return 'api:canonical-home'
    def get(self, path, params):
        assert path == '/pipelines/keys' and params == {'as_dict': True}
        self.calls += 1
        return Response()
home = Home()
viewer = access.make_viewer('alice', role='viewer')
def visible_subset(viewer, refs):
    assert set(refs) == {api_ref}, refs
    return {api_ref}
def summary(run_id, registry, proof=None):
    snapshot = {'name': 'historical-name', 'id': 'current-api-identity', 'registry': registry}
    if proof is not None: snapshot['registration_registry'] = proof
    return {'run': {'run_id': run_id, 'targets': [server_ref if registry.startswith('sql:') else api_ref],
                    'selection': json.dumps({'pipeline_definitions': [snapshot]})}, 'status': 'succeeded'}
captured_generations = {'client-fixture': True, 'server-fixture': True, 'foreign-fixture': True}
with mock.patch.object(access, 'visible_ref_subset', side_effect=visible_subset), mock.patch.object(
    engine, '_run_generations', return_value=captured_generations,
):
    guard = engine.Anonymizer(viewer, home, refs=[api_ref, server_ref])
    guard.check_generations(list(captured_generations))
    client = guard.run(summary('client-fixture', 'api:canonical-home', 'sql:declared-server'))
    server = guard.run(summary('server-fixture', 'sql:declared-server'))
    assert client is not None and server is not None
    assert home.calls == 1, home.calls
    definitions = guard.pipeline_definitions
    assert overview.run_pipeline_name(client, definitions) == 'renamed'
    assert overview.run_pipeline_name(server, definitions) == 'renamed'
    assert overview.run_pipeline_name(dict(server), definitions) is None
    assert overview.run_pipeline_name(json.loads(json.dumps(server)), definitions) is None
    assert 'registration_registry' not in json.dumps([client, server])
    assert '_snapshot_registries' not in json.dumps([client, server])
    assert json.loads(server['run']['selection'])['pipeline_definitions'][0]['registry'] == 'sql:declared-server'
    assert guard.run(summary('foreign-fixture', 'api:unrelated')) is None
    assert guard.run(summary('foreign-fixture', 'api:canonical-home', 'sql:unrelated')) is None
    assert guard.run(summary('foreign-fixture', 'sql:unrelated')) is None
    parameters['access'] = {'visibility': 'private', 'owner': 'bob'}
    revoked = engine.Anonymizer(viewer, home, refs=[api_ref, server_ref])
    assert revoked.run(summary('server-fixture', 'sql:declared-server')) is None
    assert home.calls == 2, home.calls
    parameters['access'] = {'visibility': 'everyone'}
    captured_generations['server-fixture'] = False
    unproven = engine.Anonymizer(viewer, home, refs=[api_ref, server_ref])
    assert unproven.run(summary('server-fixture', 'sql:declared-server')) is None
    captured_generations['server-fixture'] = True
    duplicates['2'] = ['mrsm', 'pipeline', 'duplicate', copy.deepcopy(parameters)]
    ambiguous = engine.Anonymizer(viewer, home, refs=[api_ref, server_ref])
    assert ambiguous.run(summary('server-fixture', 'sql:declared-server')) is None
''')
    assert result.returncode == 0, result.stderr[-6000:]
