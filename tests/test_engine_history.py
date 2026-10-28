"""Engine history keeps legacy rows while initializing its current storage schema."""

import sqlite3
import json

import pytest

import meerschaum as mrsm


@pytest.mark.parametrize('create', [True, False])
def test_successful_api_write_without_readback_keeps_its_identity_unproven(monkeypatch, create):
    from meerschaum.core.Pipeline import _saved

    class WriteOnlyAPI:
        type = 'api'

        def get_pipe_attributes(self, pipe):
            raise PermissionError('pipes:read is unavailable')

    class Registration:
        instance_keys = 'api:write_only'
        instance_connector = WriteOnlyAPI()

        def get_parameters(self, **kwargs):
            return {'pipeline': {'id': 'cached-client-id'}}

        def register(self):
            writes.append('register')
            return True, 'Registered.'

        def edit(self, **kwargs):
            writes.append('edit')
            return True, 'Edited.'

    writes = []
    monkeypatch.setattr(_saved, 'registration', lambda *args, **kwargs: Registration())
    definition = {'name': 'write-only', 'id': 'cached-client-id', 'targets': []}
    result = _saved._write(definition, 'api:write_only', create=create)
    assert writes == ['register' if create else 'edit']
    assert result['id'].startswith('legacy:')
    assert result['id'] != definition['id']


def test_api_read_uses_the_server_identity_after_a_cached_destination_is_reused(monkeypatch):
    from meerschaum.core.Pipeline import _saved

    class API:
        type = 'api'

        def get_pipe_attributes(self, pipe):
            return {'parameters': {'pipeline': {'id': 'live-moved-id', 'targets': []}}}

    class Registration:
        instance_keys = 'api:reused_destination'
        instance_connector = API()

        def get_id(self):
            return 3

        def get_parameters(self, **kwargs):
            return {'pipeline': {'id': 'deleted-cached-id', 'targets': []}}

    monkeypatch.setattr(_saved, 'registration', lambda *args, **kwargs: Registration())
    assert _saved._read('destination', 'api:reused_destination')['id'] == 'live-moved-id'


def test_initialization_upgrades_legacy_node_columns_without_rewriting_history(tmp_path):
    database = tmp_path / 'legacy-history.db'
    with sqlite3.connect(database) as connection:
        connection.executescript('''
            CREATE TABLE mrsm_engine_nodes (
                started TIMESTAMP, run_id TEXT, pipe_ref TEXT, status TEXT
            );
            INSERT INTO mrsm_engine_nodes VALUES (
                '2026-01-01 00:00:00.125000', 'legacy-run',
                'sql:history_upgrade|fixture|legacy|None', 'succeeded'
            );
        ''')
    conn = mrsm.get_connector('sql', 'history_upgrade', flavor='sqlite', database=str(database))
    mrsm.initialize_engine_metrics(instance=conn)
    with sqlite3.connect(database) as connection:
        columns = {row[1] for row in connection.execute('PRAGMA table_info(mrsm_engine_nodes)')}
        assert {'actual_started', 'registration_id', 'registration_ref'} <= columns
        row = connection.execute('''
            SELECT run_id, status, registration_id, registration_ref FROM mrsm_engine_nodes
        ''').fetchone()
        assert row == ('legacy-run', 'succeeded', None, None)
    mrsm.initialize_engine_metrics(instance=conn)


def test_initialization_adds_optional_run_selection_to_legacy_table(tmp_path):
    database = tmp_path / 'legacy-runs.db'
    with sqlite3.connect(database) as connection:
        connection.executescript('''
            CREATE TABLE mrsm_engine_runs (
                started TIMESTAMP, run_id TEXT, finished TIMESTAMP, success BOOLEAN,
                targets TEXT, workers BIGINT, num_nodes BIGINT, holder TEXT
            );
            INSERT INTO mrsm_engine_runs VALUES (
                '2026-01-01 00:00:00.125000', 'legacy-run',
                '2026-01-01 00:00:00.250000', 1, '[]', 1, 0, 'fixture'
            );
        ''')
    conn = mrsm.get_connector('sql', 'history_runs_upgrade', flavor='sqlite', database=str(database))
    mrsm.initialize_engine_metrics(instance=conn)
    with sqlite3.connect(database) as connection:
        columns = {row[1] for row in connection.execute('PRAGMA table_info(mrsm_engine_runs)')}
        assert {'selection', 'message', 'resumed_from'} <= columns
        row = connection.execute('''
            SELECT run_id, selection, resumed_from FROM mrsm_engine_runs
        ''').fetchone()
        assert row == ('legacy-run', None, None)


def test_saved_pipeline_over_api_keeps_current_nonadmin_history_and_events(tmp_path):
    """Real Python engine writes must retain remote generation authority without stamp JSON."""
    from tests.console.conftest import Server, USERNAME, PASSWORD, run_python
    from tests._binary import binary_or_skip
    binary_or_skip()
    server = Server('history-api', native=False, args=['--no-dash'], config={
        'api': {'permissions': {'actions': {'non_admin': True}}},
    })
    try:
        server.start()
        admin_uri = f'http://{USERNAME}:{PASSWORD}@127.0.0.1:{server.port}'
        writer_uri = f'http://history-writer:history-password@127.0.0.1:{server.port}'
        setup_env = {**server.env, 'MRSM_API_HISTORY_CLIENT': admin_uri}
        setup = run_python(setup_env, '''
import meerschaum as mrsm
from meerschaum.core import User
from meerschaum.api.dash import access
local = mrsm.get_connector('sql:w7console')
writer = User('history-writer', 'history-password', instance=local)
assert local.register_user(writer)[0]
rights = access.role_permissions('editor') | {'jobs:execute', 'jobs:read', 'pipes:delete'}
assert local.set_user_attributes(writer, access.attributes_with_rights({}, 'custom', rights))[0]
remote = mrsm.get_connector('api:history_client')
member = mrsm.Pipe('sql:w7console', 'history-api-member', instance=remote,
    columns={'id': 'id'}, parameters={'fetch': {'definition': 'SELECT 1 AS id'}})
assert member.register()[0]
pipeline = mrsm.Pipeline([member], instance=remote)
assert pipeline.save('history-api-pipeline')[0]
assert pipeline.definition['id'] == mrsm.Pipeline.load('history-api-pipeline', instance=remote).definition['id']
''')
        assert setup.returncode == 0, setup.stderr[-6000:] + server.log()
        client_root = tmp_path / 'client'
        client_root.mkdir()
        client_plugins = client_root / 'plugins'
        client_plugins.mkdir()
        client_env = {
            **server.env,
            'MRSM_ROOT_DIR': str(client_root),
            'MRSM_PLUGINS_DIR': str(client_plugins),
            'MRSM_API_HISTORY_CLIENT': writer_uri,
        }
        current = run_python(client_env, '''
import json
import meerschaum as mrsm
from meerschaum.api.dash import access, engine
remote = mrsm.get_connector('api:history_client')
pipeline = mrsm.Pipeline.load('history-api-pipeline', instance=remote)
plan = pipeline.plan()
assert len(plan.nodes) == 1, {'definition': pipeline.definition, 'plan': plan.raw}
run = pipeline.sync(workers=1)
assert run.success, run.message
from tests._pipeline_diag import why
assert run.run_id and run.inserted == 1, why(run)
assert [row['run']['run_id'] for row in pipeline.get_runs()] == [run.run_id]
assert pipeline.get_run(run.run_id)['summary']['run']['run_id'] == run.run_id
events = run.events()
assert {'run_started', 'node_started', 'run_finished'} <= {event['event'] for event in events}
viewer = access.make_viewer('history-writer', role='editor')
assert engine._run_generations(remote, [run.run_id]) == {run.run_id: True}
rows = engine.get_runs(remote, viewer=viewer)
assert [row['run']['run_id'] for row in rows] == [run.run_id], rows
assert engine.get_run_detail(remote, run.run_id, viewer=viewer) is not None
assert engine.get_run_events(remote, run.run_id, viewer=viewer)
assert 'registration_id' not in json.dumps(rows)
assert 'registration_registry' not in json.dumps(rows)
print(json.dumps({'run_id': run.run_id}))
''')
        assert current.returncode == 0, current.stderr[-6000:] + server.log()
        run_id = json.loads(current.stdout.strip().splitlines()[-1])['run_id']
        # Console host credentials may be administrative; the viewer still receives only
        # freshly authorized history, and the raw canonical proof never becomes page JSON.
        admin_env = {**client_env, 'MRSM_API_HISTORY_CLIENT': admin_uri}
        administrative_transport = run_python(admin_env, f'''
import json
import meerschaum as mrsm
from meerschaum.api.dash import access, engine
remote = mrsm.get_connector('api:history_client')
viewer = access.make_viewer('history-writer', role='editor')
rows = engine.get_runs(remote, viewer=viewer)
assert [row['run']['run_id'] for row in rows] == [{run_id!r}], rows
assert engine.get_run_detail(remote, {run_id!r}, viewer=viewer) is not None
assert engine.get_run_events(remote, {run_id!r}, viewer=viewer)
assert 'registration_registry' not in json.dumps(rows)
''')
        assert administrative_transport.returncode == 0, (
            administrative_transport.stderr[-6000:] + server.log()
        )
        server_execution = run_python(client_env, '''
import json, time
import meerschaum as mrsm
remote = mrsm.get_connector('api:history_client')
response = remote.post('/pipelines/history-api-pipeline/plan', json={})
assert response.ok, response.text
planned = response.json()
assert len(planned['plan']['nodes']) == 1, planned
response = remote.post('/pipelines/history-api-pipeline/sync', json={})
assert response.ok, response.text
started = response.json()
run_id = started['run_id']
pipeline = mrsm.Pipeline.load('history-api-pipeline', instance=remote)
deadline = time.monotonic() + 60
detail = None
while time.monotonic() < deadline:
    detail = pipeline.get_run(run_id)
    if detail and detail['summary']['status'] != 'running':
        break
    time.sleep(0.2)
assert detail and detail['summary']['status'] == 'succeeded', detail
assert len(detail['nodes']) == 1 and detail['nodes'][0]['status'] == 'succeeded', detail
assert any(event['event'] == 'run_finished' for event in pipeline.get_events(run_id))
print(json.dumps({'run_id': run_id}))
''')
        assert server_execution.returncode == 0, server_execution.stderr[-6000:] + server.log()
        server_run_id = json.loads(server_execution.stdout.strip().splitlines()[-1])['run_id']
        lifecycle = run_python(client_env, f'''
import json
import meerschaum as mrsm
from meerschaum.api.dash import access, engine, overview
remote = mrsm.get_connector('api:history_client')
pipeline = mrsm.Pipeline.load('history-api-pipeline', instance=remote)
identity = pipeline.definition['id']
assert pipeline.save(description='Updated through the API')[0]
assert pipeline.definition['id'] == identity
assert pipeline.apply_analysis()[0]
assert pipeline.rename('history-api-renamed')[0]
assert pipeline.definition['id'] == identity
assert mrsm.Pipeline.load('history-api-renamed', instance=remote).definition['id'] == identity
expected = {{{run_id!r}, {server_run_id!r}}}
assert {{row['run']['run_id'] for row in pipeline.get_runs()}} == expected
viewer = access.make_viewer('history-writer', role='editor')
rows = engine.get_runs(remote, viewer=viewer)
assert {{row['run']['run_id'] for row in rows}} == expected
definitions = access.visible_pipelines(remote, viewer=viewer, include_members=False)
assert all(overview.run_pipeline_name(row, definitions) == 'history-api-renamed' for row in rows)
assert 'registration_registry' not in json.dumps(rows)
member = mrsm.Pipe('sql:w7console', 'history-api-member', instance=remote)
parameters = member.get_parameters(apply_symlinks=False, refresh=True)
assert 'pipeline-history-api-pipeline' not in parameters.get('tags', [])
''')
        assert lifecycle.returncode == 0, lifecycle.stderr[-6000:] + server.log()
        legacy_ownership = run_python(setup_env, '''
import meerschaum as mrsm
from meerschaum.core.Pipeline._saved import registration
remote = mrsm.get_connector('api:history_client')
member = mrsm.Pipe('sql:w7console', 'history-api-member', instance=remote)
pipeline = mrsm.Pipeline([member], instance=remote)
assert pipeline.save('legacy-owned-history')[0]
local = mrsm.get_connector('sql:w7console')
legacy = registration('legacy-owned-history', local)
parameters = dict(legacy.get_parameters(apply_symlinks=False, refresh=True))
tag = 'pipeline-legacy-owned-history'
obsolete_tag = tag + '-obsolete'
old_id = 'legacy:owned-history'
ref = f'{remote}|sql:w7console|history-api-member|None'
parameters['pipeline']['id'] = old_id
parameters['pipeline']['tag_associations'] = [
    {'pipe_ref': ref, 'tag': tag}, {'pipe_ref': ref, 'tag': obsolete_tag},
]
legacy.parameters = parameters
assert legacy.edit(interactive=False)[0]
parameters = dict(member.get_parameters(apply_symlinks=False, refresh=True))
parameters['tags'] = list(parameters.get('tags', [])) + [tag, obsolete_tag]
parameters.setdefault('_pipeline_tags', {})[tag] = {'owners': [old_id], 'preexisting': False}
parameters['_pipeline_tags'][obsolete_tag] = {'owners': [old_id], 'preexisting': False}
member.parameters = parameters
assert member.edit(interactive=False)[0]
pipeline = mrsm.Pipeline.load('legacy-owned-history', instance=remote)
assert pipeline.apply_analysis()[0]
identity = pipeline.definition['id']
assert identity and not identity.startswith('legacy:')
assert identity == mrsm.Pipeline.load('legacy-owned-history', instance=remote).definition['id']
assert pipeline.definition['tag_associations'] == [{'pipe_ref': ref, 'tag': tag}]
# This object still carries the explicit local legacy parameters assigned above.
# Read the persisted server metadata, without those intentional call-site overrides.
parameters = remote.get_pipe_attributes(member)['parameters']
assert tag in parameters['tags']
assert obsolete_tag not in parameters['tags']
assert parameters['_pipeline_tags'][tag]['owners'] == [identity]
''')
        assert legacy_ownership.returncode == 0, legacy_ownership.stderr[-6000:] + server.log()
    finally:
        server.stop()
