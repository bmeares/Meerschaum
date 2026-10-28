"""Console SQL authorization uses server parsing, physical aliases and the actual source."""

import json

from tests.api_host._helpers import run_python, sandbox  # noqa: F401


def _json(result):
    assert result.returncode == 0, result.stderr[-7000:]
    return json.loads(next(line[5:] for line in result.stdout.splitlines() if line.startswith('JSON:')))


SETUP = '''
import json
import meerschaum as mrsm
from unittest import mock
from meerschaum.api import get_api_connector
from meerschaum.api.dash import access, sql_guard
conn = get_api_connector()
viewer = access.make_viewer('guard-reader', role='editor', teams={'data': 'member'})
'''


def test_physical_sqlite_targets_and_preview_use_the_shared_guard(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SETUP + '''
from meerschaum.api.dash import pipe_writes
from meerschaum.utils.sql import truncate_item_name
logical = 'hidden_' + 'segment_' * 150
physical = truncate_item_name(logical, 'sqlite')
assert len(logical) > 1024 and len(physical) <= 1024
hidden = mrsm.Pipe('guard:src', 'long-hidden', instance=conn, parameters={
    'target': logical, 'columns': {'id': 'id'},
    'access': {'team': 'ops', 'owner': 'other', 'visibility': 'private'},
})
assert hidden.sync([{'id': 7}])[0]
assert conn.value('SELECT id FROM "' + physical + '"') == 7
candidate = mrsm.Pipe(str(conn), 'new-definition', instance=conn, temporary=True)
metadata = sql_guard.pipe_metadata(hidden)
query = 'SELECT * FROM "' + physical + '"'
out = {
    'physical': physical in metadata['targets'],
    'definition': sql_guard.check_parameters(viewer, candidate, {'fetch': {'definition': query}}),
    'preview': sql_guard.check_query(viewer, query, candidate),
    'target': sql_guard.check_parameters(viewer, candidate, {'target': physical}),
    'config_target': sql_guard.check_parameters(viewer, candidate,
        {'target': 'MRSM{meerschaum:instance}'}),
    'internal': sql_guard.check_parameters(viewer,
        mrsm.Pipe('mrsm', 'users', instance=conn, temporary=True), {}),
    'visible_internal': access.can_see_pipe(viewer, {'target': 'mrsm_users'}),
}
print('JSON:' + json.dumps(out))
assert hidden.delete()[0]
'''))
    assert out['physical'] is True and out['visible_internal'] is False
    assert all(out[key] for key in ('definition', 'preview', 'target', 'config_target', 'internal'))


def test_postgresql_identifier_clipping_is_shared_and_offline(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SETUP + '''
pg = mrsm.get_connector('sql', 'guardoffline', flavor='postgresql', host='127.0.0.1',
                        port=1, database='guard', username='guard', schema='public')
pipe = mrsm.Pipe('guard:src', 'unicode', instance=pg, temporary=True,
                 parameters={'target': 'é' * 70})
metadata = sql_guard.pipe_metadata(pipe)
hidden = sql_guard.HiddenTables(metadata['targets'], metadata['identifier_byte_limit'])
ascii_hidden = sql_guard.HiddenTables(['x' * 63], metadata['identifier_byte_limit'])
print('JSON:' + json.dumps({
    'limit': metadata['identifier_byte_limit'],
    'physical': 'é' * 31 in metadata['targets'],
    'utf8': sql_guard.check_definition('SELECT * FROM "' + 'é' * 31 + 'ê_more"', hidden),
    'ascii': sql_guard.check_definition('SELECT * FROM "' + 'x' * 63 + '_alias"', ascii_hidden),
    'target': sql_guard.check_target('"' + 'x' * 63 + '_alias"', ascii_hidden),
    'plain': sql_guard.check_definition('SELECT 1', hidden),
}))
'''))
    assert out['limit'] == 63 and out['physical'] is True
    assert out['utf8'] and out['ascii'] and out['target']
    assert out['plain'] is None


def test_definitions_check_the_source_and_its_instance_policy(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SETUP + '''
from meerschaum.api import permissions_config
import os
source = mrsm.get_connector('sql', 'guardsource', flavor='sqlite',
                            database=os.path.join(os.environ['MRSM_ROOT_DIR'], 'guard_source.db'))
candidate = mrsm.Pipe(str(source), 'materialized', instance=conn, temporary=True)
secret = mrsm.Pipe('guard:src', 'secret', instance=source, parameters={
    'target': 'source_secret', 'access': {'team': 'ops', 'visibility': 'private'},
})
public = mrsm.Pipe('guard:src', 'public', instance=source, parameters={'target': 'source_public'})
assert secret.sync([{'id': 9}])[0] and public.sync([{'id': 1}])[0]
permissions_config['instances']['allow_multiple_instances'] = True
permissions_config['instances']['allowed_instance_keys'] = ['*']
out = {
    'source_key': sql_guard.pipe_metadata(candidate)['definition_instance_keys'],
    'hidden': sql_guard.check_parameters(viewer, candidate,
        {'fetch': {'definition': 'SELECT * FROM source_secret'}}),
    'preview_hidden': sql_guard.check_query(viewer, 'SELECT * FROM source_secret', candidate),
    'visible': sql_guard.check_query(viewer, 'SELECT * FROM source_public', candidate),
}
permissions_config['instances']['allowed_instance_keys'] = [str(conn)]
out['policy'] = sql_guard.check_query(viewer, 'SELECT 1', candidate)
permissions_config['instances']['allowed_instance_keys'] = ['*']
source.teams = ['ops']
out['teams'] = sql_guard.check_query(viewer, 'SELECT 1', candidate)
print('JSON:' + json.dumps(out))
assert secret.delete()[0] and public.delete()[0]
'''))
    assert out['source_key'] == 'sql:guardsource'
    assert out['hidden'] and out['preview_hidden']
    assert out['visible'] is None
    assert out['policy'] and out['teams']


def test_admin_object_rights_do_not_replace_the_define_sql_scope(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, SETUP + '''
from types import SimpleNamespace
from meerschaum.api.dash import new_pipe, pipe_writes
narrow = SimpleNamespace(no_auth=False, is_admin=True, can=lambda permission: False)
candidate = mrsm.Pipe(str(conn), 'narrow', instance=conn, temporary=True)
parameters = {'fetch': {'definition': 'SELECT 1'}}
print('JSON:' + json.dumps({
    'parameters': sql_guard.check_parameters(narrow, candidate, parameters),
    'preview': new_pipe.preview_refusal(narrow, str(conn), 'SELECT 1', str(conn)),
    'write': pipe_writes._check_sql(narrow, candidate, {}, parameters),
}))
'''))
    assert out['parameters'] and out['preview'] and out['write'][0] is False
