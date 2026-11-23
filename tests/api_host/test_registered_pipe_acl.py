"""List-shaped instance key results never substitute empty extras for fresh Pipe access."""

from tests.api_host._helpers import run_python


def test_list_key_extras_refresh_private_parameters_and_current_revocation(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
from unittest.mock import patch
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.dash import access
c = get_api_connector()
public = mrsm.Pipe('acl-list','public',instance=c)
private = mrsm.Pipe('acl-list','private',instance=c,
    parameters={'access':{'visibility':'private','owner':'another-user'}})
assert public.register()[0] and private.register()[0]
who = access.make_viewer('reader',role='viewer')
keys = [('acl-list','public',None,{}),('acl-list','private',None,{})]
calls = []
original = mrsm.Pipe.get_parameters
def refreshed(pipe, apply_symlinks=True, refresh=False, **kwargs):
    calls.append((pipe.metric_key,refresh))
    return original(pipe,apply_symlinks=apply_symlinks,refresh=refresh,**kwargs)
def visible():
    return {mk for ck,mk,lk,parameters in access.registered_pipes(c)
            if access.can_see_pipe(who,parameters,instance=c)}
with patch('meerschaum.utils.fetch_pipes_keys',return_value=keys), \
     patch.object(mrsm.Pipe,'get_parameters',new=refreshed):
    assert visible() == {'public'}
    assert calls and all(refresh for _,refresh in calls),calls
    # The same viewer immediately loses a Pipe whose current metadata became private.
    public.parameters={'access':{'visibility':'private','owner':'another-user'}}
    assert public.edit()[0]
    assert visible() == set()
    # A JSON-shaped or stale public extra is not an authoritative access snapshot either.
    keys[0]=('acl-list','public',None,'{}')
    keys[1]=('acl-list','private',None,{'access':{'visibility':'public'}})
    assert visible() == set()
    # A deleted registration remaining in a key list cannot expose its retained private history.
    assert private.delete()[0]
    assert visible() == set()

# SQL dictionary snapshots retain their bulk behavior, including genuinely empty public parameters.
snapshot = {1:('acl-list','public',None,{}),
            2:('acl-list','private',None,{'access':{'visibility':'private','owner':'another-user'}})}
with patch('meerschaum.utils.fetch_pipes_keys',return_value=snapshot), \
     patch.object(mrsm.Pipe,'get_parameters',side_effect=AssertionError('SQL snapshot was reloaded.')):
    assert visible() == {'public'}

# The facade may report an attribute-read failure while returning its empty local parameters.
class Unreadable:
    def get_parameters(self,**kwargs):
        assert kwargs == {'apply_symlinks':False,'refresh':True}
        self._attributes_load_failures=1
        return {}
with patch('meerschaum.utils.fetch_pipes_keys',return_value=keys), \
     patch.object(mrsm,'Pipe',side_effect=lambda *a,**k:Unreadable()):
    assert visible() == set()
''')
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]


def test_reserved_lists_and_direct_detail_use_current_metadata(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, '''
from unittest.mock import patch
import meerschaum as mrsm
from meerschaum.config import get_config
from meerschaum.api import get_api_connector
from meerschaum.api.dash import access, engine, pipes_table
c = get_api_connector()
get_config('meerschaum', 'connectors', c.type, c.label)['teams'] = ['Wet', None]
who = access.make_viewer('reader', role='viewer', teams={'Wet': 'member'},
                         grants={'isolated': True})
records = {
    'empty': {'parameters': {}},
    'private': {'parameters': {'access': {'visibility': 'private'}}},
    'malformed': {'parameters': {'access': 'invalid JSON'}},
    'legacy_json': {'parameters': {'access': '{"team": null}'}},
    'missing': {'pipe_id': 8},
}
keys = [('acl-list', metric, None, {}) for metric in records] + [('acl-list', 'failed', None, {})]
def get_attributes(pipe, **kwargs):
    if pipe.metric_key == 'failed':
        raise RuntimeError('Metadata read failed.')
    return records[pipe.metric_key]
def visible(target):
    return access.visible_pipe('acl-list', target, instance_keys=str(c), viewer=who)
with patch('meerschaum.utils.fetch_pipes_keys', return_value=keys), \\
     patch.object(c, 'get_pipe_attributes', side_effect=get_attributes), \\
     patch.object(engine, 'get_instance', return_value=c):
    assert {row['mk'] for row in pipes_table.list_rows(c, viewer=who)} == {'empty', 'legacy_json'}
    assert {pipe.metric_key for pipe in engine._instance_pipes(c, viewer=who)} == {'empty', 'legacy_json'}
    assert visible('empty') is not None
    assert visible('legacy_json') is not None
    for name in ('private', 'malformed', 'missing', 'failed'):
        assert visible(name) is None, name
''')
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]


def test_pipe_reservations_follow_named_live_config_and_patch_precedence(sandbox):
    root, plugins = sandbox
    path = root / 'config' / 'meerschaum.json'
    path.parent.mkdir(exist_ok=True)
    path.write_text('''{
      "connectors": {"sql": {
        "default": {"teams": ["Inherited"]},
        "instance": {"flavor": "sqlite", "teams": ["Old", null, 1]},
        "unreserved": {"flavor": "sqlite"},
        "patched": {"flavor": "sqlite", "teams": ["Disk"]}
      }}
    }''')
    result = run_python(root, plugins, '''
import json
import os
from pathlib import Path
import meerschaum as mrsm
from meerschaum.api.dash.connector_listing import connector_reservation
from meerschaum.config import get_config
path = Path(os.environ['MRSM_ROOT_DIR']) / 'config' / 'meerschaum.json'
original = json.loads(path.read_text())
assert connector_reservation('sql:instance') == ('Old',)
assert connector_reservation('sql:unreserved') == ()
runtime = mrsm.get_connector('sql:runtime_only', flavor='sqlite', database=':memory:',
                             teams=['Runtime'])
assert connector_reservation(runtime) == ()
assert connector_reservation('sql:patched') == ('Patch',)
def write():
    previous = path.stat().st_mtime_ns
    path.write_text(json.dumps(original))
    os.utime(path, ns=(previous + 1000000, previous + 1000000))
original['connectors']['sql']['instance']['teams'] = 'Renamed, Another'
original['connectors']['sql']['instance']['flavor'] = 'postgresql'
original['connectors']['sql']['patched']['teams'] = ['Changed on disk']
write()
assert connector_reservation('sql:instance') == ('Renamed', 'Another')
assert connector_reservation('sql:patched') == ('Patch',)
assert get_config('meerschaum', 'connectors', 'sql', 'instance', 'flavor') == 'sqlite'
del original['connectors']['sql']['instance']['teams']
write()
assert connector_reservation('sql:instance') == ()
''', extra_env={'MRSM_PATCH': '{"meerschaum":{"connectors":{"sql":{"patched":{"teams":["Patch"]}}}}}'})
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]
