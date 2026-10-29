"""List-shaped instance key results never substitute empty extras for fresh Pipe access."""

from tests.api_host._helpers import run_python, sandbox  # noqa: F401


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
            if access.can_see_pipe(who,parameters)}
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
