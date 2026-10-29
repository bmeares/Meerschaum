"""The Performance page filters trusted metadata before calling the native metrics reader."""

from tests.api_host._helpers import SIGN_IN, run_python, sandbox  # noqa: F401


def check(result):
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]


def test_performance_uses_fresh_visibility_ceilings_and_refuses_hidden_filters(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, SIGN_IN + '''
from unittest.mock import patch
import json, os
os.environ['MRSM_PATCH'] = json.dumps({'meerschaum': {'connectors': {'rc16': {
    'ops': {'teams':['Ops']}, 'sales': {'teams':['Sales']},
}}}})
from meerschaum.config.environment import apply_environment_patches
apply_environment_patches()
import meerschaum as mrsm
from meerschaum.core import User
from meerschaum.core.Pipeline._saved import save_pipeline
from meerschaum.api import get_api_connector
from meerschaum.api.dash import performance
c = get_api_connector()
admin = sign_in('perf-admin', user_type='admin')
reader = sign_in('perf-reader', role='viewer', teams={'Ops':'member'})
denied = sign_in('perf-denied', role='custom', permissions=[])
def pipe(name, owner=None, visibility='public', connector='rc16'):
    p = mrsm.Pipe(connector,name,instance=c,parameters={'access': {'owner':owner,'visibility':visibility}})
    assert p.register()[0]
    return p, str(c)+'|'+connector+'|'+name+'|None'
public, public_ref = pipe('public')
own, own_ref = pipe('own','perf-reader','private')
hidden, hidden_ref = pipe('secret','somebody-else','private')
team_blocked, blocked_ref = pipe('blocked-source',connector='rc16:sales')
assert save_pipeline('perf-public',[public_ref],c,access={'visibility':'public'})['name'] == 'perf-public'
assert save_pipeline('perf-private',[hidden_ref],c,access={'visibility':'private','owner':'somebody-else'})['name'] == 'perf-private'
calls = []
def native(**kwargs):
    calls.append(kwargs)
    return {'summary': {'syncs': 0}, 'pipes': [], 'pipelines': [], 'history': [], 'series': []}
def refused(call):
    before = len(calls)
    try: call()
    except PermissionError: pass
    else: raise AssertionError('Unauthorized metrics selection succeeded.')
    assert len(calls) == before
with patch.object(mrsm,'get_engine_metrics',native,create=True):
    result = performance.metrics(str(c),reader,'7d')
    ceiling = calls[-1]
    assert set(ceiling['visible_refs']) == {public_ref, own_ref}, ceiling
    assert ceiling['visible_pipelines'] == ['perf-public'], ceiling
    assert (ceiling['end'] - ceiling['begin']).total_seconds() == 7 * 86400
    assert ceiling['limit'] == 1000 and ceiling['buckets'] == 48
    assert hidden_ref not in str(result) and blocked_ref not in str(result) and 'perf-private' not in str(result)
    refused(lambda: performance.metrics(str(c),reader,pipe=hidden_ref))
    refused(lambda: performance.metrics(str(c),reader,pipeline='perf-private'))
    refused(lambda: performance.metrics(str(c),reader,pipe=blocked_ref))
    refused(lambda: performance.metrics(str(c),denied))
    refused(lambda: performance.metrics(str(c),{}))
    performance.metrics(str(c),admin)
    assert calls[-1]['visible_refs'] is None and calls[-1]['visible_pipelines'] is None
    performance.metrics(str(c),admin,'all')
    assert (calls[-1]['end'] - calls[-1]['begin']).total_seconds() == 30 * 86400
    # Object visibility and permission changes take effect on the next refresh without a cache reset.
    public.parameters = {'access': {'owner':'somebody-else','visibility':'private'}}
    assert public.edit()[0]
    performance.metrics(str(c),reader)
    assert calls[-1]['visible_refs'] == [own_ref], calls[-1]
    assert c.set_user_attributes(User('perf-reader',instance=c), {'role':'custom','scopes':[]})[0]
    refused(lambda: performance.metrics(str(c),reader))
'''))


def test_performance_chart_units_empty_and_error_states(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
from unittest.mock import patch
from meerschaum.api.dash import performance
from meerschaum.api.dash.pages import performance as pages
from meerschaum.api.dash.callbacks import performance as callbacks
summary = {'syncs':3,'succeeded':2,'failed':1,'success_rate':2/3,'rows':123,
           'avg_duration_seconds':0.25,'rows_per_second':164}
data = {'summary':summary,'series':[{'ts':'2026-10-28T01:00:00Z',**summary}],
        'pipes':[{'pipe_ref':'sql:apihosttest|rc16|example|None','summary':summary}],
        'pipelines':[{'name':'daily','summary':summary}],
        'history':[{'started':'2026-10-28T01:00:00Z','pipe_ref':'sql:apihosttest|rc16|example|None',
                    'success':False,'duration_seconds':0.25,'rows':0}],
        'limit':1000,'truncated':True,'retention_days':30}
body = performance.build_body(data,'sql:apihosttest','24h')
text = str(body)
for label in ('Failures','250 ms','66.7% succeeded','Rows affected/s','Seconds / sync',
              'Failed','This view is limited','30 days','unchanged sync affects zero rows'):
    assert label in text, label
assert 'No syncs in this window' in str(performance.build_body({'summary':{'syncs':0}},'sql:apihosttest','24h'))
limited = {'summary':{'syncs':0},'truncated':True}
empty = str(performance.build_body(limited,'sql:apihosttest','24h'))
assert 'No syncs in this sample' in empty and 'This view is limited' in empty
assert 'longer window' not in empty
tile = str(performance.build_overview_body(limited,'sql:apihosttest'))
assert 'No syncs in this sample' in tile and 'limited' in tile and 'View performance' in tile
assert performance.duration(None) == '—'
assert performance.timestamp('invalid') == '—'
layout = str(pages.route('/dash/performance','http://localhost/dash/performance?window=7d'))
assert 'performance-window' in layout and "value='7d'" in layout and 'Loading performance' in layout
with patch.object(performance,'metrics',side_effect=RuntimeError('PRIVATE SQL PASSWORD')):
    result = callbacks.render_performance('/dash/performance',None,'24h',None,None,0,0,1,'sql:apihosttest',{})
    assert 'could not be loaded' in str(result) and 'PRIVATE SQL PASSWORD' not in str(result)
with patch.object(performance,'metrics',side_effect=PermissionError('private object')):
    result = callbacks.render_performance('/dash/performance',None,'24h',None,None,0,0,1,'sql:apihosttest',{})
    assert 'not available' in str(result) and 'private object' not in str(result)
'''))
