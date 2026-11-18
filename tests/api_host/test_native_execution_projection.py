"""Console projection and typed Manage boundaries; native route authority has Rust HTTP tests."""

import json

from tests.api_host._helpers import run_python, sandbox  # noqa: F401


def result(process):
    assert process.returncode == 0, process.stderr[-6000:]
    line = [line for line in process.stdout.splitlines() if line.startswith('JSON:')][-1]
    return json.loads(line[5:])


BASE = '''
import json, uuid
from dataclasses import replace
from unittest import mock
from meerschaum.api.dash import access
from meerschaum.api.dash import _native_execution as native
SESSION = str(uuid.uuid4())
TEAM = str(uuid.uuid4())
viewer = access.make_viewer('alice', role='operator', teams={'ApiTeam':'member'},
    primary_team='ApiTeam', grants={'isolated':True,'plugins_write':False,
        'runs_code':False,'plugins':['probe'],'connectors':['sql:fixture']})
def projection():
    return {'schema':1,'username':'alice',
        'source':{'primary_team':'ApiTeam','team_id':TEAM,'isolated':True,'runs_code':False},
        'permissions':{'actions_execute':True,'jobs_execute':True,'jobs_write':True,'webterm_use':False},
        'capabilities':{'tracked_api':True,'api_terminals':False},
        'policy':{'non_admin_actions':True}}
launch = {'actions:execute','webterm:use','jobs:execute','jobs:write'}
'''


def test_a_base_cache_hit_rechecks_native_rights_and_removes_stale_launch_principal(sandbox):
    root, plugins = sandbox
    out = result(run_python(root, plugins, BASE + '''
from meerschaum.api.dash import sessions
access.forget()
with mock.patch.object(native,'_binding',return_value=('alice','credential-one')), \
     mock.patch.object(native,'_read',side_effect=[projection(),PermissionError('refused')]), \
     mock.patch.object(access,'viewer_for',return_value=viewer), \
     mock.patch.object(sessions,'get_username_from_session',return_value='alice'):
    allowed = access.get_viewer(session_id=SESSION)
    refused = access.get_viewer(session_id=SESSION)
    cached = access._viewer_cache[SESSION][1]
assert allowed.can('actions:execute') and allowed.can('jobs:write')
assert not allowed.can('webterm:use') and not allowed.native_execution.terminal_exec
assert not any(refused.can(scope) for scope in launch)
assert not launch.intersection(refused.principal.permissions)
assert refused.principal is not viewer.principal
assert refused.can('pipes:write') and 'pipes:write' in refused.principal.permissions
assert cached is viewer and not cached.can('actions:execute') and cached.native_execution is None
assert cached.can('jobs:write'), 'A failed overlay must not mutate the shared base record'
print('JSON:'+json.dumps({'fresh':True,'closed':True,'cache_inert':True}))
'''))
    assert out == {'fresh': True, 'closed': True, 'cache_inert': True}


def test_unknown_source_changed_session_and_narrowed_permissions_fail_closed(sandbox):
    root, plugins = sandbox
    out = result(run_python(root, plugins, BASE + '''
checks = 0
for mutation in [
    lambda value: value.update(username='replacement'),
    lambda value: value.update(schema=True),
    lambda value: value['permissions'].update(actions_execute='true'),
    lambda value: value['capabilities'].update(tracked_api=False),
    lambda value: value['policy'].update(non_admin_actions=False),
    lambda value: value['source'].update(primary_team='OtherTeam'),
    lambda value: value['source'].update(team_id=str(uuid.UUID(int=0))),
]:
    body = projection(); mutation(body)
    with mock.patch.object(native,'_binding',return_value=('alice','credential-one')), \
         mock.patch.object(native,'_read',return_value=body):
        current = native.project(viewer,SESSION)
    assert not any(current.can(scope) for scope in launch)
    assert not launch.intersection(current.principal.permissions)
    checks += 1
for after in [None,('alice','credential-two'),('replacement','credential-one')]:
    with mock.patch.object(native,'_binding',side_effect=[('alice','credential-one'),after]), \
         mock.patch.object(native,'_read',return_value=projection()):
        current = native.project(viewer,SESSION)
    assert not any(current.can(scope) for scope in launch)
    assert not launch.intersection(current.principal.permissions)
    checks += 1
wildcard=replace(viewer,permissions=frozenset({'*','pipes:write'}))
with mock.patch.object(native,'_binding',return_value=('alice','credential-one')), \
     mock.patch.object(native,'_read',side_effect=PermissionError('refused')):
    closed=native.project(wildcard,SESSION)
assert '*' not in closed.permissions and not any(closed.can(scope) for scope in launch)
assert not launch.intersection(closed.principal.permissions)
body = projection(); body['permissions'].update(actions_execute=False,jobs_execute=False,jobs_write=False)
with mock.patch.object(native,'_binding',return_value=('alice','credential-one')), \
     mock.patch.object(native,'_read',return_value=body):
    narrowed = native.project(viewer,SESSION)
assert not any(narrowed.can(scope) for scope in launch)
minimal = access.make_viewer('alice',permissions=['pipes:write'],teams={'ApiTeam':'member'},
    primary_team='ApiTeam',grants=dict(viewer.grants))
with mock.patch.object(native,'_binding',return_value=('alice','credential-one')), \
     mock.patch.object(native,'_read',return_value=projection()):
    current = native.project(minimal,SESSION)
assert current.can('actions:execute') and not current.can('jobs:execute') and not current.can('jobs:write')
for ordinary in [access.make_viewer('ordinary',role='operator'),access.make_viewer('admin',admin=True),
    access.SYSTEM, access.make_viewer('code',role='operator',teams={'Code':'member'},
    grants={'isolated':True,'plugins_write':True,'runs_code':True})]:
    with mock.patch.object(native,'_read',side_effect=AssertionError('Unexpected native query')):
        assert native.project(ordinary,SESSION) is ordinary
print('JSON:'+json.dumps({'refusals':checks,'narrow':True,'ordinary':True}))
'''))
    assert out == {'refusals': 10, 'narrow': True, 'ordinary': True}


HTTP = '''
import os, threading, http.server
from flask import Flask
from meerschaum.api.host import HOST_CONFIG_ENV
seen=[]; mode={'value':'ok'}
class Handler(http.server.BaseHTTPRequestHandler):
    def send(self,code,value,raw=False):
        data=value if raw else json.dumps(value).encode()
        self.send_response(code)
        if code==302:self.send_header('location','/must-not-follow')
        self.send_header('content-type','application/json');self.send_header('content-length',str(len(data)))
        self.end_headers();self.wfile.write(data)
    def do_GET(self):
        seen.append({'path':self.path,'cookie':self.headers.get('cookie'),
            'authorization':self.headers.get('authorization'),'host_token':self.headers.get('x-mrsm-host-token')})
        if mode['value']=='redirect':self.send(302,{})
        elif mode['value']=='large':self.send(200,b' '*4097,raw=True)
        elif mode['value']=='refuse':self.send(403,{'detail':'refused'})
        else:
            value=projection()
            if mode['value']=='no-launch-scopes':
                value['permissions'].update(actions_execute=False,jobs_execute=False,jobs_write=False)
                value['policy']['non_admin_actions']=False
            self.send(200,value)
    def do_POST(self):
        value=json.loads(self.rfile.read(int(self.headers['content-length'])))
        seen.append({'path':self.path,'cookie':self.headers.get('cookie'),
            'authorization':self.headers.get('authorization'),'body':value})
        if mode['value']=='refuse':self.send(403,{'detail':'refused'});return
        rpc={'jsonrpc':'2.0','id':value['id'],'result':{'content':[{'type':'text',
            'text':json.dumps({'success':mode['value']!='tuple-false','message':'native result'})}],'isError':False}}
        if mode['value']=='error':rpc['result']['isError']=True
        if mode['value']=='wrong-id':rpc['id']='different-request'
        if mode['value']=='malformed':rpc['result']['content'][0]['text']='not-json'
        self.send(200,rpc)
    def log_message(self,*args):pass
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
os.environ[HOST_CONFIG_ENV]=json.dumps({'server_url':f'http://127.0.0.1:{server.server_port}'})
web=Flask(__name__)
'''


def test_fixed_native_transport_keeps_the_presented_ceiling_and_never_retries_a_pipe_mutation(sandbox):
    root, plugins = sandbox
    out = result(run_python(root, plugins, BASE + HTTP + '''
try:
    with web.test_request_context(headers={'Authorization':'Bearer narrow-original'}), \
         mock.patch.object(native,'_binding',return_value=('alice','credential-one')):
        assert native.project(viewer,SESSION).can('actions:execute')
        assert seen[-1]['path']=='/execute/capabilities'
        assert seen[-1]['authorization']=='Bearer narrow-original'
        assert seen[-1]['cookie']=='mrsm-session-id='+SESSION and seen[-1]['host_token'] is None
        for failure in ['redirect','large','refuse']:
            mode['value']=failure; before=len(seen)
            closed=native.project(viewer,SESSION)
            assert not any(closed.can(scope) for scope in launch)
            assert not launch.intersection(closed.principal.permissions)
            assert len(seen)==before+1 and seen[-1]['path']=='/execute/capabilities'
        meta={'instance_keys':'sql:fixture','connector_keys':'manual:source','metric_key':'metric',
              'location_key':None,'sql':'must never enter the request'}
        mode['value']='ok'
        ok,message=native.pipe_action(viewer,SESSION,meta,'sync',begin='2026-01-02',end='2026-01-03')
        assert ok and message=='native result'
        request=seen[-1]
        assert request['path']=='/mcp' and request['authorization']=='Bearer narrow-original'
        assert request['cookie']=='mrsm-session-id='+SESSION
        assert request['body']['method']=='tools/call' and request['body']['params']['name']=='sync_pipe'
        assert request['body']['params']['arguments']=={'instance_keys':'sql:fixture',
            'connector_keys':'manual:source','metric_key':'metric','begin':'2026-01-02','end':'2026-01-03'}
        assert native.pipe_action(viewer,SESSION,meta,'verify')[0]
        assert seen[-1]['body']['params']['name']=='verify_pipe'
        minimal=replace(viewer,permissions=frozenset({'pipes:write'}))
        mode['value']='no-launch-scopes'
        assert native.pipe_action(minimal,SESSION,meta,'sync')[0], 'Typed Sync must not require public action/job scopes'
        for failure in ['tuple-false','error','wrong-id','malformed','refuse']:
            mode['value']=failure; before=sum('body' in item for item in seen)
            assert not native.pipe_action(viewer,SESSION,meta,'sync')[0]
            after=sum('body' in item for item in seen)
            assert after-before==(0 if failure=='refuse' else 1), 'A lost/refused mutation must never be retried'
        mode['value']='ok'; before=sum('body' in item for item in seen)
        with mock.patch.object(native,'_binding',side_effect=[('alice','credential-one'),('alice','credential-one'),None]):
            success,message=native.pipe_action(viewer,SESSION,meta,'sync')
        assert not success and 'Check the pipe state' in message and sum('body' in item for item in seen)==before+1
        before=sum('body' in item for item in seen)
        assert not native.pipe_action(viewer,SESSION,meta,'drop')[0]
        assert not native.pipe_action(viewer,SESSION,meta,'sync',begin={'sql':'bad'})[0]
        assert sum('body' in item for item in seen)==before
    print('JSON:'+json.dumps({'fixed':True,'ceiling':True,'typed':True,'no_retry':True}))
finally:
    server.shutdown();server.server_close();thread.join(timeout=5)
'''))
    assert out == {'fixed': True, 'ceiling': True, 'typed': True, 'no_retry': True}


def test_real_manage_callbacks_use_typed_sync_verify_and_cannot_enter_terminal_exec(sandbox):
    root, plugins = sandbox
    out = result(run_python(root, plugins, BASE + '''
import inspect
from types import SimpleNamespace
from dash._callback_context import context_value
from dash._utils import AttributeDict
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash.callbacks import execute as buttons, pipes as detail
from meerschaum.api.dash import execute, components, pipes_access, pipe_detail, webterm
pipe=SimpleNamespace(connector_keys='manual:source',metric_key='metric',location_key=None,instance_keys='sql:fixture')
meta=pipe_detail.meta_of(pipe); ref=pipe_detail.pipe_ref(pipe)
selected=replace(viewer,permissions=viewer.permissions|{'actions:execute'})
calls=[]
def typed(*args,**kwargs):calls.append((args,kwargs));return True,'native completed'
def trigger(identifier):
    value=json.dumps(identifier,separators=(',',':')) if isinstance(identifier,dict) else identifier
    context_value.set(AttributeDict(**{'triggered_inputs':[{'prop_id':value+'.n_clicks','value':1}]}))
def start(identifier):
    trigger(identifier)
    values={name:None for name in inspect.signature(buttons.start_exec).parameters}
    values.update(manage_clicks=[1],go_clicks=1,session_store_data={'session-id':SESSION})
    return buttons.start_exec(**values)
with mock.patch.object(access,'get_viewer',return_value=selected), \
     mock.patch.object(pipes_access,'viewer',return_value=selected), \
     mock.patch.object(access,'visible_pipe_from_meta',return_value=pipe), \
     mock.patch.object(access,'may_run_pipe_action',return_value=(True,'')), \
     mock.patch.object(detail,'_meta_pipe',return_value=pipe), \
     mock.patch.object(native,'pipe_action',side_effect=typed), \
     mock.patch.object(execute,'mint_bearer',side_effect=AssertionError('No new JWT')), \
     mock.patch.object(execute,'execution_request',side_effect=AssertionError('No terminal validation')), \
     mock.patch.object(execute,'run_action',side_effect=AssertionError('No generic job fallback')), \
     mock.patch.object(buttons,'is_state_authenticated',return_value=True), \
     mock.patch.object(webterm,'webterm_enabled',return_value=True):
    for action in ['sync','verify']:
        store,alert=start({'type':'manage-pipe-button','index':json.dumps(meta),'action':action})
        assert store['typed_pipe'] and 'terminal' not in store and 'job' not in store
        assert calls[-1][0][3]==action and calls[-1][0][2]==meta
    count=len(calls)
    store,alert=start({'type':'manage-pipe-button','index':json.dumps(meta),'action':'clear'})
    assert len(calls)==count and 'terminal' not in store and 'no native API-only' in str(alert)
    store,alert=start('go-button')
    assert len(calls)==count and 'terminal' not in store
    disabled,style=buttons.terminal_exec_control({'session-id':SESSION})
    assert disabled and style['display']=='none'
    nav=components.get_nav_state(session_id=SESSION)
    assert nav['execution_permissions']['actions_execute'] and not nav['terminal_exec'] and not nav['terminal']
    for action in ['sync','verify']:
        trigger('pipe-action-'+action)
        store,alert,closed=detail.run_pipe_action(1,1,None,None,None,'2026-01-02','2026-01-03',meta,{'session-id':SESSION})
        assert store['completed'] and store['ref']==ref and 'job' not in store
        assert calls[-1][0][3]==action and calls[-1][1]=={'begin':'2026-01-02','end':'2026-01-03'}
    trigger('pipe-confirm-button')
    count=len(calls)
    store,alert,closed=detail.run_pipe_action(None,None,1,{'action':'drop','ref':ref},'metric',None,None,meta,{'session-id':SESSION})
    assert store is detail.dash.no_update and len(calls)==count
for ordinary in [access.make_viewer('ordinary',role='operator'),access.make_viewer('admin',admin=True),
    access.make_viewer('code',role='operator',teams={'Code':'member'},
    grants={'isolated':True,'plugins_write':True,'runs_code':True})]:
    with mock.patch.object(access,'get_viewer',return_value=ordinary), \
         mock.patch.object(webterm,'webterm_enabled',return_value=True):
        disabled,style=buttons.terminal_exec_control({'session-id':SESSION})
        assert not disabled and style['display']=='block'
print('JSON:'+json.dumps({'typed_calls':len(calls),'terminal_closed':True,'separate_controls':True}))
'''))
    assert out == {'typed_calls': 4, 'terminal_closed': True, 'separate_controls': True}
