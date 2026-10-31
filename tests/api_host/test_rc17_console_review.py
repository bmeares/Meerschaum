"""RC17 console regressions use disposable SQLite hosts and synthetic credentials only."""

from tests.api_host._helpers import SIGN_IN, run_python, sandbox  # noqa: F401


def check(result):
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]


def test_encoded_credentials_never_reappear_in_inspection_or_connection_errors(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, '''
import json
from urllib.parse import quote, quote_plus
from unittest.mock import patch
from meerschaum.api.dash import connector_listing as listing
secret = 'review secret+/=encoded'
forms = [secret, quote(secret, safe=''), quote_plus(secret, safe=''),
         quote(quote(secret, safe=''), safe=''),
         quote(secret, safe='').replace('%2B', '%2b').replace('%2F', '%2f'),
         ''.join('%%%02x' % byte for byte in secret.encode())]
for form in forms:
    attrs = {'password':secret, 'database':form, 'host':'db.invalid'}
    row = listing._row('sql','review',attrs,'Config')
    assert row['database'] == listing.MASK, (form,row)
    assert form not in json.dumps(row), (form,row)
    error = listing.scrub('Driver echoed ' + form, listing.hidden_values(attrs,typ='sql'))
    assert secret not in error and form not in error, (form,error)
    assert row['host'] == 'db.invalid'
deep = quote(secret,safe='')
for _ in range(5): deep = quote(deep,safe='')
assert listing.scrub('Driver echoed '+deep,[secret]) == listing.MASK
# A percent-encoded query key still marks its value as private, including a copy elsewhere.
attrs = {'uri':'postgresql://reader:password-value@db.invalid/readings?%70aSsWoRd=query-private',
         'database':'query-private', 'host':'db.invalid'}
assert listing._row('sql','encoded_key',attrs,'Config')['database'] == listing.MASK
for name in ('password','PASSWORD','pass%77ord','%70aSsWoRd'):
    masked = listing.mask_connection_string('https://db.invalid/?'+name+'=query-private','api')
    assert 'query-private' not in masked, masked
# Quoting a credential must not leave a suffix behind after generic name masking.
quoted = 'part"one\\\\two\\nlast'
for value in (repr(quoted), json.dumps(quoted), json.dumps({'password':quoted})):
    hidden = listing.scrub('Driver echoed '+value,[quoted])
    assert 'part' not in hidden and 'last' not in hidden, hidden
assert listing.mask_attributes({'password':'xy','host':'xy'})['host'] == listing.MASK
assert listing.mask_attributes({'api_key':4321,'port':4321})['port'] == listing.MASK
for form in (quote('pássword/κ',safe=''),quote_plus('pássword/κ',safe='')):
    assert listing.scrub('Driver echoed '+form,['pássword/κ']) == listing.MASK
assert listing.scrub('A harmless host db.invalid', [secret]) == 'A harmless host db.invalid'
with patch.object(listing, '_test', return_value=(False,'Driver echoed '+quote_plus(secret,safe=''))), \\
     patch('meerschaum.config.get_config', return_value={'sql':{'review':{'password':secret}}}):
    result = listing.test_connector('sql:review')
    assert result['success'] is False
    assert secret not in result['message'] and quote_plus(secret,safe='') not in result['message']
'''))


def test_performance_query_drilldown_history_pages_and_denied_refresh_are_fresh(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, SIGN_IN + '''
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlencode
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.dash import performance, dash_app
from meerschaum.api.dash.callbacks import performance as callbacks
from meerschaum.api.dash.callbacks.overview import _digest
from dash.exceptions import PreventUpdate
c = get_api_connector()
session = sign_in('review-performance',role='viewer')
ref = str(c)+'|review|readings|None'
query = '?' + urlencode({'instance':str(c),'pipe':ref,'window':'7d'})
selected = callbacks.follow_performance_location(query,'/dash/performance')
assert selected == (str(c),'7d',ref,None), selected
assert callbacks.follow_performance_location('?window=invalid','/dash/performance')[1:] == ('24h',None,None)
try:
    callbacks.follow_performance_location(query,'/dash/pipes')
except PreventUpdate:
    pass
else:
    raise AssertionError('A different page changed Performance filters.')
# Query-only navigation is a callback input; it cannot depend on pathname changing.
dash_app._setup_server()
entry = next(item for item in dash_app.callback_map.values()
             if any(value['id'] == 'performance-location' and value['property'] == 'search'
                    for value in item.get('inputs',[])))
assert any(value.component_id == 'performance-pipe' for value in entry['output'])
assert {value.component_property for value in entry['output']
        if value.component_id == 'performance-pipe'} == {'value','options'}
from meerschaum.api.dash.pages import performance as pages
layout = str(pages.route('/dash/performance','http://localhost/dash/performance'+query))
assert "'label': 'Selected Pipe', 'value': '"+ref+"'" in layout, layout
history = [{'started':'2026-10-28T01:00:00Z','pipe_ref':str(c)+'|review|reading_'+str(i)+'|None',
            'success':True,'duration_seconds':0.1,'rows':1} for i in range(103)]
data = {'summary':{'syncs':103},'history':history,'series':[],'pipes':[],'pipelines':[],
        '_instance':str(c),'_pipe_options':[{'label':'Readings','value':ref}]}
calls=[]
def read(*args,**kwargs):
    calls.append((args,kwargs))
    return data
with patch.object(performance,'metrics',side_effect=read):
    result = callbacks.render_performance('/dash/performance',None,'7d',ref,None,0,0,2,str(c),session)
    assert result[3:6] == (3,2,False), result[3:]
    text = str(result[0])
    assert '51–100 of 103 syncs' in text and 'reading_50' in text and 'reading_99' in text
    assert 'reading_49' not in text and 'reading_100' not in text
    # A page click reads fresh rights/metrics; there is no browser history Store to forge.
    assert len(calls) == 1 and calls[-1][0][1] == session
    last = callbacks.render_performance('/dash/performance',None,'7d',ref,None,0,0,9999,str(c),session)
    assert last[3:6] == (3,3,False) and '101–103 of 103 syncs' in str(last[0])
    with patch.object(callbacks.dash,'ctx',SimpleNamespace(triggered_id='performance-window')):
        reset = callbacks.render_performance('/dash/performance',None,'24h',ref,None,0,0,3,str(c),session)
    assert reset[4] == 1 and '1–50 of 103 syncs' in str(reset[0])
    # Exercise the real Dash protocol: filters and options are part of the same ten outputs.
    output, registered = next((key,value) for key,value in dash_app.callback_map.items()
                              if 'performance-body.children' in key)
    values = {
        'performance-location.pathname':'/dash/performance', 'performance-location.search':query,
        'performance-window.value':'7d',
        'performance-pipe.value':ref, 'performance-pipeline.value':None,
        'performance-refresh.n_clicks':0, 'performance-interval.n_intervals':0,
        'performance-history-page.active_page':2, 'performance-instance.data':str(c),
    }
    endpoint = next(str(rule) for rule in dash_app.server.url_map.iter_rules()
                    if str(rule).endswith('/_dash-update-component'))
    response = dash_app.server.test_client().post(endpoint,json={
        'output':output,
        'outputs':[{'id':value.component_id,'property':value.component_property}
                   for value in registered['output']],
        'inputs':[{**value,'value':values[value['id']+'.'+value['property']]}
                  for value in registered['inputs']],
        'state':[{'id':'session-store','property':'data','value':session}],
        'changedPropIds':['performance-history-page.active_page'],
    })
    assert response.status_code == 200, response.get_data(as_text=True)
    rendered = response.get_json()['response']
    assert rendered['performance-history-page'] == {'max_value':3,'active_page':2}
    assert rendered['performance-history-controls']['hidden'] is False
    assert '51–100 of 103 syncs' in str(rendered['performance-body'])
    assert rendered['performance-pipe']['value'] == ref
    assert rendered['performance-pipe']['options'] == [{'label':'Readings','value':ref}]
    with patch.object(callbacks.dash,'ctx',SimpleNamespace(triggered_id='performance-location')):
        query_result = callbacks.render_performance('/dash/performance',query,'24h',None,None,0,0,3,str(c),session)
    assert query_result[6:] == (str(c),'7d',ref,None) and query_result[4] == 1
assert performance.history_window({'history':history*20},9999)[1:] == (20,20,1000)
assert performance.history_window(data,'not-a-page')[1] == 1
# Permission revocation clears prior data, page controls, and dropdown metadata.
with patch.object(performance,'metrics',side_effect=PermissionError('PRIVATE STATE')):
    denied = callbacks.render_performance('/dash/performance',None,'7d',ref,None,0,0,2,str(c),session)
    assert denied[1:6] == ([{'label':'Selected Pipe','value':ref}],[],1,1,True)
    assert denied[6:] == (str(c),'7d',ref,None)
    assert 'not available' in str(denied[0])
    old = performance.build_overview_body({'summary':{'syncs':99}},str(c))
    tile, digest = callbacks.render_performance_tile(1,{'instance':str(c)},_digest(old),session)
    assert 'not available' in str(tile) and 'PRIVATE STATE' not in str(tile)
    assert digest != _digest(old)
with patch.object(performance,'metrics',side_effect=RuntimeError('PRIVATE SQL PASSWORD')):
    error = callbacks.render_performance('/dash/performance',None,'7d',ref,None,0,0,2,str(c),session)
    assert error[1:6] == ([{'label':'Selected Pipe','value':ref}],[],1,1,True)
    assert 'PRIVATE SQL' not in str(error)
'''))


def test_source_previews_reject_duplicate_archives_and_stale_or_forged_sessions(sandbox):
    root, plugins = sandbox
    check(run_python(root, plugins, SIGN_IN + '''
import io, pathlib, tarfile
from unittest.mock import patch
from meerschaum.api import get_api_connector
from meerschaum.api.dash import plugin_sources as source, plugins as plugin_pages, sessions
from meerschaum.api.dash.callbacks import plugins as callbacks
from meerschaum.config import paths
from meerschaum.core import Plugin, User
admin = sign_in('review-source-admin',user_type='admin')
reader = sign_in('review-source-reader',role='viewer')
c = get_api_connector()
package = pathlib.Path(paths.PLUGINS_DIR_PATHS[0])/'review_source'
package.mkdir()
code = "raise RuntimeError('INSPECTION_MUST_NOT_EXECUTE')\\n<script>window.reviewExecuted = true</script>"
(package/'__init__.py').write_text(code)
# Metadata and source rendering use discovery / cached manifests, never import plugin code.
detail = plugin_pages.build_installed_plugin_detail('review_source',admin)
assert 'plugin-source-content' in str(detail)
assert source.read_source('installed','review_source','__init__.py',admin) == code
assert 'INSPECTION_MUST_NOT_EXECUTE' in str(callbacks.show_plugin_source(
    '__init__.py',{'kind':'installed','name':'review_source'},admin))
for session in (reader,{'session-id':'unknown','username':'review-source-admin','admin':True},{}):
    result = callbacks.show_plugin_source('__init__.py',{'kind':'installed','name':'review_source'},session)
    assert 'not available' in result and 'INSPECTION_MUST_NOT_EXECUTE' not in result
plugin = Plugin('review_repository',user_id=c.get_user_id(User('review-source-admin',instance=c)))
assert c.register_plugin(plugin)[0]
archives = pathlib.Path(paths.PLUGINS_ARCHIVES_RESOURCES_PATH)
archives.mkdir(parents=True,exist_ok=True)
archive_path = archives/'review_repository.tar.gz'
def archive(entries):
    with tarfile.open(archive_path,'w:gz') as stream:
        for name,content in entries:
            body=content.encode(); info=tarfile.TarInfo(name); info.size=len(body)
            stream.addfile(info,io.BytesIO(body))
def refused(call):
    try: call()
    except (ValueError,PermissionError,OSError): return
    raise AssertionError('Unsafe source preview succeeded.')
archive([('review/__init__.py','first'),('review/__init__.py','second')])
refused(lambda: source.read_source('repository','review_repository','review/__init__.py',reader))
archive([('review/__init__.py','safe')])
assert source.read_source('repository','review_repository','review/__init__.py',reader) == 'safe'
oversized = 'review/'+'x'*source.MAX_PATH_BYTES+'.py'
too_deep = '/'.join(['part']*source.MAX_PATH_PARTS+['file.py'])
multibyte = 'review/'+'é'*600+'.py'
archive([('review/__init__.py','safe'),(oversized,'skip'),(too_deep,'skip'),(multibyte,'skip')])
assert source.source_files('repository','review_repository',reader) == ['review/__init__.py']
for path in (oversized,too_deep,multibyte):
    refused(lambda path=path: source.read_source('repository','review_repository',path,reader))
malformed = 'review/bad'+chr(0xDCFF)+'.py'
archive([('review/__init__.py','safe'),(malformed,'skip')])
assert source.source_files('repository','review_repository',reader) == ['review/__init__.py']
assert callbacks.show_plugin_source(malformed,{'kind':'repository','name':'review_repository'},reader) == (
    'This file cannot be previewed. Choose another source file.')
# The actual Dash response also remains200; an unpaired surrogate never reaches its output.
from meerschaum.api.dash import dash_app
dash_app._setup_server()
output = next(key for key in dash_app.callback_map if key.startswith('plugin-source-content.children'))
endpoint = next(str(rule) for rule in dash_app.server.url_map.iter_rules()
                if str(rule).endswith('/_dash-update-component'))
response = dash_app.server.test_client().post(endpoint,json={
    'output':output, 'outputs':{'id':'plugin-source-content','property':'children'},
    'inputs':[{'id':'plugin-source-file','property':'value','value':malformed},
              {'id':'plugin-source-context','property':'data','value':{'kind':'repository','name':'review_repository'}}],
    'state':[{'id':'session-store','property':'data','value':reader}],
    'changedPropIds':['plugin-source-file.value'],
})
assert response.status_code == 200, response.get_data(as_text=True)
assert response.get_json()['response']['plugin-source-content']['children'].startswith('This file cannot be previewed.')
# PAX metadata is capped as raw expanded bytes before the tar parser can consume it.
with tarfile.open(archive_path,'w:gz',format=tarfile.PAX_FORMAT) as stream:
    body=b'safe'; info=tarfile.TarInfo('review/__init__.py'); info.size=len(body)
    info.pax_headers={'comment':'x'*8192}
    stream.addfile(info,io.BytesIO(body))
with patch.object(source,'MAX_SCAN_BYTES',2048), \\
     patch.object(source.tarfile,'open',side_effect=AssertionError('Tar parsed before the expansion cap.')) as parser:
    refused(lambda: source.source_files('repository','review_repository',reader))
    assert parser.call_count == 0
archive([('review/__init__.py','safe')])
with patch.object(source,'MAX_ARCHIVE_BYTES',1):
    refused(lambda: source.source_files('repository','review_repository',reader))
with patch.object(source,'MAX_ENTRIES',1):
    archive([('review/a.py','a'),('review/b.py','b')])
    refused(lambda: source.source_files('repository','review_repository',reader))
with patch.object(source,'MAX_FILES',1):
    refused(lambda: source.source_files('repository','review_repository',reader))
archive([('review/__init__.py','safe')])
sessions.delete_session(reader['session-id'])
assert 'not available' in callbacks.show_plugin_source(
    'review/__init__.py',{'kind':'repository','name':'review_repository'},reader)
sessions.delete_session(admin['session-id'])
assert 'not available' in callbacks.show_plugin_source(
    '__init__.py',{'kind':'installed','name':'review_source'},admin)
'''))
