"""
The console's shell (design §8.1): the top navigation, the Overview at `/dash`, the Terminal page
(v4's dashboard, `/dash/terminal`) and the Connectors page, without a browser
(`tests/console/test_console_shell.py` drives them in Chromium).
"""

import json

from tests.api_host._helpers import SIGN_IN, run_python, write_plugin, sandbox  # noqa: F401

#: Synthetic secrets: none of them may reach the browser.
SECRETS = ('Sup3rS3cretPw', 'An0therS3cret', 'patch-client-s3cret', 'tok3n-in-query')

#: Fire the console's router callback for a path (the payload Dash sends).
ROUTE = SIGN_IN + '''
def route(client, dash_app, pathname, session_id='00000000-0000-4000-8000-0000005e1101', query=''):
    callback = next(key for key in dash_app.callback_map if 'page-layout-div.children' in key)
    outputs = []
    for part in callback.strip('.').split('...'):
        component_id, _, prop = part.partition('.')
        outputs.append({'id': component_id, 'property': prop.split('@')[0]})
    payload = {
        'output': callback,
        'outputs': outputs,
        'inputs': [
            {'id': 'mrsm-location', 'property': 'pathname', 'value': pathname},
            {'id': 'session-store', 'property': 'data', 'value': {'session-id': session_id}},
        ],
        'changedPropIds': ['mrsm-location.pathname'],
        'state': [{'id': 'mrsm-location', 'property': 'href',
                   'value': 'http://localhost' + pathname + query}],
    }
    response = client.post('/dash/_dash-update-component', json=payload)
    assert response.status_code == 200, response.text[-2000:]
    return response.json()['response']['page-layout-div']['children']

def walk(node):
    if isinstance(node, list):
        for child in node:
            yield from walk(child)
        return
    if not isinstance(node, dict):
        return
    yield node
    props = node.get('props', {}) or {}
    for key in ('children', 'label'):
        yield from walk(props.get(key, None))

def ids(node):
    return [n.get('props', {}).get('id') for n in walk(node) if n.get('props', {}).get('id')]

def find(node, component_id):
    return next((n for n in walk(node) if n.get('props', {}).get('id') == component_id), None)
'''


def _out(result):
    assert result.returncode == 0, result.stderr[-4000:]
    line = next(line for line in reversed(result.stdout.splitlines()) if line.startswith('JSON:'))
    return json.loads(line[len('JSON:'):])


def test_secrets_are_masked_everywhere(sandbox):
    root, plugins = sandbox
    out = _out(run_python(root, plugins, '''
import os, json
os.environ['MRSM_SQL_SHELLSECRET'] = 'postgresql://scratchuser:Sup3rS3cretPw@127.0.0.1:1/scratch?sslpassword=tok3n-in-query'
os.environ['MRSM_API_SHELLAPI'] = 'http://apiuser:An0therS3cret@127.0.0.1:2'
os.environ['MRSM_PATCH'] = json.dumps({'meerschaum': {'connectors': {'api': {'patched': {
    'host': 'example.invalid', 'username': 'u', 'client_secret': 'patch-client-s3cret',
    'options': {'note': 'uses patch-client-s3cret inside', 'nested_token': 'patch-client-s3cret'},
}}}}})
from meerschaum.config.environment import apply_environment_patches, apply_environment_uris
apply_environment_patches()
apply_environment_uris()
from meerschaum.api.dash import connector_listing as cl

masked = {
    'uri': cl.mask_uri('postgresql://u:pw@h:5432/db'),
    'token_uri': cl.mask_uri('https://ghp_abcdef@github.example/x'),
    'db_user_only': cl.mask_uri('postgresql://postgres@localhost/db'),
    'query': cl.mask_uri('mssql://sa:p@h/db?driver=x&password=hunter22&ApiKey=k3y'),
    'names': [cl.is_secret_name(name) for name in ('password', 'api_key', 'client_secret', 'token', 'host', 'username')],
}
rows = cl.list_connectors()
by_keys = {row['keys']: row for row in rows}
text = cl.as_json(rows)
print('JSON:' + json.dumps({
    'masked': masked,
    'leaks': [secret for secret in %r if secret in text],
    'secretdb': {k: by_keys['sql:shellsecret'][k] for k in ('source', 'source_detail', 'flavor', 'host', 'port', 'database')},
    'secretdb_uri': by_keys['sql:shellsecret']['attributes']['uri'],
    'api': {k: by_keys['api:shellapi'][k] for k in ('source', 'host', 'port', 'flavor')},
    'api_password': by_keys['api:shellapi']['attributes'].get('password'),
    'patched': by_keys['api:patched']['source'],
    'patched_attrs': by_keys['api:patched']['attributes'],
    'memory': by_keys['sql:memory']['source'],
    'internal': [keys for keys in by_keys if keys.startswith('api:host')],
    'order': [row['type'] for row in rows][:3],
}))
''' % (SECRETS,)))
    assert out['leaks'] == []
    assert out['masked']['uri'] == 'postgresql://u:••••••@h:5432/db'
    assert out['masked']['token_uri'] == 'https://••••••@github.example/x'
    assert out['masked']['db_user_only'] == 'postgresql://postgres@localhost/db'
    assert 'hunter22' not in out['masked']['query'] and 'k3y' not in out['masked']['query']
    assert out['masked']['names'] == [True, True, True, True, False, False]
    assert out['secretdb'] == {
        'source': 'Environment', 'source_detail': 'MRSM_SQL_SHELLSECRET', 'flavor': 'postgresql',
        'host': '127.0.0.1', 'port': 1, 'database': 'scratch',
    }
    assert 'Sup3rS3cretPw' not in out['secretdb_uri'] and 'scratchuser:' in out['secretdb_uri']
    assert out['api'] == {'source': 'Environment', 'host': '127.0.0.1', 'port': 2, 'flavor': 'http'}
    assert out['api_password'] in (None, '••••••')
    assert out['patched'] == 'Environment'
    assert out['patched_attrs']['client_secret'] == '••••••'
    assert out['patched_attrs']['options']['nested_token'] == '••••••'
    assert 'patch-client-s3cret' not in out['patched_attrs']['options']['note']
    assert out['memory'] == 'Default'
    ### `MRSM_API_HOST=1` (the host's own flag) is not a connector.
    assert out['internal'] == []
    assert out['order'][0] == 'sql'


#: Secrets under names and in shapes a list of secret words misses (the rc15 review found each
#: one in the page's responses): an allowlist shows none of them.
REVIEW_SECRETS = {
    'pass': 'PASS-s3cret-01',
    'ssh_passphrase': 'PHRASE-s3cret-02',
    'dsn': 'host=dsn.invalid user=dsnuser password=DSN-s3cret-03 dbname=dsndb',
    'pw': 'PW-s3cret-04',
    'conn_str': 'Pwd=PWD-s3cret-05;Server=odbc.invalid;Uid=odbcuser',
    'azure': 'AccountKey=AK-s3cret-06;AccountName=acct',
    'cookie': 'session=COOKIE-s3cret-07',
    'args': ['--password', 'ARGS-s3cret-08'],
    'url': 'https://blob.invalid/container?sv=2026&sig=SIG-s3cret-09',
    'webhook': 'https://hooks.invalid/services/T0/B0/HOOK-s3cret-10',
    'pin': 987654321,
    'options': {'driver': 'ODBC Driver 18 for SQL Server', 'Pwd': 'OPT-s3cret-11'},
    'headers': {'Authorization': 'Bearer HDR-s3cret-12'},
    'credentials': {'username': 'creduser', 'password': 'CRED-s3cret-13'},
}
#: Every secret value above, and those of the env connectors of the test below.
REVIEW_SECRET_VALUES = (
    'PASS-s3cret-01', 'PHRASE-s3cret-02', 'DSN-s3cret-03', 'PW-s3cret-04', 'PWD-s3cret-05',
    'AK-s3cret-06', 'COOKIE-s3cret-07', 'ARGS-s3cret-08', 'SIG-s3cret-09', 'HOOK-s3cret-10',
    '987654321', 'OPT-s3cret-11', 'HDR-s3cret-12', 'CRED-s3cret-13',
    'AT-s3cret-14', 'VK-s3cret-15', 'JSON-s3cret-16', 'JP-s3cret-17', 'HK-s3cret-18',
)


def test_connectors_page_shows_only_public_settings(sandbox):
    """
    The page sends the values of connection settings only (flavor, host, port, database, user,
    driver, ...): every other value is masked whatever its name, through the real router response.
    """
    root, plugins = sandbox
    out = _out(run_python(root, plugins, ROUTE + '''
import os, json
os.environ['MRSM_PATCH'] = json.dumps({'meerschaum': {'connectors': {'sql': {'revpatch': dict(
    flavor='postgresql', host='patch.invalid', port=5439, database='patchdb', username='patchuser',
    **%r
)}}}})
### A password holding an unencoded `@`, a Valkey password, a JSON env connector, a webhook URI.
os.environ['MRSM_SQL_ATSECRET'] = 'postgresql://atuser:p@ss-AT-s3cret-14@127.0.0.1:1/atdb'
os.environ['MRSM_VALKEY_REVCACHE'] = 'redis://:VK-s3cret-15@127.0.0.1:1/0'
os.environ['MRSM_SQL_JSONSECRET'] = json.dumps({
    'flavor': 'postgresql', 'host': '127.0.0.1', 'port': 1, 'database': 'jsondb',
    'username': 'jsonuser', 'password': 'JSON-s3cret-16', 'pass': 'JP-s3cret-17',
})
os.environ['MRSM_PATCH'] = json.dumps({'meerschaum': {'connectors': {
    **json.loads(os.environ['MRSM_PATCH'])['meerschaum']['connectors'],
    'hookish': {'main': {'host': 'hooks.invalid', 'uri': 'https://hooks.invalid/services/HK-s3cret-18'}},
}}})
from meerschaum.config.environment import apply_environment_patches, apply_environment_uris
apply_environment_patches()
apply_environment_uris()
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
from meerschaum.api.dash import dash_app, sessions
from meerschaum.api.dash import connector_listing as cl
app = build_app()
client = TestClient(app)
client.get('/dash/_dash-layout')
sign_in('shelluser', user_type='admin', session_id='00000000-0000-4000-8000-0000005e1101')
client.cookies.set('mrsm-session-id', '00000000-0000-4000-8000-0000005e1101')
page = route(client, dash_app, '/dash/connectors')
rows = {row['keys']: row for row in cl.list_connectors()}
assert find(page, 'connector-info-modal') is not None
assert not [n for n in walk(page) if n.get('props', {}).get('className') == 'connectors-attributes']
callback = next(key for key in dash_app.callback_map if 'connector-inspect-content' in key)
inspections = []
for keys in ('sql:revpatch','sql:atsecret','valkey:revcache','sql:jsonsecret','hookish:main'):
    button = json.dumps({'type':'connector-inspect-button','index':keys},sort_keys=True,separators=(',',':'))
    response = client.post('/dash/_dash-update-component',json={
        'output':callback,
        'outputs':{'id':{'type':'connector-inspect-content','index':keys},'property':'children'},
        'inputs':[{'id':button,'property':'n_clicks','value':1}],
        'state':[{'id':'session-store','property':'data','value':{'session-id':'00000000-0000-4000-8000-0000005e1101'}}],
        'changedPropIds':[button+'.n_clicks'],
    })
    assert response.status_code == 200, response.text[-2000:]
    inspections.append(next(iter(response.json()['response'].values()))['children'])
text = json.dumps([page,*inspections], ensure_ascii=False)
pre = [n['props'].get('children') for n in walk(inspections) if n.get('props', {}).get('className') == 'connectors-attributes']
print('JSON:' + json.dumps({
    'leaks': [secret for secret in %r if secret in text or secret in cl.as_json(list(rows.values()))],
    'escaped_mask': '\\\\u2022' in text,
    'shown_mask': any('••••••' in str(p) for p in pre),
    'revpatch': rows['sql:revpatch']['attributes'],
    'revpatch_cols': {k: rows['sql:revpatch'][k] for k in ('flavor', 'host', 'port', 'database')},
    'at': {k: rows['sql:atsecret'][k] for k in ('host', 'port', 'database')},
    'at_uri': rows['sql:atsecret']['attributes'].get('uri'),
    'valkey': {k: rows['valkey:revcache'][k] for k in ('flavor', 'host', 'port', 'database')},
    'json': rows['sql:jsonsecret']['attributes'],
    'hook': {k: rows['hookish:main'][k] for k in ('host', 'database')},
    'hook_uri': rows['hookish:main']['attributes'].get('uri'),
    'public': [name for name in sorted(cl.PUBLIC_ATTRIBUTES) if cl.is_secret_name(name)],
    'text': {
        'error': cl.mask_text('FATAL: password=hunter22 for postgresql://u:p@ss@h/db?sig=abc "api_key": "K3Y42" Authorization: Bearer abcdef123456'),
        'scrub': cl.scrub('could not reach ss-AT-s3cret-14@127.0.0.1 (p%%40ss)', ['p@ss-AT-s3cret-14', 'p%%40ss']),
    },
    'strings': [
        cl.mask_connection_string('Driver={ODBC Driver 18; x};Server=tcp:h,1433;Uid=u;Pwd=p w;', 'sql'),
        cl.mask_connection_string("host=h user=u password='a b' dbname=d", 'sql'),
        cl.mask_connection_string('sqlite:////srv/data/x.db', 'sql'),
        cl.mask_connection_string('https://tok3n@github.invalid/x', 'api'),
        cl.mask_connection_string('https://user:pw', 'api'),
        cl.mask_connection_string('just-a-token-value', 'api'),
        cl.mask_connection_string("mssql://sa:p@h/db?application_name=O'Brien&magic=m4gic&Encrypt=no", 'sql'),
    ],
}, ensure_ascii=False))
''' % (REVIEW_SECRETS, REVIEW_SECRET_VALUES)))
    assert out['leaks'] == []
    ### The masks read as dots, not `\\u2022` escapes.
    assert out['escaped_mask'] is False and out['shown_mask'] is True
    attributes = out['revpatch']
    ### Connection settings are shown; everything else is masked whatever its name.
    assert attributes['host'] == 'patch.invalid' and attributes['port'] == 5439
    assert attributes['username'] == 'patchuser' and attributes['database'] == 'patchdb'
    for name in ('pass', 'ssh_passphrase', 'pw', 'azure', 'cookie', 'args', 'url', 'webhook', 'pin',
                 'headers', 'credentials'):
        value = attributes[name]
        assert value == '••••••' or value == {'Authorization': '••••••'}, (name, value)
    assert attributes['dsn'] == 'host=dsn.invalid user=dsnuser password=•••••• dbname=dsndb'
    assert attributes['conn_str'] == 'Pwd=••••••;Server=odbc.invalid;Uid=odbcuser'
    assert attributes['options'] == {'driver': 'ODBC Driver 18 for SQL Server', 'Pwd': '••••••'}
    assert out['revpatch_cols'] == {
        'flavor': 'postgresql', 'host': 'patch.invalid', 'port': 5439, 'database': 'patchdb',
    }
    ### The part of an `@`-holding password a URI parser leaves before the host is not shown.
    assert out['at']['host'] == '127.0.0.1' and out['at']['database'] == 'atdb'
    assert out['at_uri'] == 'postgresql://atuser:••••••@127.0.0.1:1/atdb'
    assert out['valkey'] == {'flavor': 'redis', 'host': '127.0.0.1', 'port': 1, 'database': '0'}
    assert out['json']['username'] == 'jsonuser' and out['json']['pass'] == '••••••'
    ### A URI path is a database only for databases: a webhook's path is masked.
    assert out['hook'] == {'host': 'hooks.invalid', 'database': None}
    assert out['hook_uri'] == 'https://hooks.invalid/••••••'
    ### No public name looks like a secret's.
    assert out['public'] == []
    assert out['text']['error'] == (
        'FATAL: password=•••••• for postgresql://u:••••••@h/db?sig=•••••• "api_key": •••••• '
        'Authorization: •••••• ••••••'
    )
    assert out['text']['scrub'] == 'could not reach ••••••@127.0.0.1 (••••••)'
    assert out['strings'] == [
        'Driver={ODBC Driver 18; x};Server=tcp:h,1433;Uid=u;Pwd=••••••;',
        'host=h user=u password=•••••• dbname=d',
        'sqlite:////srv/data/x.db',
        'https://••••••@github.invalid/••••••',
        'https://user:••••••',
        '••••••',
        ### A quote means nothing in a URL query: the next setting is still judged on its own.
        "mssql://sa:••••••@h/db?application_name=O'Brien&magic=••••••&Encrypt=no",
    ]


def test_connectors_are_for_administrators_under_secure(sandbox):
    """
    `--secure` (`api:permissions:actions:non_admin: false`): the Connectors page and its link are
    for administrators, like the Terminal; anyone may still not test a connection but an admin.
    """
    root, plugins = sandbox
    out = _out(run_python(root, plugins, ROUTE + '''
import json
import meerschaum as mrsm
from meerschaum.core import User
from meerschaum.config import get_config
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
from meerschaum.api.dash import dash_app, sessions
from meerschaum.api.dash.callbacks.connectors_page import test_connection_click
conn = mrsm.get_connector('sql:apihosttest')
for username, typ in (('secadmin', 'admin'), ('secuser', None)):
    user = User(username, 'correct-horse-9', instance=conn, **({'type': typ} if typ else {}))
    if conn.get_user_id(user) is None:
        assert conn.register_user(user)[0]
app = build_app()
client = TestClient(app)
client.get('/dash/_dash-layout')
admin_id, user_id = '00000000-0000-4000-8000-00000000a0a1', '00000000-0000-4000-8000-00000000a0a2'
sessions.set_session(admin_id, {'username': 'secadmin'})
sessions.set_session(user_id, {'username': 'secuser'})

def page_of(session_id):
    client.cookies.set('mrsm-session-id', session_id)
    page = route(client, dash_app, '/dash/connectors', session_id=session_id)
    return {
        'ids': [i for i in ids(page) if isinstance(i, str)],
        'text': json.dumps(page),
    }

admin, user = page_of(admin_id), page_of(user_id)
refused = test_connection_click(1, {'session-id': user_id})
print('JSON:' + json.dumps({
    'non_admin': get_config('api', 'permissions', 'actions', 'non_admin'),
    'admin': ['connectors-table' in admin['ids'], 'console-nav-connectors' in admin['ids']],
    ### (The instance selector names instances; the list's sources and databases are not sent.)
    'user': ['connectors-table' in user['ids'], 'console-nav-connectors' in user['ids'],
             'The connectors are not available to your account' in user['text'],
             'MRSM_SQL_APIHOSTTEST' in user['text'] or 'instance.db' in user['text']],
    'admin_sees': 'MRSM_SQL_APIHOSTTEST' in admin['text'],
    'refused': str(refused),
}))
''', extra_env={'MRSM_CONFIG': '{"api": {"permissions": {"actions": {"non_admin": false}}}}'}))
    assert out['non_admin'] is False
    assert out['admin'] == [True, True] and out['admin_sees'] is True
    ### A non-administrator gets neither the list nor its link.
    assert out['user'] == [False, False, True, False]
    assert 'Only administrators can test connections.' in out['refused']


def test_connection_test_reports_without_secrets(sandbox):
    root, plugins = sandbox
    out = _out(run_python(root, plugins, '''
import os, json, time
os.environ['MRSM_SQL_SHELLBAD'] = 'postgresql://scratchuser:Sup3rS3cretPw@127.0.0.1:1/scratch'
os.environ['MRSM_CONNECT_TIMEOUT'] = '3'
from meerschaum.config.environment import apply_environment_uris
apply_environment_uris()
from meerschaum.api.dash.connector_listing import test_connector, connect_timeout
good = test_connector('sql:apihosttest')
started = time.perf_counter()
bad = test_connector('sql:shellbad')
missing = test_connector('sql:not_defined_anywhere')
print('JSON:' + json.dumps({
    'good': good['success'], 'bad': bad, 'missing': missing['success'],
    'elapsed': time.perf_counter() - started, 'timeout': connect_timeout(),
}))
'''))
    assert out['good'] is True
    assert out['bad']['success'] is False
    assert 'Sup3rS3cretPw' not in out['bad']['message'] and out['bad']['message']
    assert out['missing'] is False
    assert out['timeout'] == 3.0
    assert out['elapsed'] < 30


def test_router_serves_the_shell_pages_with_the_navbar(sandbox):
    root, plugins = sandbox
    out = _out(run_python(root, plugins, ROUTE + '''
import json
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
from meerschaum.api.dash import dash_app, sessions
from meerschaum.api.dash.callbacks.dashboard import _paths, _pages, terminal_redirect
app = build_app()
client = TestClient(app)
client.get('/dash/_dash-layout')
sign_in('shelluser', user_type='admin', session_id='00000000-0000-4000-8000-0000005e1101')
client.cookies.set('mrsm-session-id', '00000000-0000-4000-8000-0000005e1101')
layout = client.get('/dash/_dash-layout').text

pages = {}
for path in ('/dash', '/dash/terminal', '/dash/webterm', '/dash/connectors', '/dash/jobs',
             '/dash/pipelines', '/dash/tokens', '/dash/users', '/dash/plugins', '/dash/password-reset'):
    page = route(client, dash_app, path)
    nav = find(page, 'pages-navbar-div')
    active = [n['props'].get('id') for n in walk(nav) if n.get('props', {}).get('active') is True]
    pages[path] = {
        'nav': (nav or {}).get('props', {}).get('data-mrsm-nav'),
        'active': active,
        'ids': [i for i in ids(page) if isinstance(i, str)],
        'user': [n['props'].get('children') for n in walk(nav) if 'console-nav-username' in str(n.get('props', {}).get('className', ''))],
    }
redirect = route(client, dash_app, '/dash', query='?action=sync&subaction=pipes')
print('JSON:' + json.dumps({
    'offcanvas': 'pages-offcanvas' in layout,
    'pages': pages,
    'redirect': [n['props'].get('data-mrsm-redirect') for n in walk(redirect) if n.get('props', {}).get('data-mrsm-redirect')],
    'redirects': [terminal_redirect('/dash', 'http://x/dash?instance=sql:main'),
                  terminal_redirect('/dash', 'http://x/dash/'),
                  terminal_redirect('/dash', 'http://x/dash/nope?action=show'),
                  terminal_redirect('/dash', 'http://x/dash?metric_keys=a,b')],
    'pages_table': sorted(_pages),
}))
'''))
    assert out['offcanvas'] is False
    for path, page in out['pages'].items():
        assert page['nav'] == 'console', path
        assert page['user'] == ['shelluser'], path
    assert 'overview-page' in out['pages']['/dash']['ids']
    assert out['pages']['/dash']['active'] == []
    terminal = out['pages']['/dash/terminal']
    assert 'go-button' in terminal['ids'] and 'instance-select' in terminal['ids']
    assert 'pages-instance-select' not in terminal['ids']
    assert 'get-items-menu' not in terminal['ids'] and 'get-runs-button' not in terminal['ids']
    assert out['pages']['/dash/webterm']['ids'] == terminal['ids']
    assert 'connectors-table' in out['pages']['/dash/connectors']['ids']
    assert out['pages']['/dash/connectors']['active'] == ['console-nav-connectors']
    assert out['pages']['/dash/jobs']['active'] == ['console-nav-jobs']
    assert out['pages']['/dash/pipelines']['active'] == ['console-nav-pipelines']
    assert out['pages']['/dash/tokens']['active'] == ['console-nav-tokens']
    assert out['pages']['/dash/password-reset']['active'] == ['console-nav-settings']
    for path in ('/dash/jobs', '/dash/tokens', '/dash/pipelines'):
        assert 'pages-instance-select' in out['pages'][path]['ids'], path
    ### The host runs without the web terminal here: no Terminal link.
    assert all('console-nav-terminal' not in page['ids'] for page in out['pages'].values())
    assert out['redirect'] == ['/dash/terminal?action=sync&subaction=pipes']
    assert out['redirects'] == [None, None, None, '/dash/terminal?metric_keys=a,b']
    assert {'Overview', 'Pipes', 'Pipelines', 'Jobs', 'Connectors', 'Plugins', 'Terminal'} <= set(out['pages_table'])
    assert 'Runs' not in out['pages_table'] and 'Web Console' not in out['pages_table']


def test_navbar_follows_permissions_and_plugin_pages(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'shellpages', '''
from meerschaum.plugins import web_page

@web_page('shellpages/plain', login_required=False, page_group='Shell Reports')
def plain():
    from dash import html
    return html.Div('plain page', id='shell-plain')

@web_page('shellpages/withnav', login_required=False, page_group='Shell Reports', navbar=True)
def withnav():
    from dash import html
    return html.Div('nav page', id='shell-withnav')

@web_page('shellpages/bare', login_required=False, skip_navbar=True)
def bare():
    from dash import html
    return [html.Div('bare page', id='shell-bare')]
''')
    out = _out(run_python(root, plugins, ROUTE + '''
import json
from unittest import mock
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
from meerschaum.api.dash import dash_app, sessions, components
from meerschaum.api.dash.callbacks.dashboard import _paths
app = build_app()
client = TestClient(app)
client.get('/dash/_dash-layout')
sign_in('shelluser', user_type='admin', session_id='00000000-0000-4000-8000-0000005e1101')
client.cookies.set('mrsm-session-id', '00000000-0000-4000-8000-0000005e1101')

def nav_kind(path):
    page = route(client, dash_app, path)
    nav = find(page, 'pages-navbar-div')
    return (nav or {}).get('props', {}).get('data-mrsm-nav')

kinds = {path: nav_kind(path) for path in ('/dash/shellpages/plain', '/dash/shellpages/withnav', '/dash/shellpages/bare')}
groups = components.get_plugin_nav_pages()

import meerschaum.api.dash.webterm as webterm
with mock.patch.object(webterm, 'webterm_enabled', lambda: True):
    with components.console_nav_context('/dash/terminal', '00000000-0000-4000-8000-0000005e1101'):
        admin_nav = components.build_pages_navbar()
    ### The terminal needs `webterm:use` (the Operator role and administrators): an Editor's bar
    ### has no Terminal; an Operator's has.
    editor = sign_in('shelleditor', role='editor')['session-id']
    operator = sign_in('shelloperator', role='operator')['session-id']
    with components.console_nav_context('/dash/terminal', editor):
        secure_nav = components.build_pages_navbar()
    with components.console_nav_context('/dash/terminal', operator):
        operator_nav = components.build_pages_navbar()
with components.console_nav_context('/dash/plugins', None):
    anonymous_nav = components.build_pages_navbar()

def nav_ids(nav):
    return [i for i in ids(nav.to_plotly_json() if hasattr(nav, 'to_plotly_json') else nav) if isinstance(i, str)]

import plotly.utils
def plain(component):
    return json.loads(json.dumps(component, cls=plotly.utils.PlotlyJSONEncoder))

print('JSON:' + json.dumps({
    'kinds': kinds,
    'groups': groups,
    'admin': ids(plain(admin_nav)),
    'secure': ids(plain(secure_nav)),
    'operator': ids(plain(operator_nav)),
    'anonymous': ids(plain(anonymous_nav)),
    'sections': [components.section_of_path(p) for p in (
        '/dash', '/dash/', '/dash/pipes/sql:main/x', '/dash/pipelines/daily', '/dash/runs',
        '/dash/webterm', '/dash/password-reset', '/dash/shellpages/plain')],
}))
'''))
    assert out['kinds'] == {
        '/dash/shellpages/plain': 'plugin',
        '/dash/shellpages/withnav': 'console',
        '/dash/shellpages/bare': None,
    }
    ### The plugins' pages are the Pages menu, grouped; the console's own Settings page is not.
    assert out['groups'] == {
        'Shell Reports': [
            ['Shellpages / Plain', '/dash/shellpages/plain'],
            ['Shellpages / Withnav', '/dash/shellpages/withnav'],
        ],
        ### A page without a group is listed under its plugin.
        'Shellpages': [['Shellpages / Bare', '/dash/shellpages/bare']],
    }
    assert 'console-nav-terminal' in out['admin']
    assert 'console-nav-terminal' not in out['secure']
    assert 'console-nav-terminal' in out['operator']
    assert 'sign-out-button' in out['admin']
    ### Not signed in: Sign in instead of the user menu.
    assert 'console-nav-sign-in' in out['anonymous'] and 'sign-out-button' not in out['anonymous']
    assert out['sections'] == [
        'overview', 'overview', 'pipes', 'pipelines', 'jobs', 'terminal', 'settings', None,
    ]


def test_overview_summaries(sandbox):
    root, plugins = sandbox
    out = _out(run_python(root, plugins, '''
import json
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
import meerschaum as mrsm
from meerschaum.api.dash import dash_app, overview

instance = mrsm.get_connector('sql:apihosttest')
for metric in ('alpha', 'beta', 'gamma'):
    pipe = mrsm.Pipe('shell', metric, instance=instance)
    if pipe.get_id() is None:
        assert pipe.register()[0]
internal = mrsm.Pipe('mrsm', 'pipeline', 'shell-line', instance=instance)
if internal.get_id() is None:
    internal.register()

ref = lambda metric: overview.pipe_ref(str(instance), 'shell', metric, None)
statuses = {
    ref('alpha'): {'consecutive_failures': 2, 'success': False},
    ref('beta'): {'consecutive_failures': 0, 'success': True},
}
had = hasattr(mrsm, 'get_sync_statuses')
original = getattr(mrsm, 'get_sync_statuses', None)
mrsm.get_sync_statuses = lambda instance=None: statuses
with_statuses = overview.pipes_summary(instance)
if original is None:
    del mrsm.get_sync_statuses
else:
    mrsm.get_sync_statuses = original

def job(status, health=None, result=(True, 'ok'), name='job'):
    return SimpleNamespace(name=name, status=status, health=health, result=result)

states = [overview.basic_job_state(j) for j in (
    job('running', {'state': 'healthy'}), job('running', {'state': 'stalled'}),
    job('running', {'state': 'failing'}), job('paused'), job('stopped'),
    job('stopped', result=(False, 'boom')), job('running', None),
)]

now = datetime.now(timezone.utc)
def run(run_id, status, minutes, targets, selection=None):
    started = (now - timedelta(minutes=minutes)).isoformat().replace('+00:00', 'Z')
    return {'run': {'run_id': run_id, 'started': started, 'targets': targets,
                    'selection': json.dumps(selection or {})},
            'status': status, 'duration_seconds': 1.5, 'counts': {'succeeded': 1, 'failed': 0}}
runs = [
    run('pipeline-a', 'succeeded', 1, ['i|shell|alpha|None']),
    run('pipeline-b', 'succeeded', 2, ['i|shell|alpha|None']),
    run('daily-1', 'failed', 30, ['i|shell|beta|None'],
        {'pipeline_definitions': [{'name': 'daily', 'id': 'x'}]}),
    run('pipeline-c', 'succeeded', 3, ['i|shell|alpha|None', 'i|shell|beta|None']),
]
overview._get_runs = lambda instance, limit, **kwargs: list(runs)
overview._list_pipelines = lambda instance, **kwargs: {'daily': {'name': 'daily', 'targets': []}}
from meerschaum.jobs import Job
real_state = overview.job_state(Job('shell-overview-job', 'show version', executor_keys='local'))
recent = overview.recent_runs(instance)
pipelines = overview.pipelines_summary(instance)
body = overview.build_runs_body(recent)

print('JSON:' + json.dumps({
    'without': overview.pipes_summary(instance) if not had else None,
    'with': with_statuses,
    'states': states,
    'real_state': real_state,
    'recent': [[r['run_id'], r['status'], r['runs'], r['pipeline']] for r in recent],
    'pipelines': pipelines,
    'ago': [overview.time_ago((now - timedelta(seconds=s)).isoformat(), now) for s in (5, 300, 7200, 172800)],
    'durations': [overview.format_duration(s) for s in (0.42, 12, 300, 7500)],
    'counts': overview.counts_text({'succeeded': 2, 'skipped': 0, 'upstream_failed': 1}),
    'query': overview.with_query('/dash/pipes', status='failing', instance=None),
    'empty_runs': str(overview.build_runs_body([])),
}, default=str))
'''))
    if out['without'] is not None:
        assert out['without'] == {'total': 3, 'failing': None, 'statuses': False}
    ### Internal `mrsm` pipes (saved Pipelines' registry) do not count.
    assert out['with'] == {'total': 3, 'failing': 1, 'statuses': True}
    assert out['states'] == ['running', 'stalled', 'failing', 'paused', 'stopped', 'failing', 'running']
    ### A real job's state is one the Jobs page filters by.
    assert out['real_state'] in ('running', 'stalled', 'failing', 'paused', 'stopped')
    ### Failures first, then the newest; a job syncing the same pipe again and again is one row.
    assert out['recent'] == [
        ['daily-1', 'failed', 1, 'daily'],
        ['pipeline-a', 'succeeded', 2, None],
        ['pipeline-c', 'succeeded', 1, None],
    ]
    assert out['pipelines']['count'] == 1
    assert out['pipelines']['pipelines'][0]['status'] == 'failed'
    assert out['pipelines']['pipelines'][0]['run_id'] == 'daily-1'
    assert out['ago'] == ['just now', '5 min ago', '2 h ago', '2 d ago']
    assert out['durations'] == ['0.4 s', '12 s', '5 min', '2 h 5 min']
    assert out['counts'] == '2 succeeded · 1 upstream failed'
    assert out['query'] == '/dash/pipes?status=failing'
    assert 'No recent runs' in out['empty_runs']


def test_overview_tiles_render_for_a_session(sandbox):
    root, plugins = sandbox
    out = _out(run_python(root, plugins, SIGN_IN + '''
import os, json, pathlib
### A fresh instance of its own: the other tests of this module register pipes.
os.environ['MRSM_SQL_TILESDB'] = 'sqlite:///' + str(pathlib.Path.cwd() / 'tiles.db')
from meerschaum.config.environment import apply_environment_uris
apply_environment_uris()
import plotly.utils
from dash.exceptions import PreventUpdate
import meerschaum as mrsm
from meerschaum.api.dash import dash_app, sessions
import meerschaum.api.dash.callbacks.overview as co

sign_in('tileuser', session_id='00000000-0000-4000-8000-00000071e001')
store = {'instance': 'sql:tilesdb', 'query': None}
session = {'session-id': '00000000-0000-4000-8000-00000071e001'}
try:
    co.render_pipes_tile(0, store, None, {})
    anonymous = 'updated'
except PreventUpdate:
    anonymous = 'prevented'
empty = co.render_pipes_tile(0, store, None, session)
pipe = mrsm.Pipe('tiles', 'one', instance='sql:tilesdb')
if pipe.get_id() is None:
    assert pipe.register()[0]
filled = co.render_pipes_tile(1, store, None, session)
again = co.render_pipes_tile(2, store, filled[1], session)
jobs = co.render_jobs_tile(0, None, session)
runs = co.render_runs_tile(0, store, None, session)
pipelines = co.render_pipelines_tile(0, store, None, session)
dump = lambda value: json.loads(json.dumps(value, cls=plotly.utils.PlotlyJSONEncoder))

### The page: the empty state is the Pipes tile's own; every tile is always there.
from meerschaum.api.dash.components import console_nav_context
from meerschaum.api.dash.pages import overview as overview_page
with console_nav_context('/dash', '00000000-0000-4000-8000-00000071e001'):
    layout = dump(overview_page.route('/dash', 'http://x/dash?instance=sql:tilesdb'))

def _ids(node):
    found = []
    def visit(node):
        if isinstance(node, list):
            for child in node:
                visit(child)
        elif isinstance(node, dict):
            props = node.get('props', {}) or {}
            if props.get('id'):
                found.append(props['id'])
            visit(props.get('children', None))
    visit(node)
    return found

def _find(node, component_id):
    if isinstance(node, list):
        for child in node:
            hit = _find(child, component_id)
            if hit is not None:
                return hit
    elif isinstance(node, dict):
        props = node.get('props', {}) or {}
        if props.get('id') == component_id:
            return node
        return _find(props.get('children', None), component_id)
    return None

### Dash fills its callback map on the app's first request.
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
TestClient(build_app()).get('/dash/_dash-layout')
outputs = next(key for key in dash_app.callback_map if 'overview-pipes-body.children' in key)
print('JSON:' + json.dumps({
    'layout': {
        'pipes_tile': _ids(_find(layout, 'overview-pipes')),
        'grid': [i for i in _ids(_find(layout, 'overview-tiles')) if i in (
            'overview-pipes', 'overview-pipelines', 'overview-jobs', 'overview-runs', 'overview-empty')],
        'tiles_hidden': (_find(layout, 'overview-tiles') or {}).get('props', {}).get('hidden'),
    },
    'outputs': outputs,
    'anonymous': anonymous,
    'empty': [empty[2], empty[3]],
    'filled': [filled[2], filled[3], 'overview-pipes-total' in json.dumps(dump(filled[0]))],
    'unchanged': [type(again[0]).__name__, type(again[1]).__name__],
    'jobs': 'overview-jobs-total' in json.dumps(dump(jobs[0])),
    'runs': json.dumps(dump(runs[0])),
    'pipelines': json.dumps(dump(pipelines[0])),
}))
'''))
    assert out['anonymous'] == 'prevented'
    ### No pipes: the Pipes tile shows the empty state instead of its numbers (`[empty hidden,
    ### body hidden]`); the other tiles stay (the review saw the server's failing jobs vanish).
    assert out['empty'] == [False, True]
    assert 'overview-empty' in out['layout']['pipes_tile']
    assert 'overview-pipes-body' in out['layout']['pipes_tile']
    assert out['layout']['grid'] == [
        'overview-pipes', 'overview-empty', 'overview-pipelines', 'overview-jobs', 'overview-runs',
    ]
    assert out['layout']['tiles_hidden'] in (None, False)
    assert 'overview-pipes-body.hidden' in out['outputs'] and 'overview-tiles' not in out['outputs']
    assert out['filled'] == [True, False, True]
    ### The same tile is not sent again.
    assert out['unchanged'] == ['NoUpdate', 'NoUpdate']
    assert out['jobs']
    ### A new instance: no runs, and New pipeline instead of a list.
    assert 'No recent runs' in out['runs']
    assert 'New pipeline' in out['pipelines'] and 'overview-pipelines-list' not in out['pipelines']


def test_plugin_connector_types_and_runtime_connectors(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'shelltype', '''
from meerschaum.connectors import make_connector, Connector

@make_connector
class ShellwidgetConnector(Connector):
    REQUIRED_ATTRIBUTES = ['host']

    def test_connection(self):
        return True, "widget answered"
''')
    out = _out(run_python(root, plugins, '''
import os, json
os.environ['MRSM_PATCH'] = json.dumps({'meerschaum': {'connectors': {'shellwidget': {'main': {
    'host': 'widget.invalid', 'api_key': 'Sup3rS3cretPw',
}}}}})
from meerschaum.config.environment import apply_environment_patches
apply_environment_patches()
import meerschaum as mrsm
### The host loads the plugins (and so their connector types), as the console's server does.
from meerschaum.api.host import build_app
build_app()
### A connector a plugin builds while running (not in the configuration).
mrsm.get_connector('sql', 'shellruntime', flavor='sqlite', database=':memory:')
from meerschaum.api.dash.connector_listing import list_connectors, test_connector, as_json
rows = {row['keys']: row for row in list_connectors()}
print('JSON:' + json.dumps({
    'widget': {k: rows['shellwidget:main'][k] for k in ('type', 'plugin', 'source', 'host', 'testable')},
    'widget_key': rows['shellwidget:main']['attributes']['api_key'],
    'runtime': {k: rows['sql:shellruntime'][k] for k in ('source', 'flavor', 'database')},
    'test': test_connector('shellwidget:main'),
    'leak': 'Sup3rS3cretPw' in as_json(list(rows.values())),
}))
'''))
    assert out['widget'] == {
        'type': 'shellwidget', 'plugin': 'shelltype', 'source': 'Environment',
        'host': 'widget.invalid', 'testable': True,
    }
    assert out['widget_key'] == '••••••'
    assert out['runtime'] == {'source': 'Runtime', 'flavor': 'sqlite', 'database': ':memory:'}
    assert out['test']['success'] is True and out['test']['message'] == 'widget answered'
    assert out['leak'] is False
