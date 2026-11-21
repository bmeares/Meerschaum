"""Publishing and complete token copying through the real console and a local API."""

import io
import json
import tarfile
import urllib.request

import pytest

from tests.console.conftest import PASSWORD, USERNAME, Server, Tab
from tests.console.test_credential_interop import login, python_json, request

pytestmark = pytest.mark.console


@pytest.fixture(scope='module')
def publishing_server():
    server = Server('rc30-publish', native=False,
                    config={'api': {'webterm': {'tmux': {'enabled': False}}}})
    # Every repository in this test points to this fixture, including the default label.
    for label in ('MRSM', 'RC30PASSWORD', 'RC30CLIENT', 'RC30KEY', 'RC30CONFIGURED',
                  'RC30PASSWORD_SAVED', 'RC30CLIENT_SAVED', 'RC30KEY_SAVED'):
        server.env[f'MRSM_API_{label}'] = server.base_url
    config = json.loads(server.env['MRSM_CONFIG'])
    config['meerschaum']['connectors'] = {'api': {'rc30configured': {
        'host': '127.0.0.1', 'port': server.port, 'protocol': 'http',
        'username': USERNAME, 'password': PASSWORD,
    }}}
    # URI variables supply the address; configured credentials exercise automatic login.
    server.env['MRSM_CONFIG'] = json.dumps(config)
    for method in ('password', 'client', 'key', 'configured', 'password_saved', 'client_saved', 'key_saved'):
        (server.plugins / f'rc30_{method}.py').write_text(
            f'"""Publish test {method}."""\n__version__ = "1.2.3"\nVALUE = "source"\n')
    try:
        server.start()
        server.credential = python_json(server, '''
owner = User('alice', instance=conn)
token = Token(label='publisher', user=owner, scopes=['*'], instance=conn)
assert token.register()[0]
print('INTEROP:' + json.dumps({'id': str(token.id), 'secret': token.secret, 'key': token.get_api_key()}))
''')
        yield server
    finally:
        server.stop()


@pytest.mark.parametrize('method,persist', [
    ('password', False), ('client', False), ('key', False), ('configured', False),
    ('password', True), ('client', True), ('key', True),
])
def test_publish_authentication_and_current_source(publishing_server, browser, method, persist):
    server = publishing_server
    context = browser.new_context(viewport={'width': 1200, 'height': 900})
    tab = Tab(context.new_page(), server.base_url)
    page = tab.page
    suffix = method + ('_saved' if persist else '')
    name = f'rc30_{suffix}'
    repository = f'api:rc30{suffix}'
    try:
        tab.login(path='/dash')
        tab.goto(f'/dash/plugins/installed/{name}', '#plugin-publish-open')
        page.click('#plugin-publish-open')
        page.locator('#plugin-publish-modal').wait_for()
        page.select_option('#plugin-publish-repository', repository)
        if method != 'configured':
            page.locator('#plugin-publish-login').wait_for(state='visible')
            if persist:
                page.locator('#plugin-publish-persist').check()
            if method == 'password':
                page.fill('#plugin-publish-username', USERNAME)
                page.fill('#plugin-publish-password', 'wrong-password')
                page.click('#plugin-publish-login')
                page.get_by_text('Sign-in failed. Check your credentials and try again.').wait_for()
                assert page.locator('#plugin-publish-submit').is_disabled()
                page.fill('#plugin-publish-password', PASSWORD)
            elif method == 'client':
                page.locator('#plugin-publish-method label', has_text='Client ID and secret').click()
                page.fill('#plugin-publish-client-id', server.credential['id'])
                page.fill('#plugin-publish-client-secret', server.credential['secret'])
            else:
                page.locator('#plugin-publish-method label', has_text='API key').click()
                page.fill('#plugin-publish-api-key', server.credential['key'])
            page.click('#plugin-publish-login')
        page.wait_for_function("!document.querySelector('#plugin-publish-submit')?.disabled")
        assert not page.locator('#plugin-publish-auth').is_visible()
        for field in ('password', 'client-secret', 'api-key'):
            assert page.locator(f'#plugin-publish-{field}').input_value() == ''
        if method != 'configured':
            stored = page.evaluate("JSON.parse(sessionStorage.getItem('plugin-publish-credentials'))")
            entries = stored.get('repositories', {})
            assert (repository in entries) is (not persist)
            if not persist:
                assert entries[repository]['credentials']
            # A reload retains temporary browser credentials; saved credentials also work in
            # a fresh server-side connector, so neither path relies on a server memory cache.
            tab.goto(f'/dash/plugins/installed/{name}', '#plugin-publish-open')
            page.click('#plugin-publish-open')
            page.select_option('#plugin-publish-repository', repository)
            page.wait_for_function("!document.querySelector('#plugin-publish-submit')?.disabled")
            assert not page.locator('#plugin-publish-auth').is_visible()
            saved = python_json(server, f'''
from meerschaum.config._read_config import read_config
attrs = read_config(keys=['meerschaum'], write_missing=False, substitute=False)['meerschaum'].get('connectors', {{}}).get('api', {{}}).get({repository.split(':', 1)[1]!r}, {{}})
print('INTEROP:' + json.dumps({{'saved': any(attrs.get(key) for key in ('password', 'client_secret', 'api_key'))}}))
''')
            assert saved['saved'] is persist
        # Package the current on-disk source, even when it changed after opening the dialog.
        path = server.plugins / f'{name}.py'
        path.write_text(path.read_text().replace('source', 'updated'))
        page.click('#plugin-publish-submit')
        page.get_by_text(f"Published '{name}' to {repository}.", exact=True).wait_for(timeout=30000)
        caller = login(server)
        status, attributes = request(server, f'/plugins/{name}/attributes', caller)
        assert status == 200, attributes
        assert attributes['description'] == f'Publish test {suffix}.'
        download = urllib.request.Request(server.base_url + f'/plugins/{name}',
                                         headers={'Authorization': 'Bearer ' + caller})
        with urllib.request.urlopen(download, timeout=30) as response:
            with tarfile.open(fileobj=io.BytesIO(response.read()), mode='r:gz') as archive:
                assert archive.extractfile(f'{name}.py').read().decode() == path.read_text()
        assert not tab.errors, tab.errors
        if method == 'password' or persist:
            # Saved credentials work in another browser; temporary credentials stay here.
            other = browser.new_context()
            try:
                visitor = Tab(other.new_page(), server.base_url)
                visitor.login(path='/dash')
                visitor.goto(f'/dash/plugins/installed/{name}', '#plugin-publish-open')
                visitor.page.click('#plugin-publish-open')
                visitor.page.select_option('#plugin-publish-repository', repository)
                if persist:
                    visitor.page.wait_for_function("!document.querySelector('#plugin-publish-submit')?.disabled")
                    assert not visitor.page.locator('#plugin-publish-auth').is_visible()
                else:
                    visitor.page.locator('#plugin-publish-login').wait_for(state='visible')
                    assert visitor.page.locator('#plugin-publish-submit').is_disabled()
            finally:
                other.close()
    except Exception:
        print('Publish dialog:', page.locator('#plugin-publish-alert').inner_text())
        print('Browser errors:', tab.errors)
        raise
    finally:
        context.close()


def test_copy_token_details_includes_every_displayed_value(publishing_server, browser):
    context = browser.new_context(viewport={'width': 1200, 'height': 900},
                                  permissions=['clipboard-read', 'clipboard-write'])
    tab = Tab(context.new_page(), publishing_server.base_url)
    page = tab.page
    try:
        tab.login(path='/dash')
        tab.goto('/dash/tokens', '#tokens-create-button')
        page.click('#tokens-create-button')
        page.locator('#tokens-register-button').wait_for()
        page.fill('#tokens-name-input', 'rc30 clipboard')
        page.click('#tokens-register-button')
        page.locator('#tokens-register-table').wait_for()
        rows = page.locator('#tokens-register-table tr').all()
        details = {row.locator('th,td').nth(0).inner_text(): row.locator('th,td').nth(1).inner_text()
                   for row in rows}
        page.get_by_role('button', name='Copy token details').click()
        copied = page.evaluate("""async () => {
            const deadline = Date.now() + 10000;
            while (Date.now() < deadline) {
                const text = await navigator.clipboard.readText();
                if (text.includes('Label: rc30 clipboard') && text.includes('API Key:')) return text;
                await new Promise(resolve => setTimeout(resolve, 50));
            }
            throw new Error('Token details were not copied');
        }""")
        assert 'Label: rc30 clipboard' in copied
        assert details['API Key']
        for label, value in details.items():
            assert f'{label}: {value}' in copied
        assert not tab.errors, tab.errors
    finally:
        context.close()
