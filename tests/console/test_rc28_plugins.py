#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
RC28 Plugins page in a real browser (opt-in, see `conftest.py`): New plugin (the circled plus)
asks `bootstrap plugins`'s questions and lands on the new plugin's source in the editor; an
installed plugin's files are edited and saved in the source view, and a file changed on disk
since it was opened is not overwritten; the Language column and the source tree show the
languages' Font Awesome logos. Creating and editing are the web terminal's privilege: only
administrators on a server that serves the terminal see them, and the callbacks refuse anyone
else's requests.
"""

from __future__ import annotations

import json
import os
import pathlib
import shutil

import pytest

from tests.console.conftest import Server, Tab

pytestmark = pytest.mark.console

PASSWORD = 'correct-horse-rc28'
SCREENSHOTS = pathlib.Path(
    os.environ.get('MRSM_TEST_SCREENSHOTS', '')
    or (pathlib.Path.home() / '.cache' / 'mrsm-console-tests' / 'screenshots')
) / 'rc28-plugins'

SETUP = '''
from meerschaum.core import User
import meerschaum as mrsm
conn = mrsm.get_connector('sql:w7console')
for name, role in (('rc28-operator', 'operator'), ('rc28-viewer', 'viewer')):
    account = User(name, %(password)r, instance=conn, attributes={'role': role})
    if conn.get_user_id(account) is None:
        success, msg = conn.register_user(account)
        assert success, msg
''' % {'password': PASSWORD}

#: What New plugin and editing refuse a non-administrator.
REFUSAL = 'Only administrators can create and edit plugins.'


def screenshot(page, name: str) -> None:
    SCREENSHOTS.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(SCREENSHOTS / f'{name}.png'), full_page=False)


@pytest.fixture(scope='module')
def plugins_server():
    """A server that serves the web terminal (so administrators may author plugins)."""
    server = Server(
        'rc28-plugins',
        config={'api': {'webterm': {'tmux': {'enabled': False}}}},
        native=False,
        webterm=True,
    )
    (server.plugins / 'rc28_conflict.py').write_text('VALUE = "start"\n')
    try:
        server.start()
        result = server.python(SETUP)
        assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
        yield server
    finally:
        server.stop()


def _new_tab(browser, server, width):
    context = browser.new_context(viewport={'width': width, 'height': 900})
    return context, Tab(context.new_page(), server.base_url)


def _open_new_plugin(tab, name):
    page = tab.page
    page.get_by_role('button', name='New plugin', exact=True).click()
    page.locator('#plugins-new-modal').wait_for()
    page.fill('#plugins-new-name', name)


def _no_horizontal_scroll(page) -> bool:
    return page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')


def _logo_rendered(page, selector: str) -> bool:
    """The Font Awesome glyph is drawn (its stylesheet and font loaded)."""
    page.evaluate('document.fonts.ready.then(() => true)')
    return page.evaluate(
        """(selector) => {
            const icon = document.querySelector(selector);
            if (!icon) return false;
            const before = getComputedStyle(icon, '::before').content;
            return !!before && before !== 'none' && before !== 'normal'
                && icon.getBoundingClientRect().width > 0;
        }""",
        selector,
    )


@pytest.mark.parametrize('width', [1440, 390])
def test_new_python_plugin_opens_in_the_editor_and_saves(plugins_server, browser, width):
    name = f'rc28_py_{width}'
    context, tab = _new_tab(browser, plugins_server, width)
    page = tab.page
    try:
        tab.login(path='/dash')
        tab.goto('/dash/plugins', '#installed-plugins-table')
        headers = page.locator('#installed-plugins-table thead th').all_text_contents()
        assert 'Language' in headers and 'Kind' not in headers
        row = page.locator('#installed-plugins-table tr[id*="w7_pyfig"]')
        assert row.get_by_role('img', name='Python').count() >= 1
        if width >= 768:
            assert _logo_rendered(page, '#installed-plugins-table td.plugin-kind .fa-python')
        else:
            assert _logo_rendered(page, '#installed-plugins-table .plugin-mobile-meta .fa-python')
        new_button = page.get_by_role('button', name='New plugin', exact=True)
        assert _logo_rendered(page, '#plugins-new-button .fa-circle-plus')
        box = new_button.bounding_box()
        assert box['width'] >= (44 if width < 768 else 40) - 0.5, box
        assert _no_horizontal_scroll(page)
        screenshot(page, f'plugins-installed-{width}')

        _open_new_plugin(tab, name)
        page.get_by_text(f'Single file ({name}.py)', exact=True).wait_for()
        assert page.get_by_text(f'Package ({name}/__init__.py)', exact=True).is_visible()
        ### `bootstrap plugins`'s features, Fetch data picked by default.
        labels = page.locator('#plugins-new-features label').all_text_contents()
        assert labels == ['Fetch data', 'Connector', 'Instance connector', 'New actions',
                          'New API endpoints', 'New web console page']
        assert page.locator('#plugins-new-features input[value="fetch"]').is_checked()
        assert not page.locator('#plugins-new-action-field').is_visible()
        page.locator('#plugins-new-features label', has_text='New actions').click()
        page.locator('#plugins-new-action').wait_for()
        page.fill('#plugins-new-action', 'sing song')
        page.fill('#plugins-new-dt', 'timestamp')
        screenshot(page, f'plugins-new-python-{width}')
        page.click('#plugins-new-create')
        page.wait_for_url(f'**/dash/plugins/installed/{name}?*', timeout=30000)
        editor = page.locator('#plugin-source-editor')
        editor.wait_for(timeout=30000)
        assert 'def sing_song(' in editor.input_value()
        assert '"timestamp"' in editor.input_value()
        assert f"Created plugin '{name}'." in page.inner_text('#plugin-source-alert')
        assert page.get_by_role('button', name='Save').is_visible()
        assert _no_horizontal_scroll(page)
        ### The editor keeps the code view's colors (not the theme's textarea grey).
        assert editor.evaluate('e => getComputedStyle(e).backgroundColor') == 'rgb(23, 33, 43)'
        ### The gutter is exactly as tall as the editor (its numbers scroll with the text).
        heights = page.evaluate("""() => [
            document.getElementById('plugin-source-editor').getBoundingClientRect().height,
            document.querySelector('.plugin-source-gutter').getBoundingClientRect().height,
        ]""")
        assert abs(heights[0] - heights[1]) <= 1, heights
        page.locator('.plugin-source-workspace').scroll_into_view_if_needed()
        screenshot(page, f'plugins-editor-python-{width}')

        ### Typing: Tab indents (four spaces); Save writes the file and closes the editor.
        editor.click()
        page.keyboard.press('Control+End')
        page.keyboard.type('\n\ndef edited_in_the_console():\n')
        page.keyboard.press('Tab')
        page.keyboard.type('return 28')
        assert editor.input_value().endswith('def edited_in_the_console():\n    return 28')
        gutter = page.locator('.plugin-source-gutter').inner_text().split()
        assert gutter[-1] == str(editor.input_value().count('\n') + 1)
        page.get_by_role('button', name='Save').click()
        page.locator('#plugin-source-save-alert').wait_for(timeout=30000)
        assert f'Saved {name}.py.' in page.inner_text('#plugin-source-save-alert')
        page.locator('#plugin-source-code .source-line').first.wait_for()
        assert 'edited_in_the_console' in page.inner_text('#plugin-source-code')
        assert page.locator('#plugin-source-editor').count() == 0
        on_disk = (plugins_server.plugins / f'{name}.py').read_text()
        assert on_disk.endswith('def edited_in_the_console():\n    return 28')
        assert not [p for p in plugins_server.plugins.iterdir() if p.name.endswith('.mrsm-edit')]
        assert page.get_by_role('button', name='Edit').is_visible()
        page.locator('.plugin-source-workspace').scroll_into_view_if_needed()
        screenshot(page, f'plugins-saved-python-{width}')
        assert not tab.errors, tab.errors
    finally:
        context.close()


def test_new_rust_package_lands_on_its_crate_source(plugins_server, browser):
    name = 'rc28-rs'
    context, tab = _new_tab(browser, plugins_server, 1440)
    page = tab.page
    try:
        tab.login(path='/dash')
        tab.goto('/dash/plugins', '#installed-plugins-table')
        _open_new_plugin(tab, name)
        page.locator('#plugins-new-language label', has_text='Rust').click()
        package = page.get_by_text(f'Package ({name}/Cargo.toml)', exact=True)
        package.wait_for()
        package.click()
        page.locator('#plugins-new-features label', has_text='New web console page').click()
        screenshot(page, 'plugins-new-rust-1440')
        page.click('#plugins-new-create')
        page.wait_for_url(f'**/dash/plugins/installed/{name}?*', timeout=30000)
        editor = page.locator('#plugin-source-editor')
        editor.wait_for(timeout=30000)
        assert f'meerschaum_plugin::export_plugin!(Rc28Rs);' in editor.input_value()
        assert page.inner_text('.plugin-source-toolbar code') == 'src/lib.rs'
        for path in ('plugin.toml', 'Cargo.toml', 'src/lib.rs'):
            assert (plugins_server.plugins / name / path).is_file(), path
        ### The tree and the toolbar show the language's logo.
        assert page.locator('.plugin-source-toolbar [role="img"][aria-label="Rust"]').count() == 1
        assert page.locator('.plugin-source-tree [role="img"][aria-label="Rust"]').count() >= 1
        assert _logo_rendered(page, '.plugin-source-tree .fa-rust')
        ### While editing, the tree keeps the file open.
        page.get_by_role('button', name='Cargo.toml', exact=True).click()
        page.wait_for_timeout(500)
        assert page.inner_text('.plugin-source-toolbar code') == 'src/lib.rs'
        screenshot(page, 'plugins-editor-rust-1440')
        ### Fullscreen gives the editor the rest of the screen.
        page.get_by_role('button', name='Fullscreen', exact=True).click()
        box = editor.bounding_box()
        assert box['height'] > 0.6 * page.viewport_size['height'], box
        screenshot(page, 'plugins-editor-rust-fullscreen-1440')
        page.get_by_role('button', name='Exit fullscreen', exact=True).click()
        page.get_by_role('button', name='Cancel').click()
        page.locator('#plugin-source-code .source-token-keyword').first.wait_for(timeout=30000)
        assert page.locator('#plugin-source-editor').count() == 0
        page.get_by_role('button', name='Cargo.toml', exact=True).click()
        page.wait_for_function(
            "document.querySelector('.plugin-source-toolbar code')?.textContent === 'Cargo.toml'"
        )
        ### The plugin's page names its language with the logo.
        details = page.inner_text('.plugin-detail-properties')
        assert 'Language' in details and 'Rust' in details
        screenshot(page, 'plugins-rust-detail-1440')
        assert not tab.errors, tab.errors
    finally:
        context.close()
        ### A Rust plugin is built whenever native plugins are next loaded: none is left behind.
        shutil.rmtree(plugins_server.plugins / name, ignore_errors=True)


@pytest.mark.parametrize('width', [1440, 390])
def test_a_file_changed_on_disk_is_not_overwritten(plugins_server, browser, width):
    path = plugins_server.plugins / 'rc28_conflict.py'
    path.write_text('VALUE = "start"\n')
    context, tab = _new_tab(browser, plugins_server, width)
    page = tab.page
    try:
        tab.login(path='/dash')
        tab.goto('/dash/plugins/installed/rc28_conflict', '#plugin-source-content')
        page.get_by_role('button', name='Edit').click()
        editor = page.locator('#plugin-source-editor')
        editor.wait_for(timeout=30000)
        assert editor.input_value() == 'VALUE = "start"\n'
        path.write_text('VALUE = "disk"\n')
        editor.fill('VALUE = "browser"\n')
        page.get_by_role('button', name='Save').click()
        page.locator('#plugin-source-save-alert').wait_for(timeout=30000)
        message = page.inner_text('#plugin-source-save-alert')
        assert 'rc28_conflict.py changed on disk after it was opened, so it was not saved.' in message
        assert path.read_text() == 'VALUE = "disk"\n'
        ### The editor keeps what was typed.
        assert editor.input_value() == 'VALUE = "browser"\n'
        assert _no_horizontal_scroll(page)
        page.locator('.plugin-source-workspace').scroll_into_view_if_needed()
        screenshot(page, f'plugins-conflict-{width}')
        page.get_by_role('button', name='Cancel').click()
        page.wait_for_function(
            "document.getElementById('plugin-source-code')?.textContent.includes('disk')"
        )
        assert not tab.errors, tab.errors
    finally:
        context.close()


def _session_id(context) -> str:
    return next(c['value'] for c in context.cookies() if c['name'] == 'mrsm-session-id')


def _callback_key(page, base_url: str, fragment: str) -> str:
    response = page.request.get(f'{base_url}/dash/_dash-dependencies')
    assert response.ok, response.status
    return next(item['output'] for item in response.json() if fragment in item['output'])


def _outputs(key: str):
    parts = key.strip('.').split('...')
    outputs = []
    for part in parts:
        ident, prop = part.rsplit('.', 1)
        outputs.append({'id': json.loads(ident) if ident.startswith('{') else ident,
                        'property': prop.split('@')[0]})
    return outputs if key.startswith('..') else outputs[0]


def _forge(page, base_url, key, inputs, state):
    changed = inputs[0][0] if isinstance(inputs[0], list) else inputs[0]
    ident = changed['id']
    prop_id = json.dumps(ident, sort_keys=True, separators=(',', ':')) if isinstance(ident, dict) else ident
    response = page.request.post(
        f'{base_url}/dash/_dash-update-component',
        data=json.dumps({
            'output': key, 'outputs': _outputs(key), 'inputs': inputs, 'state': state,
            'changedPropIds': [prop_id + '.' + changed['property']],
        }),
        headers={'content-type': 'application/json'},
    )
    return response.status, response.text()


@pytest.mark.parametrize('username', ['rc28-operator', 'rc28-viewer'])
@pytest.mark.parametrize('width', [1440, 390])
def test_non_administrators_cannot_create_or_edit_plugins(plugins_server, browser, username, width):
    """
    An operator (who may use the terminal) and a viewer see no New plugin and no Edit, and their
    forged callback requests are refused on the server: nothing is written.
    """
    path = plugins_server.plugins / 'rc28_conflict.py'
    before = path.read_text()
    context, tab = _new_tab(browser, plugins_server, width)
    page = tab.page
    try:
        tab.login(username, PASSWORD, path='/dash')
        tab.goto('/dash/plugins', '#installed-plugins-table')
        page.wait_for_timeout(1000)
        assert page.locator('#plugins-new-button').count() == 0
        assert page.locator('.plugin-new-button').count() == 0
        screenshot(page, f'plugins-{username}-{width}')
        tab.goto('/dash/plugins/installed/rc28_conflict', '#plugins-content-div')
        page.get_by_text('Installed source is available to administrators.').wait_for()
        assert page.locator('.plugin-source-edit').count() == 0

        session = {'session-id': _session_id(context)}
        key = _callback_key(page, plugins_server.base_url, 'plugin-source-context.data')
        for action in ('edit', 'save'):
            status, body = _forge(page, plugins_server.base_url, key, [[{
                'id': {'type': 'plugin-source-action', 'action': action},
                'property': 'n_clicks', 'value': 1,
            }]], [
                {'id': 'plugin-source-file', 'property': 'value', 'value': 'rc28_conflict.py'},
                {'id': 'plugin-source-context', 'property': 'data',
                 'value': {'kind': 'installed', 'name': 'rc28_conflict'}},
                {'id': 'plugin-source-data', 'property': 'data', 'value': {
                    'path': 'rc28_conflict.py', 'editing': True,
                    'digest': __import__('hashlib').sha256(before.encode()).hexdigest(),
                }},
                {'id': 'plugin-source-editor', 'property': 'value', 'value': 'FORGED = True\n'},
                {'id': 'session-store', 'property': 'data', 'value': session},
            ])
            assert status == 200 and REFUSAL in body, (action, status, body[:500])
        key = _callback_key(page, plugins_server.base_url, 'plugins-navigate.data')
        status, body = _forge(page, plugins_server.base_url, key, [
            {'id': 'plugins-new-create', 'property': 'n_clicks', 'value': 1},
        ], [
            {'id': 'plugins-new-name', 'property': 'value', 'value': 'rc28_forged'},
            {'id': 'plugins-new-language', 'property': 'value', 'value': 'Python'},
            {'id': 'plugins-new-packaging', 'property': 'value', 'value': 'file'},
            {'id': 'plugins-new-features', 'property': 'value', 'value': ['fetch']},
            {'id': 'plugins-new-action', 'property': 'value', 'value': ''},
            {'id': 'plugins-new-dt', 'property': 'value', 'value': ''},
            {'id': 'plugins-new-directory', 'property': 'value', 'value': None},
            {'id': 'session-store', 'property': 'data', 'value': session},
        ])
        assert status == 200 and REFUSAL in body, (status, body[:500])
        assert path.read_text() == before
        assert not (plugins_server.plugins / 'rc28_forged.py').exists()
        assert not tab.errors, tab.errors
    finally:
        context.close()
