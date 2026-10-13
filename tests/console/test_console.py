#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Playwright tests of the v5 console against a real `mrsm start api` (design §14 "Console tests").
Opt-in (`MRSM_TEST_CONSOLE=1`); see `tests/console/conftest.py`. All data is synthetic.
"""

import json
import tarfile
import pathlib

import pytest

from tests.console.conftest import USERNAME, PASSWORD, Tab, http

pytestmark = pytest.mark.console

#: The figure both `w7_pyfig` (Python) and `w7_native_fig` (Rust) emit.
FIGURE = {
    'data': [{'type': 'bar', 'x': ['a', 'b', 'c'], 'y': [3, 1, 2], 'name': 'w7'}],
    'layout': {'title': {'text': 'W7 figure'}},
}

CORE_PAGES = (
    '/dash', '/dash/dashboard', '/dash/pipes', '/dash/jobs', '/dash/plugins',
    '/dash/users', '/dash/tokens', '/dash/settings', '/dash/pipelines', '/dash/webterm',
)


def _ignore(error: str) -> bool:
    ### Chromium logs a failed resource as a console error without the URL; the matching
    ### `response` entry carries it, so only the latter is asserted.
    return error.startswith('console.error: Failed to load resource')


def _errors(tab: Tab) -> list:
    return [e for e in tab.errors if not _ignore(e)]


def test_login_round_trip(server, context):
    page = context.new_page()
    page.goto(f"{server.base_url}/dash")
    assert page.url == f"{server.base_url}/dash/login"

    ### A wrong password does not start a session.
    page.fill('input[name=username]', USERNAME)
    page.fill('input[name=password]', 'not-the-password')
    page.click('button:has-text("Log in")')
    assert not [c for c in context.cookies() if c['name'] == 'mrsm-session-id']
    page.goto(f"{server.base_url}/dash")
    assert page.url == f"{server.base_url}/dash/login"

    tab = Tab(page, server.base_url)
    tab.login()
    cookies = [c for c in context.cookies() if c['name'] == 'mrsm-session-id']
    assert len(cookies) == 1
    assert cookies[0]['httpOnly'] is True and cookies[0]['sameSite'] == 'Lax'
    assert cookies[0]['path'] == '/'
    assert page.locator('nav[aria-label=Console]').is_visible()
    assert _errors(tab) == []

    page.click('button:has-text("Log out")')
    page.wait_for_url(f"{server.base_url}/dash/login")
    assert not [c for c in context.cookies() if c['name'] == 'mrsm-session-id']
    page.goto(f"{server.base_url}/dash/pipes")
    assert page.url == f"{server.base_url}/dash/login"


def test_every_console_page_renders_without_errors(server, tab):
    tab.login()
    for path in CORE_PAGES:
        tab.page.goto(f"{server.base_url}{path}")
        tab.page.wait_for_load_state('networkidle')
        assert tab.page.locator('main#content').is_visible(), path
        assert tab.page.locator('nav[aria-label=Console] a').count() >= 8, path
    assert _errors(tab) == [], _errors(tab)

    ### Every link of the navbar, plugin pages and plugin-owned Dash apps included.
    links = tab.page.eval_on_selector_all(
        'nav[aria-label=Console] a', 'els => els.map(e => e.getAttribute("href"))',
    )
    assert '/dash/w7-figure' in links
    assert '/dash/w7_dashapp/w7-dash' in links
    for href in links:
        tab.page.goto(f"{server.base_url}{href}")
        tab.page.wait_for_load_state('networkidle')
        assert tab.page.url.startswith(f"{server.base_url}/dash"), href
    assert _errors(tab) == [], _errors(tab)


def _rendered_figure(page, selector: str = '#w7-figure'):
    page.wait_for_selector(f"{selector}[data-rendered=true]")
    return page.eval_on_selector(selector, 'el => ({data: el.data, layout: el.layout})')


def _strip(figure: dict) -> dict:
    ### Plotly adds defaults (uid, xaxis ranges, ...) to what `newPlot` keeps; compare the
    ### parts the plugin sent.
    data = [
        {k: v for k, v in trace.items() if k in ('type', 'x', 'y', 'name')}
        for trace in figure['data']
    ]
    return {'data': data, 'layout': {'title': {'text': figure['layout']['title']['text']}}}


def test_python_and_rust_pages_render_the_same_figure(server, tab):
    tab.login()
    tab.page.goto(f"{server.base_url}/dash/w7-figure")
    python_figure = _rendered_figure(tab.page)
    assert _strip(python_figure) == FIGURE
    assert tab.page.locator('#w7-figure .trace.bars .point').count() == 3
    assert tab.page.locator('h2:has-text("W7 Python page")').is_visible()

    ### HTMX: a page element requests a plugin API route and swaps the fragment.
    tab.page.click('#w7-load')
    tab.page.wait_for_selector('#w7-fragment')
    assert 'Fragment from a Python plugin route' in tab.page.inner_text('#w7-slot')

    if not server.native_built:
        pytest.skip(f"The native plugin was not built: {(server.root / 'native_build.log').read_text()[-800:]}")
    tab.page.goto(f"{server.base_url}/dash/w7-native-figure")
    rust_figure = _rendered_figure(tab.page)
    assert tab.page.locator('h2:has-text("W7 Rust page")').is_visible()
    assert tab.page.locator('#w7-figure .trace.bars .point').count() == 3
    assert rust_figure == python_figure
    assert _strip(rust_figure) == FIGURE
    assert _errors(tab) == [], _errors(tab)


def test_plugin_owned_dash_app_fires_a_callback(server, tab):
    tab.login()
    tab.page.goto(f"{server.base_url}/dash/w7_dashapp/w7-dash")
    tab.page.wait_for_selector('#w7-dash-title')
    ### The core stores are filled from the console's session cookie.
    tab.page.wait_for_function(
        "document.querySelector('#w7-session') && "
        "document.querySelector('#w7-session').textContent === 'session: active'"
    )
    tab.page.click('#w7-btn')
    tab.page.wait_for_function("document.querySelector('#w7-out').textContent === 'clicked 1'")
    tab.page.click('#w7-btn')
    tab.page.wait_for_function("document.querySelector('#w7-out').textContent === 'clicked 2'")
    ### The Dash app's navbar links back to the console.
    assert tab.page.locator('nav.mrsm-plugin-navbar a[href="/dash/pipes"]').count() == 1
    assert _errors(tab) == [], _errors(tab)


def test_old_v4_urls_redirect_to_the_plugin_app(server, tab):
    tab.login()
    response = tab.page.request.get(f"{server.base_url}/dash/w7-dash?x=1", max_redirects=0)
    assert response.status in (302, 307), response.status
    assert response.headers['location'] == '/dash/w7_dashapp/w7-dash?x=1'
    tab.page.goto(f"{server.base_url}/dash/w7-dash?x=1")
    tab.page.wait_for_selector('#w7-dash-title')
    assert tab.page.url == f"{server.base_url}/dash/w7_dashapp/w7-dash?x=1"
    ### A URL already under the plugin's name is not redirected.
    response = tab.page.request.get(f"{server.base_url}/dash/w7_dashapp/w7-dash", max_redirects=0)
    assert response.status == 200


def test_session_is_shared_by_console_dash_app_and_api_plugin(server, context):
    anonymous = context.new_page()
    response = anonymous.request.get(f"{server.base_url}/w7/session")
    assert response.json() == {'active': False, 'username': None}
    anonymous.goto(f"{server.base_url}/dash/w7_dashapp/w7-dash")
    anonymous.wait_for_selector('text=Login required')

    tab = Tab(context.new_page(), server.base_url)
    tab.login()
    ### The API plugin route (served by the Python host) reads the console's session.
    response = tab.page.request.get(f"{server.base_url}/w7/session")
    assert response.json() == {'active': True, 'username': USERNAME}
    tab.page.goto(f"{server.base_url}/dash/w7_dashapp/w7-dash")
    tab.page.wait_for_function(
        "document.querySelector('#w7-session') && "
        "document.querySelector('#w7-session').textContent === 'session: active'"
    )
    ### Logging out in the console ends it everywhere.
    tab.page.goto(f"{server.base_url}/dash")
    tab.page.click('button:has-text("Log out")')
    tab.page.wait_for_url(f"{server.base_url}/dash/login")
    assert tab.page.request.get(f"{server.base_url}/w7/session").json()['active'] is False
    tab.page.goto(f"{server.base_url}/dash/w7_dashapp/w7-dash")
    tab.page.wait_for_selector('text=Login required')


def _plugin_archive(tmp_path: pathlib.Path, name: str) -> pathlib.Path:
    source = tmp_path / name
    source.mkdir()
    (source / '__init__.py').write_text("__version__ = '0.1.0'\nrequired = []\n")
    archive = tmp_path / f"{name}.tar.gz"
    with tarfile.open(archive, 'w:gz') as tar:
        tar.add(source, arcname=name)
    return archive


def test_plugin_archive_upload_through_the_console(server, tab, tmp_path):
    archive = _plugin_archive(tmp_path, 'w7_uploaded')
    tab.login()
    tab.page.goto(f"{server.base_url}/dash/plugins")
    tab.page.fill('form[action="/dash/plugins/upload"] input[name=name]', 'w7_uploaded')
    tab.page.fill('form[action="/dash/plugins/upload"] input[name=version]', '0.1.0')
    tab.page.fill('form[action="/dash/plugins/upload"] input[name=description]', 'Uploaded in a test')
    tab.page.set_input_files('form[action="/dash/plugins/upload"] input[type=file]', str(archive))
    tab.page.click('button:has-text("Upload plugin")')
    tab.page.wait_for_selector('[role=status]')
    assert 'Success' in tab.page.inner_text('[role=status]'), tab.page.inner_text('[role=status]')
    stored = tab.page.request.get(f"{server.base_url}/plugins/w7_uploaded")
    assert stored.status == 200 and stored.body() == archive.read_bytes()
    assert tab.page.request.get(f"{server.base_url}/plugins/w7_uploaded/version").json() == '0.1.0'
    ### The new plugin is listed on the plugins page.
    tab.page.goto(f"{server.base_url}/dash/plugins")
    assert tab.page.locator('article h2:has-text("w7_uploaded")').count() == 1


def _login_request(page, base_url: str) -> None:
    response = page.request.post(
        f"{base_url}/dash/login",
        form={'username': USERNAME, 'password': PASSWORD},
        max_redirects=0,
    )
    assert response.status == 303, response.status


def test_dash_route_group_hides_console_pages_only(gated_server, context):
    base = gated_server.base_url
    page = context.new_page()
    tab = Tab(page, base)
    ### The index redirects to the login page when the console's group is not allowed.
    response = page.request.get(f"{base}/", max_redirects=0)
    assert response.headers['location'] == '/dash/login'
    assert page.request.get(f"{base}/dash/login").status == 200
    assert page.request.get(f"{base}/dash/assets/console.js").status == 200
    _login_request(page, base)
    for hidden in ('/dash', '/dash/pipes', '/dash/jobs', '/dash/plugins', '/dash/users',
                   '/dash/tokens', '/dash/settings', '/dash/pipelines', '/dash/register'):
        assert page.request.get(f"{base}{hidden}", max_redirects=0).status == 404, hidden
    ### Plugin pages are always served and show no core links.
    page.goto(f"{base}/dash/w7-figure")
    _rendered_figure(page)
    assert page.locator('nav[aria-label=Console] a[href="/dash/jobs"]').count() == 0
    assert page.locator('nav[aria-label=Console] a[href="/dash/w7-figure"]').count() == 1
    page.goto(f"{base}/dash/w7-dash")
    page.wait_for_selector('#w7-dash-title')
    assert page.url == f"{base}/dash/w7_dashapp/w7-dash"
    ### Other route groups keep working.
    assert page.request.get(f"{base}/version").status == 200
    assert page.request.get(f"{base}/pipes").status in (200, 401)
    assert _errors(tab) == [], _errors(tab)


def test_no_dash_serves_no_console_and_no_plugin_pages(nodash_server, context):
    base = nodash_server.base_url
    page = context.new_page()
    for path in ('/dash', '/dash/login', '/dash/pipes', '/dash/w7-figure', '/dash/w7-dash',
                 '/dash/w7_dashapp/', '/dash/w7_dashapp/_dash-layout', '/_pages/w7-figure'):
        assert page.request.get(f"{base}{path}", max_redirects=0).status in (404, 401, 403), path
    ### The API plugin's routes still work through the host.
    assert page.request.get(f"{base}/w7/session").json() == {'active': False, 'username': None}
    assert page.request.get(f"{base}/version").status == 200
    assert page.request.get(f"{base}/", max_redirects=0).headers['location'] in ('/docs', '/openapi.json')


SETUP_PIPES = """
import meerschaum as mrsm
conn = mrsm.get_connector('sql:w7console')
conn.exec("CREATE TABLE IF NOT EXISTS w7_raw (dt TIMESTAMP, id INTEGER, val REAL)")
conn.exec("INSERT INTO w7_raw VALUES ('2026-01-01 00:00:00', 1, 1.5), ('2026-01-02 00:00:00', 2, 2.5)")
parent = mrsm.Pipe('sql:w7console', 'w7parent', instance='sql:w7console', parameters={
    'columns': {'datetime': 'dt', 'id': 'id'},
    'fetch': {'definition': 'SELECT * FROM w7_raw'},
})
assert parent.register()[0]
child = mrsm.Pipe('sql:w7console', 'w7child', instance='sql:w7console', parameters={
    'columns': {'datetime': 'dt', 'id': 'id'},
    'sql': "SELECT * FROM {{ Pipe('sql:w7console', 'w7parent') }}",
})
assert child.register()[0]
"""


def test_pipelines_page_graph_gantt_and_live_updates(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    sync = server.mrsm('sync', 'pipes', '-i', 'sql:w7console', '--nopretty')
    assert sync.returncode == 0, sync.stdout[-1500:] + sync.stderr[-1500:]

    tab.login()
    tab.page.goto(f"{server.base_url}/dash/pipelines")
    ### The dependency graph (cytoscape draws onto canvases) with its edge table.
    tab.page.wait_for_selector('#pipeline-graph [data-mrsm-graph][data-rendered=true] canvas', state='attached')
    assert tab.page.locator('#pipeline-graph table tbody tr').count() == 1
    row = tab.page.inner_text('#pipeline-graph table tbody tr')
    assert 'w7parent' in row and 'w7child' in row and 'template' in row
    ### The Gantt chart of the current run: an actual bar per pipe (planned bars need the
    ### planner's end times, which the run rows do not carry yet).
    tab.page.wait_for_selector('#pipeline-gantt [data-mrsm-figure][data-rendered=true]')
    assert tab.page.locator('#pipeline-gantt .trace.bars .point').count() >= 2
    assert tab.page.locator('#pipeline-gantt table tbody tr').count() == 2
    assert 'succeeded' in tab.page.inner_text('#pipeline-gantt table tbody')
    ### The runs table, and the run's events over SSE.
    assert tab.page.locator('#pipeline-runs table tbody tr').count() == 1
    assert 'succeeded' in tab.page.inner_text('#pipeline-runs table tbody tr')
    tab.page.wait_for_function(
        "document.querySelector('pre[data-mrsm-events]').textContent.trim().length > 0",
        timeout=20000,
    )
    events = tab.page.inner_text('pre[data-mrsm-events]')
    assert 'w7parent' in events and 'w7child' in events

    ### A sync while the page is open appears without a reload (the runs table polls).
    again = server.mrsm('sync', 'pipes', '-i', 'sql:w7console', '--nopretty')
    assert again.returncode == 0, again.stdout[-1500:] + again.stderr[-1500:]
    tab.page.wait_for_function(
        "document.querySelectorAll('#pipeline-runs table tbody tr').length === 2",
        timeout=45000,
    )
    ### Filtering the runs by status.
    tab.page.goto(f"{server.base_url}/dash/pipelines?status=failed")
    assert 'No runs match' in tab.page.inner_text('#pipeline-runs')
    assert _errors(tab) == [], _errors(tab)


def test_webterm_is_served_through_the_host(webterm_server, context):
    base = webterm_server.base_url
    page = context.new_page()
    ### The terminal route authenticates the console's session through the shared store.
    assert 'Unauthorized' in page.request.get(f"{base}/webterm/not-a-session").text()
    assert page.request.get(f"{base}/webterm/not-a-session").status == 401
    tab = Tab(page, base)
    tab.login()
    cookie = [c for c in context.cookies() if c['name'] == 'mrsm-session-id'][0]['value']
    page.goto(f"{base}/dash/webterm")
    link = page.locator('main a:has-text("Open web terminal")')
    assert link.get_attribute('href') == f"/webterm/{cookie}"
    ### The terminal process starts with the host; give it a moment to listen.
    for _ in range(40):
        response = page.request.get(f"{base}/webterm/{cookie}")
        if response.status == 200:
            break
        page.wait_for_timeout(500)
    assert response.status == 200, response.status
    assert 'Meerschaum Shell' in response.text()
    assert webterm_server.port_is_open(webterm_server.webterm_port)
