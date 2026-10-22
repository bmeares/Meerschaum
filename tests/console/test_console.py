#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Playwright tests of the web console against a real `mrsm start api` (design §14 "Console tests",
D21): the Plotly Dash console of v4 on the Python API host behind the Rust server. Opt-in
(`MRSM_TEST_CONSOLE=1`); see `tests/console/conftest.py`. All data is synthetic.
"""

import json
import time

import pytest

from tests.console.conftest import USERNAME, PASSWORD, Tab, http

pytestmark = pytest.mark.console

#: The figure both `w7_pyfig` (Python) and `w7_native_fig` (Rust) emit.
FIGURE = {
    'data': [{'type': 'bar', 'x': ['a', 'b', 'c'], 'y': [3, 1, 2], 'name': 'w7'}],
    'layout': {'title': {'text': 'W7 figure'}},
}

CORE_PAGES = (
    '/dash', '/dash/pipes', '/dash/jobs', '/dash/plugins', '/dash/users', '/dash/tokens',
    '/dash/pipelines', '/dash/password-reset',
)


def _errors(tab: Tab) -> list:
    return list(tab.errors)


def _frame(page, selector: str = 'iframe.mrsm-contract-frame'):
    """The frame of a page-contract page (an iframe of the host's `/_pages/<path>?frame=1`)."""
    page.wait_for_selector(selector, timeout=30000)
    frame = page.query_selector(selector).content_frame()
    assert frame is not None
    return frame


def _wait_text(page, selector: str, text: str, timeout: int = 30000) -> None:
    page.wait_for_function(
        "([selector, text]) => { const el = document.querySelector(selector); "
        "return el && el.textContent.includes(text); }",
        arg=[selector, text],
        timeout=timeout,
    )


def test_login_round_trip(server, context):
    page = context.new_page()
    tab = Tab(page, server.base_url)
    ### The console's pages need a login: `/dash` is the login page without a session.
    tab.goto('/dash', '#username-input')
    assert page.locator('#login-button').is_visible()

    ### A wrong password does not start a session.
    page.fill('#username-input', USERNAME)
    page.fill('#password-input', 'not-the-password')
    page.click('#login-button')
    _wait_text(page, '#login-alert-div', 'Invalid login')
    assert not [c for c in context.cookies() if c['name'] == 'mrsm-session-id']

    tab.login()
    cookies = [c for c in context.cookies() if c['name'] == 'mrsm-session-id']
    assert len(cookies) == 1
    assert cookies[0]['httpOnly'] is True and cookies[0]['sameSite'] == 'Lax'
    assert cookies[0]['path'] == '/'
    assert _errors(tab) == []

    page.click('#sign-out-button')
    page.wait_for_selector('#username-input')
    assert not [c for c in context.cookies() if c['name'] == 'mrsm-session-id']
    tab.goto('/dash/pipes', '#username-input')


def test_every_console_page_renders_without_errors(server, tab):
    tab.login()
    for path in CORE_PAGES:
        tab.goto(path)
        tab.page.wait_for_load_state('networkidle')
        assert tab.page.locator('#logo-img').count() == 1, path
        assert tab.page.url.startswith(f"{server.base_url}/dash"), path
    assert _errors(tab) == [], _errors(tab)

    ### The logo opens the page menu: the console's pages and the plugins' (grouped by their group).
    tab.goto('/dash/pipes')
    tab.page.click('#logo-img')
    tab.page.wait_for_selector('#pages-offcanvas.show')
    menu = tab.page.inner_text('#pages-offcanvas')
    for label in ('Web Console', 'Pipes', 'Jobs', 'Plugins', 'Users', 'Tokens', 'Pipelines'):
        assert label in menu, label
    assert 'W7 Python' in menu or 'W7 Figure' in menu
    assert 'W7 Dash' in menu
    assert _errors(tab) == [], _errors(tab)


def test_dashboard_dropdowns_list_the_registered_pipes(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    tab.page.click('#connector-keys-dropdown')
    tab.page.wait_for_selector('.Select-option, .dash-dropdown-option, [role=option]')
    options = tab.page.locator('[role=option]').all_inner_texts()
    assert any('sql:w7console' in option for option in options), options
    assert _errors(tab) == [], _errors(tab)


def test_dashboard_pipes_button_lists_cards_and_sections(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    tab.page.click('#get-pipes-button')
    tab.page.wait_for_selector('.card-title:has-text("w7parent")')
    titles = tab.page.locator('.card-title').all_inner_texts()
    assert any('w7parent' in t for t in titles) and any('w7child' in t for t in titles)
    assert _errors(tab) == [], _errors(tab)

    ### The sections of a pipe's card, each loaded when it opens.
    tab.goto('/dash/pipes/sql:w7console/w7parent')
    for title, expect in (
        ('Keys', 'w7parent'),
        ('Statistics', 'Row count'),
        ('Columns', 'DB Type'),
        ('Parameters', 'columns'),
        ('SQL Query', 'SELECT'),
        ('Recent Data', 'val'),
        ('Query Data', 'Row Limit'),
        ('Sync Documents', 'Sync'),
    ):
        tab.page.click(f'.accordion-button:has-text("{title}")')
        _wait_text(tab.page, '.accordion-collapse.show', expect)
    assert _errors(tab) == [], _errors(tab)


def test_pipe_card_runs_queries_and_edits(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    tab.goto('/dash/pipes/sql:w7console/w7parent')

    ### Query: the rows come back as a table.
    tab.page.click('.accordion-button:has-text("Query Data")')
    tab.page.wait_for_selector('.accordion-collapse.show button:text-is("Query")')
    tab.page.click('.accordion-collapse.show button:text-is("Query")')
    _wait_text(tab.page, '.accordion-collapse.show', '2026-01-02')

    ### Sync documents: one new row.
    tab.page.click('.accordion-button:has-text("Sync Documents")')
    tab.page.wait_for_selector('.accordion-collapse.show .ace_editor')
    tab.page.evaluate(
        """(text) => document.querySelector('.accordion-collapse.show .ace_editor').env.editor
            .setValue(text, -1)""",
        json.dumps([{'dt': '2026-02-01 00:00:00', 'id': 3, 'val': 9.5}]),
    )
    tab.page.click('.accordion-collapse.show button:text-is("Sync")')
    _wait_text(tab.page, '.accordion-collapse.show', 'Inserted 1')

    ### Parameters: edit and save.
    tab.page.click('.accordion-button:has-text("Parameters")')
    tab.page.wait_for_selector('.accordion-collapse.show .ace_editor')
    parameters = {
        'columns': {'datetime': 'dt', 'id': 'id'},
        'fetch': {'definition': 'SELECT * FROM w7_raw'},
        'tags': ['dash-edit'],
    }
    tab.page.evaluate(
        """(text) => document.querySelector('.accordion-collapse.show .ace_editor').env.editor
            .setValue(text, -1)""",
        json.dumps(parameters),
    )
    tab.page.click('.accordion-collapse.show button:text-is("Update")')
    _wait_text(tab.page, '.accordion-collapse.show', 'Successfully edited')
    saved = server.python(
        "import meerschaum as mrsm, json\n"
        "pipe = mrsm.Pipe('sql:w7console', 'w7parent', instance='sql:w7console')\n"
        "print('JSON:' + json.dumps({'rows': pipe.get_rowcount(), 'tags': pipe.tags}))\n"
    )
    out = json.loads([l for l in saved.stdout.splitlines() if l.startswith('JSON:')][-1][5:])
    assert out['rows'] == 3 and out['tags'] == ['dash-edit']
    assert _errors(tab) == [], _errors(tab)


def test_pipe_card_shows_parents_and_children(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    tab.goto('/dash/pipes/sql:w7console/w7child')
    tab.page.click('.accordion-button:has-text("Parents & Children")')
    _wait_text(tab.page, '.accordion-collapse.show', 'Parents (1)')
    body = tab.page.inner_text('.accordion-collapse.show')
    assert 'Children (0)' in body and 'w7parent' in body
    ### The parent links to its own card.
    href = tab.page.locator('.accordion-collapse.show a:has-text("w7parent")').get_attribute('href')
    assert href.startswith('/dash/pipes/sql%3Aw7console/w7parent')

    tab.goto('/dash/pipes/sql:w7console/w7parent')
    tab.page.click('.accordion-button:has-text("Parents & Children")')
    _wait_text(tab.page, '.accordion-collapse.show', 'Children (1)')
    tab.page.click('.accordion-collapse.show a.nav-link:has-text("Children")')
    _wait_text(tab.page, '.accordion-collapse.show .tab-pane.active', 'w7child')
    assert _errors(tab) == [], _errors(tab)


def test_exec_runs_once_in_existing_terminal(webterm_server, context):
    from tests.console.test_webterm import _open_terminal, _rows, _wait_for_text
    server = webterm_server
    page = context.new_page()
    from tests.console.conftest import Tab
    tab = Tab(page, server.base_url)
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    frame = _open_terminal(page, server.base_url, '/dash')
    frame.wait_for_function('window.terminal.session.idle === true')
    frame_identity = frame.evaluate('window._rc6Identity = Math.random()')
    requests = []
    page.on('request', lambda request: requests.append(request.url))
    page.click('#go-button')
    page.wait_for_function("() => { const id = window._mrsmExecSubmitted; const frame = document.getElementById('webterm-iframe'); return !!(id && frame?.contentWindow?.terminal?.session.requests.has(id)); }")
    acknowledgment = page.evaluate("document.getElementById('webterm-iframe').contentWindow.terminal.session.requests.get(window._mrsmExecSubmitted)")
    assert acknowledgment['accepted'], acknowledgment
    _wait_for_text(frame, 'w7parent')
    frame.wait_for_function('window.terminal.session.idle === true')
    assert frame.evaluate('window._rc6Identity') == frame_identity
    assert page.locator('#webterm-div').is_visible()
    assert not page.locator('#exec-div').is_visible()
    assert not any(url.endswith('/execute') for url in requests)
    # Duplicate delivery is acknowledged without typing another command.
    store = page.evaluate("window._mrsmExecSubmitted")
    before = _rows(frame).count('show pipes')
    page.evaluate("id => document.getElementById('webterm-iframe').contentWindow.postMessage({action:'__EXEC', request_id:id, command:'show pipes'}, location.origin)", store)
    page.wait_for_timeout(200)
    assert _rows(frame).count('show pipes') == before
    frame.evaluate("window.terminal.term.input('unfinished')")
    page.click('#go-button')
    _wait_text(page, '#terminal-exec-status', 'unfinished input')
    assert frame.evaluate('window._rc6Identity') == frame_identity
    frame.evaluate("window.terminal.term.input('\\x15\\r')")
    assert _errors(tab) == [], _errors(tab)
    page.close()


def test_exec_refuses_what_the_dashboard_does_not_offer(server, tab):
    tab.login()
    tab.page.click('#go-button')
    _wait_text(tab.page, '#terminal-exec-status', 'Terminal is unavailable')
    ### The server applies the action's permissions itself: a non-admin user is refused.
    anonymous = tab.page.request.post(
        f"{server.base_url}/execute", data=json.dumps({'sysargs': ['show', 'version']}),
        headers={'content-type': 'application/json'},
    )
    assert anonymous.status in (401, 403)
    assert _errors(tab) == [], _errors(tab)


def test_a_non_admin_runs_what_the_console_offers_and_is_refused_the_rest(server, tab):
    """`POST /execute` limits a non-administrator to the console's actions, with a clear message."""
    result = server.python("""
import meerschaum as mrsm
from meerschaum.core import User
conn = mrsm.get_connector('sql:w7console')
success, msg = conn.register_user(User('bobby', 'correct-horse-2', instance=conn))
assert success, msg
""")
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login('bobby', 'correct-horse-2')
    ### An action the server keeps from non-administrators (it runs code): refused, with the reason.
    tab.page.select_option('#action-dropdown', 'python')
    tab.page.click('#go-button')
    _wait_text(tab.page, '#exec-alert-div', 'server refused this command', timeout=30000)
    ### The dashboard's own omit list comes from the server (`GET /execute/policy`).
    options = tab.page.locator('#action-dropdown option').all_inner_texts()
    assert 'show' in options and 'sh' not in options and 'stack' not in options
    ### The default selection (`show pipes`) is allowed for any user: the server reads
    ### `api:permissions:actions:non_admin` (true by default) for `/execute`, as the console does.
    tab.page.select_option('#action-dropdown', 'show')
    tab.page.click('#go-button')
    _wait_text(tab.page, '#terminal-exec-status', 'Terminal is unavailable', timeout=60000)
    assert _errors(tab) == [], _errors(tab)


def test_manage_menu_syncs_a_pipe_and_confirms_destructive_actions(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    tab.page.click('#get-pipes-button')
    tab.page.wait_for_selector('.card-title:has-text("w7parent")')
    card = tab.page.locator('.card:has(.card-title:has-text("w7parent"))').first
    card.locator('.dropdown-toggle:has-text("Manage")').click()
    tab.page.click('.dropdown-menu.show .dropdown-item:text-is("Sync")')
    tab.page.wait_for_selector('#exec-div', state='visible')
    _wait_text(tab.page, '#exec-view', 'w7parent', timeout=60000)

    ### Clear asks first: cancel leaves the data alone.
    before_rows = server.python(ROWCOUNT).stdout.strip()
    assert before_rows.isdigit() and int(before_rows) >= 2, before_rows
    card.locator('.dropdown-toggle:has-text("Manage")').click()
    tab.page.click('.dropdown-menu.show .dropdown-item:text-is("Clear")')
    tab.page.wait_for_selector('.modal.show')
    assert 'clear' in tab.page.inner_text('#manage-confirm-body')
    assert "Pipe(" in tab.page.inner_text('#manage-confirm-body')
    tab.page.click('#manage-cancel-button')
    tab.page.wait_for_selector('.modal.show', state='detached')
    rows = server.python(ROWCOUNT)
    assert rows.stdout.strip() == before_rows, rows.stdout + rows.stderr[-500:]
    assert _errors(tab) == [], _errors(tab)


def test_jobs_page_starts_and_deletes_a_job(server, tab):
    started = server.mrsm('show', 'version', '-d', '--name', 'w7job', '--yes', '--nopretty')
    assert started.returncode == 0, started.stdout + started.stderr
    tab.login()
    tab.goto('/dash/jobs', '.card-title')
    tab.page.wait_for_selector('.card-title:has-text("w7job")')
    ### Other jobs (the actions the dashboard ran) have cards too: work in this job's.
    card = tab.page.locator('.card:has(.card-title:has-text("w7job"))').first
    card.locator('button:text-is("Start")').wait_for()
    began = lambda: server.python(
        "from meerschaum.jobs import Job\n"
        "job = Job('w7job', executor_keys='local')\n"
        "print(job.began, job.status)"
    ).stdout.strip()
    first_began = began().rsplit(' ', 1)[0]
    ### The click is a request to the host (a click before the page's callbacks are bound is lost,
    ### so click again): wait until the job really started again (a new `began`) and ended, or
    ### deleting it below would race the start.
    current = ''
    for attempt in range(4):
        card.locator('button:text-is("Start")').wait_for()
        card.locator('button:text-is("Start")').click()
        deadline = time.time() + 20
        while time.time() < deadline:
            current = began()
            if current.rsplit(' ', 1)[0] != first_began and current.endswith('stopped'):
                break
            time.sleep(0.5)
        if current.rsplit(' ', 1)[0] != first_began:
            break
    assert current.rsplit(' ', 1)[0] != first_began and current.endswith('stopped'), current
    ### The card shows the status of the job as of its last refresh (every few seconds).
    time.sleep(4)
    card.locator('p:has-text("Stopped")').first.wait_for()
    ### The card's buttons are rebuilt every few seconds: a click that lands on a button which
    ### is being replaced is lost, so click until the confirmation opens.
    for attempt in range(3):
        card.locator('button:text-is("Delete")').wait_for()
        card.locator('button:text-is("Delete")').click()
        try:
            tab.page.wait_for_selector('.modal.show', timeout=15000)
            break
        except Exception:
            if attempt == 2:
                raise
    ### A job that is still winding down cannot be deleted yet: the modal stays open and says so,
    ### so confirm again (deleting is idempotent) before calling it a failure.
    for attempt in range(3):
        tab.page.click('.modal.show button:text-is("Delete")')
        try:
            tab.page.wait_for_selector('.modal.show', state='detached', timeout=15000)
            break
        except Exception:
            if attempt == 2:
                raise AssertionError(
                    f"The job was not deleted: {tab.page.locator('.modal.show').inner_text()!r}"
                )
            time.sleep(2)
    ### Gone from the server (the card stays until the page reloads, as in v4).
    deadline = time.time() + 30
    while time.time() < deadline:
        listing = server.mrsm('show', 'jobs', '--nopretty')
        if 'w7job' not in listing.stdout:
            break
        time.sleep(1)
    assert 'w7job' not in listing.stdout
    tab.goto('/dash/jobs')
    tab.page.wait_for_load_state('networkidle')
    assert tab.page.locator('.card-title:has-text("w7job")').count() == 0
    assert _errors(tab) == [], _errors(tab)


def test_follow_logs_streams_a_running_job(server, tab):
    started = server.mrsm(
        'sync', 'pipes', '-i', 'sql:w7console', '--loop', '--min-seconds', '3',
        '-d', '--name', 'w7loop', '--yes', '--nopretty',
    )
    assert started.returncode == 0, started.stdout + started.stderr
    try:
        tab.login()
        tab.page.click('#get-jobs-button')
        tab.page.wait_for_selector('.card-title:has-text("w7loop")')
        card = tab.page.locator('.card:has(.card-title:has-text("w7loop"))').first
        card.locator('.dropdown-toggle:has-text("Logs")').click()
        tab.page.click('.dropdown-menu.show .dropdown-item:text-is("Follow logs")')
        tab.page.wait_for_selector('#exec-div', state='visible')
        _wait_text(tab.page, '#exec-header', 'show logs w7loop')
        tab.page.wait_for_function(
            "document.querySelector('#exec-view').textContent.trim().length > 0", timeout=60000,
        )
        assert _errors(tab) == [], _errors(tab)
    finally:
        server.mrsm('stop', 'jobs', 'w7loop', '--nopretty')
        server.mrsm('delete', 'jobs', 'w7loop', '--yes', '--nopretty')


def test_page_contract_page_renders_a_figure_in_its_frame(server, tab):
    tab.login()
    tab.goto('/dash/w7-figure')
    frame = _frame(tab.page)
    frame.wait_for_selector('#w7-figure[data-rendered=true]')
    figure = frame.eval_on_selector('#w7-figure', 'el => ({data: el.data, layout: el.layout})')
    assert _strip(figure) == FIGURE
    assert frame.locator('#w7-figure .trace.bars .point').count() == 3
    assert frame.locator('h2:has-text("W7 Python page")').is_visible()
    ### The console's navbar stays around the page.
    assert tab.page.locator('#pages-navbar-div #logo-img').count() == 1

    ### HTMX: a page element requests a plugin API route and swaps the fragment.
    frame.click('#w7-load')
    frame.wait_for_selector('#w7-fragment')
    assert 'Fragment from a Python plugin route' in frame.inner_text('#w7-slot')
    assert _errors(tab) == [], _errors(tab)


def _strip(figure: dict) -> dict:
    ### Plotly adds defaults (uid, xaxis ranges, ...) to what `newPlot` keeps; compare the
    ### parts the plugin sent.
    data = [
        {k: v for k, v in trace.items() if k in ('type', 'x', 'y', 'name')}
        for trace in figure['data']
    ]
    return {'data': data, 'layout': {'title': {'text': figure['layout']['title']['text']}}}


def _rendered_figure(frame, selector: str = '#w7-figure'):
    frame.wait_for_selector(f"{selector}[data-rendered=true]")
    return frame.eval_on_selector(selector, 'el => ({data: el.data, layout: el.layout})')


def test_native_page_renders_the_same_figure(server, tab):
    if not server.native_built:
        pytest.skip(f"The native plugin was not built: {(server.root / 'native_build.log').read_text()[-800:]}")
    tab.login()
    tab.goto('/dash/w7-figure')
    python_figure = _rendered_figure(_frame(tab.page))
    tab.goto('/dash/w7-native-figure')
    frame = _frame(tab.page)
    rust_figure = _rendered_figure(frame)
    assert frame.locator('h2:has-text("W7 Rust page")').is_visible()
    assert frame.locator('#w7-figure .trace.bars .point').count() == 3
    assert rust_figure == python_figure
    assert _strip(rust_figure) == FIGURE
    assert _errors(tab) == [], _errors(tab)


def _mount(frame, attribute: str, spec: dict, element_id: str, extra_style: str = '') -> None:
    ### Add a page-contract element the way an HTMX swap does, then let the page draw it.
    frame.evaluate(
        """([attribute, spec, id, style]) => {
            const el = document.createElement('div');
            el.id = id;
            el.style.cssText = 'width:420px;height:300px;' + style;
            el.setAttribute(attribute, JSON.stringify(spec));
            document.querySelector('main').append(el);
            document.body.dispatchEvent(new Event('htmx:afterSwap', {bubbles: true}));
        }""",
        [attribute, spec, element_id, extra_style],
    )


#: A figure as plotly.js 2 / plotly.py 5 wrote it: string titles, `titlefont`, a mapbox trace.
LEGACY_FIGURE = {
    'data': [
        {'type': 'bar', 'x': ['a', 'b'], 'y': [1, 2], 'name': 'bars'},
    ],
    'layout': {
        'title': 'Legacy title',
        'xaxis': {'title': 'Legacy x', 'titlefont': {'size': 18}},
        'yaxis': {'title': {'text': 'Already current'}},
    },
}
LEGACY_MAP_FIGURE = {
    'data': [{'type': 'scattermapbox', 'lat': [10], 'lon': [20], 'mode': 'markers'}],
    'layout': {'title': 'Legacy map', 'mapbox': {'style': 'white-bg', 'zoom': 2}},
}


def test_legacy_plotly_figures_are_rewritten_for_plotly_4(server, tab):
    tab.login()
    tab.goto('/dash/w7-figure')
    frame = _frame(tab.page)
    _rendered_figure(frame)
    _mount(frame, 'data-mrsm-figure', LEGACY_FIGURE, 'legacy-bars')
    figure = _rendered_figure(frame, '#legacy-bars')
    assert figure['layout']['title']['text'] == 'Legacy title'
    assert figure['layout']['xaxis']['title']['text'] == 'Legacy x'
    assert figure['layout']['xaxis']['title']['font']['size'] == 18
    assert 'titlefont' not in figure['layout']['xaxis']
    assert figure['layout']['yaxis']['title']['text'] == 'Already current'
    assert frame.locator('#legacy-bars .trace.bars .point').count() == 2

    ### A mapbox trace becomes the maplibre-based `scattermap`; the `mapbox` subplot becomes `map`.
    _mount(frame, 'data-mrsm-figure', LEGACY_MAP_FIGURE, 'legacy-map')
    figure = _rendered_figure(frame, '#legacy-map')
    assert figure['data'][0]['type'] == 'scattermap'
    assert 'mapbox' not in figure['layout'] and figure['layout']['map']['zoom'] == 2
    assert figure['layout']['title']['text'] == 'Legacy map'
    assert _errors(tab) == [], _errors(tab)


def test_plotly_figures_have_no_cloud_button_unless_they_ask(server, tab):
    tab.login()
    tab.goto('/dash/w7-figure')
    frame = _frame(tab.page)
    _rendered_figure(frame)
    frame.hover('#w7-figure')
    assert frame.locator('#w7-figure .modebar-btn').count() > 0
    assert frame.locator('#w7-figure .modebar-btn[data-title="Share chart..."]').count() == 0
    optin = dict(FIGURE, config={'showSendToCloud': True, 'displayModeBar': True})
    _mount(frame, 'data-mrsm-figure', optin, 'cloud-optin')
    _rendered_figure(frame, '#cloud-optin')
    assert frame.locator('#cloud-optin .modebar-btn[data-title="Share chart..."]').count() == 1


def test_maps_load_maplibre_on_demand_and_render(server, tab):
    requested = []
    tab.page.on('request', lambda r: requested.append(r.url.rsplit('/', 1)[-1].split('?')[0]))
    tab.login()
    tab.goto('/dash/w7-figure')
    frame = _frame(tab.page)
    _rendered_figure(frame)
    ### MapLibre is an ES module (and a module worker): a page without a map never fetches it.
    assert 'maplibre-gl.mjs' not in requested and 'maplibre-gl-worker.mjs' not in requested

    geojson = {
        'type': 'FeatureCollection',
        'features': [
            {'type': 'Feature', 'properties': {}, 'geometry': {'type': 'Point', 'coordinates': [-82.4, 34.85]}},
        ],
    }
    with tab.page.expect_response(lambda r: r.url.split('?')[0].endswith('/maplibre-gl-worker.mjs')) as worker:
        _mount(frame, 'data-mrsm-map', dict(geojson, center=[-82.4, 34.85], zoom=6), 'geo-map')
        frame.wait_for_selector('#geo-map[data-rendered=true]')
    assert worker.value.status == 200
    assert 'javascript' in worker.value.headers['content-type']
    frame.wait_for_selector('#geo-map canvas.maplibregl-canvas')
    assert frame.locator('#geo-map .maplibregl-ctrl-zoom-in').count() == 1

    ### A layer spec: any style document (here one with no network sources).
    style = {
        'version': 8, 'sources': {},
        'layers': [{'id': 'bg', 'type': 'background', 'paint': {'background-color': '#dde7ee'}}],
    }
    _mount(frame, 'data-mrsm-map', {'style': style, 'center': [0, 0], 'zoom': 2}, 'style-map')
    frame.wait_for_selector('#style-map[data-rendered=true]')
    frame.wait_for_selector('#style-map canvas.maplibregl-canvas')
    assert 'Map could not be displayed' not in frame.inner_text('main')
    assert requested.count('maplibre-gl.mjs') == 1
    assert _errors(tab) == [], _errors(tab)


def test_plugin_dash_page_fires_a_callback_on_the_core_app(server, tab):
    tab.login()
    tab.goto('/dash/w7-dash', '#w7-dash-title')
    ### The core stores are the console's: `session-store` carries the login.
    _wait_text(tab.page, '#w7-session', 'session: active')
    tab.page.click('#w7-btn')
    _wait_text(tab.page, '#w7-out', 'clicked 1')
    tab.page.click('#w7-btn')
    _wait_text(tab.page, '#w7-out', 'clicked 2')
    ### The page has the console's navbar.
    assert tab.page.locator('#pages-navbar-div #logo-img').count() == 1
    assert _errors(tab) == [], _errors(tab)


def test_rc1_plugin_paths_redirect_to_the_v4_paths(server, tab):
    tab.login()
    response = tab.page.request.get(f"{server.base_url}/dash/w7_dashapp/w7-dash?x=1", max_redirects=0)
    assert response.status in (302, 307), response.status
    assert response.headers['location'] == '/dash/w7-dash?x=1'
    tab.page.goto(f"{server.base_url}/dash/w7_dashapp/w7-dash?x=1")
    tab.page.wait_for_selector('#w7-dash-title')
    assert tab.page.url == f"{server.base_url}/dash/w7-dash?x=1"


def test_session_is_shared_by_console_and_api_plugin(server, context):
    anonymous = Tab(context.new_page(), server.base_url)
    response = anonymous.page.request.get(f"{server.base_url}/w7/session")
    assert response.json() == {'active': False, 'username': None}
    ### A page that needs a login is the login page for an anonymous visitor.
    anonymous.goto('/dash/w7-dash', '#username-input')

    tab = Tab(context.new_page(), server.base_url)
    tab.login()
    ### The API plugin route (served by the Python host) reads the console's session.
    response = tab.page.request.get(f"{server.base_url}/w7/session")
    assert response.json() == {'active': True, 'username': USERNAME}
    tab.goto('/dash/w7-dash', '#w7-dash-title')
    _wait_text(tab.page, '#w7-session', 'session: active')
    ### Logging out in the console ends it everywhere.
    tab.page.click('#sign-out-button')
    tab.page.wait_for_selector('#username-input')
    assert tab.page.request.get(f"{server.base_url}/w7/session").json()['active'] is False
    tab.goto('/dash/w7-dash', '#username-input')


def test_plugins_page_lists_hosted_plugins(server, tab):
    tab.login()
    tab.goto('/dash/plugins')
    tab.page.wait_for_selector('#plugins-content-div')
    _wait_text(tab.page, '#plugins-content-div', 'plugin')
    assert _errors(tab) == [], _errors(tab)


def test_users_page_lists_the_users(server, tab):
    tab.login()
    tab.goto('/dash/users')
    tab.page.wait_for_selector(f'.card:has-text("{USERNAME}")')
    assert _errors(tab) == [], _errors(tab)


def test_tokens_page_registers_a_token(server, tab):
    tab.login()
    tab.goto('/dash/tokens')
    tab.page.wait_for_selector('#tokens-create-button')
    tab.page.click('#tokens-create-button')
    tab.page.wait_for_selector('.modal.show')
    tab.page.fill('.modal.show input[type=text]', 'w7-token')
    tab.page.click('.modal.show button:text-is("Register")')
    _wait_text(tab.page, '.modal.show', 'Client Secret')
    shown = tab.page.inner_text('.modal.show')
    assert 'Client Secret' in shown and 'mrsm-key:' in shown
    ### The secret is only ever shown once: it is stored hashed.
    listing = tab.page.request.get(f"{server.base_url}/tokens")
    assert listing.status in (200, 401, 403)
    assert _errors(tab) == [], _errors(tab)


def test_password_reset_validates_both_fields(server, tab):
    tab.login()
    tab.goto('/dash/password-reset', '#password-reset-input')
    assert tab.page.locator('#password-reset-button').is_disabled()
    tab.page.fill('#password-reset-input', 'a')
    tab.page.wait_for_selector('#password-reset-input.is-invalid')
    tab.page.fill('#password-reset-input', 'a-new-correct-horse-2')
    tab.page.wait_for_selector('#password-reset-input.is-valid')
    tab.page.fill('#password-reset-confirm-input', 'something-else-entirely')
    tab.page.wait_for_selector('#password-reset-confirm-input.is-invalid')
    assert tab.page.locator('#password-reset-button').is_disabled()
    tab.page.fill('#password-reset-confirm-input', 'a-new-correct-horse-2')
    tab.page.wait_for_selector('#password-reset-confirm-input.is-valid')
    assert not tab.page.locator('#password-reset-button').is_disabled()
    assert _errors(tab) == [], _errors(tab)


def test_dash_route_group_hides_console_pages_only(gated_server, context):
    base = gated_server.base_url
    tab = Tab(context.new_page(), base)
    ### The login page is always served so plugin pages requiring a login keep an entrypoint.
    tab.goto('/dash/login', '#username-input')
    tab.goto('/dash', '#username-input')
    tab.page.fill('#username-input', USERNAME)
    tab.page.fill('#password-input', PASSWORD)
    tab.page.click('#login-button')
    tab.page.wait_for_selector('#logo-img, h2:has-text("Page not found")', timeout=30000)
    ### The console's own pages are gone (v4's error page); other route groups keep working.
    for hidden in ('/dash/pipes', '/dash/jobs', '/dash/users', '/dash/tokens'):
        tab.goto(hidden, 'h2:has-text("Page not found")')
    ### Plugin pages are always served.
    tab.goto('/dash/w7-figure')
    _rendered_figure(_frame(tab.page))
    assert tab.page.request.get(f"{base}/version").status == 200


def test_no_dash_serves_no_console_and_no_plugin_pages(nodash_server, context):
    base = nodash_server.base_url
    page = context.new_page()
    for path in ('/dash/pipes', '/dash/w7-figure', '/dash/w7-dash', '/dash/_dash-layout',
                 '/_pages/w7-figure'):
        response = page.request.get(f"{base}{path}", max_redirects=0)
        assert response.status in (404, 401, 403) or 'pip install' in response.text(), path
    assert 'dash-renderer' not in page.request.get(f"{base}/dash/").text()
    ### The API plugin's routes still work through the host.
    assert page.request.get(f"{base}/w7/session").json() == {'active': False, 'username': None}
    assert page.request.get(f"{base}/version").status == 200


ROWCOUNT = (
    "import meerschaum as mrsm\n"
    "print(mrsm.Pipe('sql:w7console', 'w7parent', instance='sql:w7console').get_rowcount())"
)

def test_no_auth_console_needs_no_login(noauth_server, context):
    tab = Tab(context.new_page(), noauth_server.base_url)
    tab.goto('/dash', '#go-button')
    tab.wait_for_dashboard()
    ### Everything the login gates is open, including the pages that list and run things.
    tab.page.click('#go-button')
    _wait_text(tab.page, '#terminal-exec-status', 'Terminal is unavailable', timeout=60000)
    for path in ('/dash/pipes', '/dash/jobs', '/dash/tokens', '/dash/pipelines'):
        tab.goto(path)
        assert tab.page.locator('#username-input').count() == 0, path
    assert tab.errors == [], tab.errors


def test_without_dash_the_console_is_a_pointer_page(nodashpkg_server, context):
    """
    When the host cannot import Dash it says so, and `/dash` explains how to get the console
    (the standalone binary serves the API, `/docs`, `/engine/*` and MCP; the console is Python's).
    """
    base = nodashpkg_server.base_url
    page = context.new_page()
    response = page.request.get(f"{base}/dash")
    assert response.status == 200
    assert 'pip install meerschaum[dash]' in response.text()
    assert 'dash-renderer' not in response.text()
    assert page.request.get(f"{base}/dash/").status == 200
    assert page.request.get(f"{base}/dash/pipes").status == 404
    ### The rest of the server, and the host's plugin routes, are unaffected.
    assert page.request.get(f"{base}/version").status == 200
    assert page.request.get(f"{base}/w7/session").json() == {'active': False, 'username': None}
    ### One warning in the log, not one per request.
    log = nodashpkg_server.log_path.read_text(errors='replace')
    assert log.count('The web console is not available') <= 1
    assert 'Traceback' not in log, log[-2000:]


SETUP_PIPES = """
import meerschaum as mrsm
conn = mrsm.get_connector('sql:w7console')
conn.exec("CREATE TABLE IF NOT EXISTS w7_raw (dt TIMESTAMP, id INTEGER, val REAL)")
conn.exec("DELETE FROM w7_raw")
conn.exec("INSERT INTO w7_raw VALUES ('2026-01-01 00:00:00', 1, 1.5), ('2026-01-02 00:00:00', 2, 2.5)")
parent = mrsm.Pipe('sql:w7console', 'w7parent', instance='sql:w7console', parameters={
    'columns': {'datetime': 'dt', 'id': 'id'},
    'fetch': {'definition': 'SELECT * FROM w7_raw'},
})
if parent.get_id() is None:
    assert parent.register()[0]
child = mrsm.Pipe('sql:w7console', 'w7child', instance='sql:w7console', parameters={
    'columns': {'datetime': 'dt', 'id': 'id'},
    'sql': "SELECT * FROM {{ Pipe('sql:w7console', 'w7parent') }}",
})
if child.get_id() is None:
    assert child.register()[0]
if parent.get_rowcount() == 0:
    success, msg = parent.sync()
    assert success, msg
"""


def test_pipelines_page_graph_gantt_and_runs(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    sync = server.mrsm('sync', 'pipes', '-i', 'sql:w7console', '--nopretty')
    assert sync.returncode == 0, sync.stdout[-1500:] + sync.stderr[-1500:]

    tab.login()
    tab.goto('/dash/pipelines', '#pipelines-graph-div')
    tab.page.wait_for_selector('.pipeline-stage', timeout=60000)
    assert tab.page.locator('.pipeline-stage').count() == 2
    assert "Pipe(" in tab.page.inner_text('.pipeline-stage >> nth=0')
    tab.page.locator('.pipeline-stage').first.click()
    _wait_text(tab.page, '#pipeline-inspector', 'Query')
    assert tab.page.locator('#pipeline-inspector a', has_text='Open pipe').count() == 1
    tab.page.locator('#pipeline-fit').click()
    assert tab.page.locator('#pipeline-canvas svg path').count() == 2
    ### The Gantt chart of the latest run, and its nodes.
    tab.page.wait_for_selector('#pipelines-gantt .barlayer .point', timeout=60000)
    assert tab.page.locator('#pipelines-gantt .barlayer .point').count() >= 2
    assert tab.page.text_content('#pipelines-gantt .gtitle').startswith('Run ')
    assert tab.page.locator('#pipelines-nodes-table tbody tr').count() == 2
    assert 'succeeded' in tab.page.inner_text('#pipelines-nodes-table tbody')
    ### The runs table and the run's events.
    assert tab.page.locator('#pipelines-runs-table tbody tr').count() >= 1
    assert 'succeeded' in tab.page.inner_text('#pipelines-runs-table tbody tr')
    _wait_text(tab.page, '#pipelines-events-pre', 'run_started', timeout=30000)
    events = tab.page.inner_text('#pipelines-events-pre')
    assert 'w7parent' in events and 'w7child' in events

    ### A sync while the page is open appears without a reload (the page refreshes itself).
    before = tab.page.locator('#pipelines-runs-table tbody tr').count()
    again = server.mrsm('sync', 'pipes', '-i', 'sql:w7console', '--nopretty')
    assert again.returncode == 0, again.stdout[-1500:] + again.stderr[-1500:]
    tab.page.wait_for_function(
        "(n) => document.querySelectorAll('#pipelines-runs-table tbody tr').length > n",
        arg=before,
        timeout=60000,
    )
    ### Filtering the runs by status (the filters live in the URL).
    tab.goto('/dash/pipelines?status=failed', '#pipelines-runs-div')
    _wait_text(tab.page, '#pipelines-runs-div', 'No runs match')
    assert _errors(tab) == [], _errors(tab)


def test_the_console_embeds_the_servers_terminal(webterm_server, context):
    base = webterm_server.base_url
    tab = Tab(context.new_page(), base)
    tab.login()
    cookie = [c for c in context.cookies() if c['name'] == 'mrsm-session-id'][0]['value']
    tab.goto('/dash', '#webterm-iframe')
    assert tab.page.get_attribute('#webterm-iframe', 'src') == f"/webterm/{cookie}"
    response = tab.page.request.get(f"{base}/webterm/{cookie}")
    assert response.status == 200, response.status
    assert 'Meerschaum' in response.text()
    ### The dashboard embeds the same terminal beside the action form.
    tab.goto('/dash', '#go-button')
    tab.page.wait_for_selector('#webterm-iframe')
    assert tab.page.get_attribute('#webterm-iframe', 'src') == f"/webterm/{cookie}"
    ### The terminal shares the native API listener; no separate terminal service is started.
    assert webterm_server.port_is_open(webterm_server.port)
    if webterm_server.webterm_port != webterm_server.port:
        assert not webterm_server.port_is_open(webterm_server.webterm_port)
    assert _errors(tab) == [], _errors(tab)


def test_without_the_terminal_the_dashboard_shows_the_banner(server, tab):
    tab.login()
    ### `server` runs with `--no-webterm`: v4's banner takes the terminal's place.
    tab.page.wait_for_selector('#console-pre')
    assert 'v' in tab.page.inner_text('#console-pre')
    assert tab.page.locator('#webterm-iframe').count() == 0
    tab.goto('/dash/webterm', '#go-button')
    assert tab.page.locator('#webterm-iframe').count() == 0
    assert _errors(tab) == [], _errors(tab)
