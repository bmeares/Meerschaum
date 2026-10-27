#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Pipes pages (rc15 Q3, Q4, Q27, B5) against a real `mrsm start api`: the table renders,
searches, filters, sorts and opens a pipe; a pipe's tabs render; Sync streams its output and
the destructive actions confirm with the pipe's metric key; Manage is offered to sessions that
may run actions only; New pipe previews a query (its columns fill the pickers) and registers the
pipe; every page fits a phone. Opt-in (`MRSM_TEST_CONSOLE=1`); all data is synthetic.
"""

import json

import pytest

from tests.console.conftest import Server, Tab, USERNAME, PASSWORD, CONNECTOR_SECRET
from tests.console.test_console import SETUP_PIPES, _wait_text, _errors

pytestmark = pytest.mark.console

PARENT = '/dash/pipes/sql:w7console/w7parent'

MORE_PIPES = SETUP_PIPES + """
for i in range(12):
    pipe = mrsm.Pipe('sql:w7console', f'w7table_{i:02d}', instance='sql:w7console', parameters={
        'columns': {'datetime': 'dt', 'id': 'id'},
        ### No `{{ Pipe() }}` reference: the other tests count `w7parent`'s children.
        'sql': "SELECT * FROM w7_raw",
        'tags': ['w7tables'] + (['w7odd'] if i % 2 else []),
    })
    if pipe.get_id() is None:
        assert pipe.register()[0]
"""

ROWS = (
    "import meerschaum as mrsm, json\n"
    "pipe = mrsm.Pipe('sql:w7console', {metric!r}, instance='sql:w7console')\n"
    "print('JSON:' + json.dumps({{'id': pipe.get_id(), 'rows': pipe.get_rowcount(),\n"
    "    'parameters': pipe.get_parameters(apply_symlinks=False, refresh=True)}}))\n"
)


def _pipe(server, metric):
    out = server.python(ROWS.format(metric=metric))
    assert out.returncode == 0, out.stderr[-2000:]
    return json.loads([line for line in out.stdout.splitlines() if line.startswith('JSON:')][-1][5:])


def _stats_loaded(page):
    page.wait_for_function(
        "() => document.querySelectorAll('tr.pipes-table-row').length > 0 && "
        "![...document.querySelectorAll('.pipes-col-rows')].some(td => td.textContent.trim() === '…')",
        timeout=60000,
    )


def _labels(page):
    return [label.strip() for label in page.locator('tr.pipes-table-row .pipes-col-pipe a').all_inner_texts()]


def _rows_column(page):
    """The counted Rows cells of the table, in order (exact on SQLite; `—` cells sort last)."""
    texts = [text.strip().replace(',', '') for text in page.locator('tr.pipes-table-row .pipes-col-rows').all_inner_texts()]
    return [int(text) for text in texts if text.isdigit()]


@pytest.fixture(scope='module')
def secure_server():
    """A server whose non-administrators may not run actions (`start api --secure`)."""
    instance = Server('secure', args=['--secure'], native=False)
    try:
        instance.start()
        result = instance.python(SETUP_PIPES + """
from meerschaum.core import User
assert conn.register_user(User('w7user', 'correct-horse-3', instance=conn))[0]
""")
        assert result.returncode == 0, result.stderr[-2000:]
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()


def test_table_lists_searches_filters_sorts_and_opens_a_pipe(server, tab):
    result = server.python(MORE_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    tab.goto('/dash/pipes', 'tr.pipes-table-row')
    _stats_loaded(tab.page)
    labels = _labels(tab.page)
    assert "Pipe('sql:w7console', 'w7parent')" in labels and "Pipe('sql:w7console', 'w7child')" in labels
    ### Internal pipes (a saved Pipeline's registry) are not listed.
    assert not any("Pipe('mrsm'" in label for label in labels)
    parent_row = tab.page.locator('tr.pipes-table-row', has_text="'w7parent'")
    rows = _pipe(server, 'w7parent')['rows']
    assert rows >= 2
    assert parent_row.locator('.pipes-col-rows').inner_text().strip() == f"{rows:,}"
    assert parent_row.locator('.pipes-col-sync-time').inner_text().startswith('2026-')
    assert 'pipes' in tab.page.inner_text('#pipes-summary')

    ### Search matches keys, targets and tags.
    tab.page.fill('#pipes-search', 'w7odd')
    tab.page.wait_for_function("document.querySelectorAll('tr.pipes-table-row').length === 6", timeout=30000)
    tab.page.fill('#pipes-search', '')
    tab.page.wait_for_function("document.querySelectorAll('tr.pipes-table-row').length > 6", timeout=30000)

    ### A tag filter is in the URL; a shared link opens filtered.
    tab.goto('/dash/pipes?tags=w7tables', 'tr.pipes-table-row')
    tab.page.wait_for_function("document.querySelectorAll('tr.pipes-table-row').length === 12", timeout=30000)
    assert 'w7tables' in tab.page.inner_text('#pipes-tags-dropdown')

    ### Sorting by Rows: most rows first, again the other way (other tests may sync more pipes,
    ### so the order is checked, not which pipe comes first).
    tab.goto('/dash/pipes?connector_keys=sql%3Aw7console', 'tr.pipes-table-row')
    _stats_loaded(tab.page)
    tab.page.click('#pipes-sort-rows')
    tab.page.wait_for_function(
        "document.getElementById('pipes-header-rows').getAttribute('aria-sort') === 'descending'",
        timeout=30000,
    )
    _stats_loaded(tab.page)
    assert _rows_column(tab.page)[0] >= rows
    assert _rows_column(tab.page) == sorted(_rows_column(tab.page), reverse=True)
    tab.page.click('#pipes-sort-rows')
    tab.page.wait_for_function(
        "document.getElementById('pipes-header-rows').getAttribute('aria-sort') === 'ascending'",
        timeout=30000,
    )
    _stats_loaded(tab.page)
    assert _rows_column(tab.page) == sorted(_rows_column(tab.page))
    assert _rows_column(tab.page)[0] < rows

    ### A row opens its pipe's page in place.
    tab.page.locator('tr.pipes-table-row', has_text="'w7parent'").locator('.pipes-col-rows').click()
    tab.page.wait_for_selector('#pipe-detail-title', timeout=30000)
    assert tab.page.url.split('?')[0].endswith('/dash/pipes/sql%3Aw7console/w7parent')
    assert "Pipe('sql:w7console', 'w7parent')" in tab.page.inner_text('#pipe-detail-title')
    assert _errors(tab) == [], _errors(tab)


def test_a_pipes_tabs_render(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    tab.goto(PARENT, '#pipe-detail-content .pipes-tiles')
    overview = tab.page.inner_text('#pipe-detail-content').lower()
    for text in ('Sync time', 'Last sync', 'Columns', 'datetime', 'Target'):
        assert text.lower() in overview, text
    for title, expect in (
        ('Data', 'Recent rows'),
        ('Parameters', 'columns'),
        ('Definition', 'SELECT'),
        ('Lineage', 'Children (1)'),
        ('Overview', 'Last sync'),
    ):
        tab.page.click(f'#pipe-detail-tabs .nav-link:text-is("{title}")')
        _wait_text(tab.page, '#pipe-detail-content', expect)
    ### The active tab is in the URL.
    tab.page.click('#pipe-detail-tabs .nav-link:text-is("Definition")')
    tab.page.wait_for_function("location.search.includes('tab=definition')", timeout=30000)
    tab.goto(PARENT + '?tab=lineage', '#pipe-detail-tabs')
    _wait_text(tab.page, '#pipe-detail-content', 'Parents (0)')
    ### The old URL of a pipe's card still opens its page.
    tab.goto('/dash/pipes/sql:w7console/w7parent?instance=sql:w7console', '#pipe-detail-content .pipes-tiles')
    assert _errors(tab) == [], _errors(tab)


def test_sync_streams_its_output_and_destructive_actions_confirm(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    tab.goto(PARENT, '#pipe-detail-content .pipes-tiles')
    for button in ('#pipe-action-sync', '#pipe-action-verify', '#pipe-edit-toggle', '#pipe-manage-menu'):
        assert tab.page.locator(button).count() == 1, button

    tab.page.click('#pipe-action-sync')
    tab.page.wait_for_selector('#pipe-action-panel:not([hidden])', timeout=30000)
    assert tab.page.inner_text('#pipe-action-command').startswith('sync pipes -c sql:w7console -m w7parent')
    tab.page.wait_for_function(
        "document.getElementById('pipe-action-status').textContent === 'finished'", timeout=120000,
    )
    assert tab.page.inner_text('#pipe-action-output').strip()

    ### Clear asks for the metric key; Cancel leaves the rows alone.
    before = _pipe(server, 'w7parent')['rows']
    assert before >= 2
    tab.page.click('#pipe-manage-menu .dropdown-toggle')
    tab.page.click('#pipe-manage-clear')
    tab.page.wait_for_selector('#pipe-confirm-modal', state='visible')
    assert "Pipe('sql:w7console', 'w7parent')" in tab.page.inner_text('#pipe-confirm-modal')
    assert tab.page.is_disabled('#pipe-confirm-button')
    tab.page.fill('#pipe-confirm-input', 'w7paren')
    assert tab.page.is_disabled('#pipe-confirm-button')
    tab.page.fill('#pipe-confirm-input', 'w7parent')
    tab.page.wait_for_function("!document.getElementById('pipe-confirm-button').disabled")
    tab.page.click('#pipe-confirm-cancel')
    tab.page.wait_for_selector('#pipe-confirm-modal', state='hidden')
    assert _pipe(server, 'w7parent')['rows'] == before

    ### Delete is confirmed the same way.
    tab.page.click('#pipe-manage-menu .dropdown-toggle')
    tab.page.click('#pipe-manage-delete')
    tab.page.wait_for_selector('#pipe-confirm-modal', state='visible')
    tab.page.wait_for_function("document.getElementById('pipe-confirm-button').textContent === 'Delete'")
    tab.page.click('#pipe-confirm-cancel')
    tab.page.wait_for_selector('#pipe-confirm-modal', state='hidden')
    assert _pipe(server, 'w7parent')['id'] is not None
    assert _errors(tab) == [], _errors(tab)


def test_actions_are_absent_for_a_user_who_may_not_run_them(secure_server, context):
    tab = Tab(context.new_page(), secure_server.base_url)
    ### Without a session the page is the login form.
    tab.goto(PARENT, '#username-input')
    assert tab.page.locator('#pipe-action-sync').count() == 0
    tab.login('w7user', 'correct-horse-3')
    tab.goto(PARENT, '#pipe-detail-content .pipes-tiles')
    for button in ('#pipe-action-sync', '#pipe-action-verify', '#pipe-edit-toggle', '#pipe-manage-menu',
                   '#pipe-manage-delete'):
        assert tab.page.locator(button).count() == 0, button
    tab.goto('/dash/pipes', 'tr.pipes-table-row')
    assert tab.page.locator('#pipes-new-button').count() == 0
    ### An administrator gets them.
    admin = Tab(context.browser.new_context().new_page(), secure_server.base_url)
    admin.login(USERNAME, PASSWORD)
    admin.goto(PARENT, '#pipe-detail-tabs')
    admin.page.wait_for_selector('#pipe-manage-menu')
    assert admin.page.locator('#pipe-action-sync').count() == 1
    admin.goto('/dash/pipes', 'tr.pipes-table-row')
    assert admin.page.locator('#pipes-new-button').count() == 1
    assert _errors(tab) == [] and _errors(admin) == [], (_errors(tab), _errors(admin))
    admin.page.context.close()


def test_new_pipe_preview_fills_the_pickers_and_register_creates_the_pipe(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    metric = 'w7new'
    server.python(f"import meerschaum as mrsm\nmrsm.Pipe('sql:w7console', {metric!r}, instance='sql:w7console').delete()\n")
    try:
        tab.login()
        tab.goto('/dash/pipes', 'tr.pipes-table-row')
        tab.page.click('#pipes-new-button')
        tab.page.wait_for_selector('#new-pipe-connector', timeout=30000)
        tab.page.fill('#new-pipe-connector', 'sql:w7console')
        tab.page.fill('#new-pipe-metric', metric)
        tab.page.evaluate(
            """(text) => {
                const el = document.getElementById('new-pipe-query');
                const editor = (el.classList.contains('ace_editor') ? el : el.querySelector('.ace_editor')).env.editor;
                editor.setValue(text, -1);
            }""",
            "SELECT * FROM {{ Pipe('sql:w7console', 'w7parent') }} ORDER BY dt",
        )
        tab.page.click('#new-pipe-preview')
        tab.page.wait_for_selector('#new-pipe-preview-result table', timeout=60000)
        preview = tab.page.inner_text('#new-pipe-preview-result')
        assert 'dt' in preview and 'val' in preview and '2026-01-01' in preview
        ### The datetime column is guessed; the columns are offered for both pickers.
        assert tab.page.input_value('#new-pipe-datetime') == 'dt'
        options = tab.page.eval_on_selector_all('#new-pipe-columns option', 'nodes => nodes.map(n => n.value)')
        assert options == ['dt', 'id', 'val']
        tab.page.fill('#new-pipe-id', 'id')
        ### Nothing is saved before Register.
        assert _pipe(server, metric)['id'] is None
        tab.page.click('#new-pipe-register')
        tab.page.wait_for_selector('#pipe-detail-title', timeout=60000)
        assert tab.page.url.split('?')[0].endswith(f'/dash/pipes/sql%3Aw7console/{metric}')
        saved = _pipe(server, metric)
        assert saved['id'] is not None
        assert saved['parameters']['columns'] == {'datetime': 'dt', 'id': 'id'}
        assert 'w7parent' in saved['parameters']['fetch']['definition']

        ### Registering it again is refused inline.
        tab.goto('/dash/pipes/new', '#new-pipe-connector')
        tab.page.fill('#new-pipe-connector', 'sql:w7console')
        tab.page.fill('#new-pipe-metric', metric)
        tab.page.click('#new-pipe-register')
        _wait_text(tab.page, '#new-pipe-alert', 'already registered')
        ### Missing keys are named.
        tab.page.fill('#new-pipe-metric', '')
        tab.page.click('#new-pipe-register')
        _wait_text(tab.page, '#new-pipe-alert', 'Enter the metric')
        assert _errors(tab) == [], _errors(tab)
    finally:
        server.python(f"import meerschaum as mrsm\nmrsm.Pipe('sql:w7console', {metric!r}, instance='sql:w7console').delete()\n")


def test_register_and_sync_follows_the_first_sync(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    metric = 'w7new_synced'
    server.python(f"import meerschaum as mrsm\nmrsm.Pipe('sql:w7console', {metric!r}, instance='sql:w7console').delete()\n")
    try:
        tab.login()
        tab.goto('/dash/pipes/new', '#new-pipe-connector')
        tab.page.fill('#new-pipe-connector', 'sql:w7console')
        tab.page.fill('#new-pipe-metric', metric)
        tab.page.evaluate(
            """(text) => {
                const el = document.getElementById('new-pipe-query');
                (el.classList.contains('ace_editor') ? el : el.querySelector('.ace_editor')).env.editor.setValue(text, -1);
            }""",
            "SELECT * FROM w7_raw",
        )
        tab.page.fill('#new-pipe-datetime', 'dt')
        tab.page.click('#new-pipe-register-sync')
        tab.page.wait_for_selector('#pipe-action-panel:not([hidden])', timeout=60000)
        tab.page.wait_for_function(
            "document.getElementById('pipe-action-status').textContent === 'finished'", timeout=120000,
        )
        assert _pipe(server, metric)['rows'] == 2
        assert _errors(tab) == [], _errors(tab)
    finally:
        server.python(f"import meerschaum as mrsm\nmrsm.Pipe('sql:w7console', {metric!r}, instance='sql:w7console').delete()\n")


def test_pipes_pages_fit_a_phone(server, browser):
    result = server.python(MORE_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    context = browser.new_context(viewport={'width': 390, 'height': 844}, has_touch=True, is_mobile=True)
    try:
        tab = Tab(context.new_page(), server.base_url)
        tab.login()
        measure = """() => {
            const small = [...document.querySelectorAll('.pipes-page button, .pipes-page a.btn, .pipes-page input:not([type=checkbox]):not([type=radio]), .pipes-page select, #pipe-detail-tabs .nav-link')]
                .filter(el => el.offsetParent !== null)
                .filter(el => el.getBoundingClientRect().height < 43.5)
                .map(el => (el.id || el.className || el.tagName) + ':' + Math.round(el.getBoundingClientRect().height));
            return {overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth, small};
        }"""
        for path, selector in (
            ('/dash/pipes', 'tr.pipes-table-row'),
            (PARENT, '#pipe-detail-content .pipes-tiles'),
            (PARENT + '?tab=data', '#pipe-detail-content .pipes-section'),
            ('/dash/pipes/new', '#new-pipe-connector'),
        ):
            tab.goto(path, selector)
            tab.page.wait_for_timeout(500)
            layout = tab.page.evaluate(measure)
            assert layout['overflow'] <= 0, (path, layout)
            assert layout['small'] == [], (path, layout)
        ### The table is a list of cards: no header row, every pipe's keys visible.
        tab.goto('/dash/pipes', 'tr.pipes-table-row')
        assert not tab.page.locator('#pipes-table thead').is_visible()
        assert tab.page.locator('#pipes-sort-select').is_visible()
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()


def test_the_search_is_in_the_url_without_a_history_entry_per_word(server, tab):
    result = server.python(MORE_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    tab.goto('/dash/pipes', 'tr.pipes-table-row')
    history = tab.page.evaluate('history.length')
    ### Typing (slower than the box's debounce) updates `?q=` in place.
    tab.page.type('#pipes-search', 'w7odd', delay=350)
    tab.page.wait_for_function("document.querySelectorAll('tr.pipes-table-row').length === 6", timeout=30000)
    tab.page.wait_for_function("new URLSearchParams(location.search).get('q') === 'w7odd'", timeout=30000)
    assert tab.page.evaluate('history.length') == history
    ### A filter keeps the search in the URL; the shared link opens the same rows.
    tab.page.select_option('#pipes-status-select', 'never')
    tab.page.wait_for_function(
        "new URLSearchParams(location.search).get('status') === 'never' && "
        "new URLSearchParams(location.search).get('q') === 'w7odd'",
        timeout=30000,
    )
    shared = tab.page.url[len(server.base_url):]
    tab.goto(shared, 'tr.pipes-table-row')
    tab.page.wait_for_function("document.querySelectorAll('tr.pipes-table-row').length === 6", timeout=30000)
    assert tab.page.input_value('#pipes-search') == 'w7odd'
    ### Clearing the box drops it.
    tab.page.fill('#pipes-search', '')
    tab.page.wait_for_function("!new URLSearchParams(location.search).has('q')", timeout=30000)
    assert _errors(tab) == [], _errors(tab)


def test_a_pipe_whose_instance_cannot_be_read_is_not_called_unregistered(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    bodies = []

    def _record(response):
        if response.url.startswith(server.base_url):
            try:
                bodies.append(response.text())
            except Exception:
                pass

    tab.login()
    tab.page.on('response', _record)
    ### `sql:w7secret` refuses connections (the server fixture): the page says so, offers nothing.
    tab.goto('/dash/pipes/demo:x/y?instance=sql:w7secret', '#pipe-detail-tabs')
    _wait_text(tab.page, '#pipe-detail-content', 'Could not read sql:w7secret')
    assert 'not registered' not in tab.page.inner_text('#pipe-detail-content')
    tab.page.wait_for_selector('#pipe-detail-actions', state='hidden')
    assert CONNECTOR_SECRET not in tab.page.content()
    assert not [body for body in bodies if CONNECTOR_SECRET in body]
    ### A pipe that is really not registered offers Register, which fills in New pipe.
    tab.goto('/dash/pipes/sql:w7console/w7missing/w7loc', '#pipe-detail-tabs')
    _wait_text(tab.page, '#pipe-detail-content', 'is not registered.')
    tab.page.wait_for_selector('#pipe-detail-actions', state='hidden')
    tab.page.click('#pipe-detail-register')
    tab.page.wait_for_selector('#new-pipe-connector', timeout=30000)
    assert tab.page.input_value('#new-pipe-connector') == 'sql:w7console'
    assert tab.page.input_value('#new-pipe-metric') == 'w7missing'
    assert tab.page.input_value('#new-pipe-location-key') == 'w7loc'
    assert _pipe(server, 'w7missing')['id'] is None
    ### A registered pipe's actions stay.
    tab.goto(PARENT, '#pipe-detail-content .pipes-tiles')
    assert tab.page.locator('#pipe-detail-actions').is_visible()
    assert _errors(tab) == [], _errors(tab)


def test_new_pipe_and_copy_leave_out_the_hosts_own_variables(server, tab):
    result = server.python(SETUP_PIPES)
    assert result.returncode == 0, result.stderr[-2000:]
    tab.login()
    tab.goto('/dash/pipes/new', '#new-pipe-connector')
    connectors = tab.page.eval_on_selector_all('#new-pipe-connectors option', 'nodes => nodes.map(n => n.value)')
    instances = tab.page.eval_on_selector_all('#new-pipe-instance option', 'nodes => nodes.map(n => n.value)')
    tab.goto(PARENT, '#pipe-detail-content .pipes-tiles')
    tab.page.click('#pipe-manage-menu .dropdown-toggle')
    tab.page.click('#pipe-manage-copy')
    tab.page.wait_for_selector('#pipe-copy-instance', state='visible')
    copy = tab.page.eval_on_selector_all('#pipe-copy-instance option', 'nodes => nodes.map(n => n.value)')
    for name, values in (('connectors', connectors), ('instances', instances), ('copy', copy)):
        assert 'sql:w7console' in values, (name, values)
        assert not [value for value in values if value.startswith('api:host')], (name, values)
    assert _errors(tab) == [], _errors(tab)
