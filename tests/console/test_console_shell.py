#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's shell in Chromium (design §8.1): the top navigation on every console page, the
Overview at `/dash`, the Terminal page (v4's dashboard) and the Connectors page, on a desktop and
at phone width. Opt-in (`MRSM_TEST_CONSOLE=1`); see `tests/console/conftest.py`.
"""

import pytest

from tests.console.conftest import CONNECTOR_SECRET, Tab
from tests.console.test_console import SETUP_PIPES, _errors, _wait_text

#: The pipes of `test_console.py`, synced without reading a row count first.
SHELL_SETUP = SETUP_PIPES.replace('if parent.get_rowcount() == 0:\n    ', 'if True:\n    ')

pytestmark = pytest.mark.console

OVERVIEW_PIPELINE = (
    "\nfrom meerschaum.core.Pipeline._saved import save_pipeline\n"
    "save_pipeline('overview-line', ['sql:w7console|sql:w7console|w7parent|None', "
    "'sql:w7console|sql:w7console|w7child|None'], 'sql:w7console')\n"
)

#: Console pages and the section the navigation marks on each.
SECTIONS = {
    '/dash/pipes': 'pipes',
    '/dash/pipelines': 'pipelines',
    '/dash/jobs': 'jobs',
    '/dash/runs': 'jobs',
    '/dash/connectors': 'connectors',
    '/dash/plugins': 'plugins',
    '/dash/tokens': 'tokens',
    '/dash/users': 'users',
    '/dash/password-reset': 'settings',
}


def test_overview_tiles_link_to_their_pages(server, tab):
    setup = server.python(SHELL_SETUP + OVERVIEW_PIPELINE)
    assert setup.returncode == 0, setup.stderr[-2000:]
    sync = server.mrsm('sync', 'pipelines', 'overview-line', '-i', 'sql:w7console', '--nopretty')
    assert sync.returncode == 0, sync.stdout[-1500:] + sync.stderr[-1500:]

    tab.login(path='/dash')
    page = tab.page
    page.wait_for_selector('#overview-pipes-total')
    assert int(page.inner_text('#overview-pipes-total').replace(',', '')) >= 2
    ### Saved Pipelines with their last run; the run is in Recent runs too.
    _wait_text(page, '#overview-pipelines-list', "'overview-line'")
    _wait_text(page, '#overview-runs-list', "'overview-line'")
    assert 'Succeeded' in page.inner_text('#overview-pipelines-list').title()
    page.wait_for_selector('#overview-jobs-total')
    for state in ('running', 'stalled', 'failing', 'stopped'):
        assert page.locator(f'#overview-jobs-{state}').count() == 1, state
    assert page.get_attribute('#overview-jobs-stalled', 'href') == '/dash/jobs?status=stalled'
    ### Not an empty instance: the tiles, not the empty state.
    assert not page.locator('#overview-empty').is_visible()

    ### Each tile opens its page; a Pipeline opens on its last run.
    link = page.locator('#overview-pipelines-list a', has_text='overview-line').first
    assert 'run_id=overview-line-' in link.get_attribute('href')
    link.click()
    page.wait_for_function("location.pathname === '/dash/pipelines/overview-line'")
    page.click('#logo-img')
    page.wait_for_selector('#overview-pipes-total')
    page.click('#overview-pipes .overview-tile-title')
    page.wait_for_function("location.pathname === '/dash/pipes'")
    assert _errors(tab) == [], _errors(tab)


def test_overview_of_an_empty_instance_offers_new_pipe(server, tab):
    tab.login(path='/dash')
    tab.goto('/dash?instance=sql:w7empty', '#overview-page')
    page = tab.page
    page.wait_for_selector('#overview-empty', state='visible')
    assert page.inner_text('#overview-empty .overview-empty-title') == 'No pipes yet'
    assert page.get_attribute('#overview-new-pipe', 'href') == '/dash/pipes/new'
    ### `server` runs `--no-webterm`: no Open terminal.
    assert page.locator('#overview-open-terminal').count() == 0
    ### The empty state takes the Pipes tile's place only: the server's jobs and the instance's
    ### Pipelines and runs keep their tiles.
    assert page.locator('#overview-pipes #overview-empty').count() == 1
    assert not page.locator('#overview-pipes-body').is_visible()
    page.wait_for_selector('#overview-jobs-total')
    _wait_text(page, '#overview-runs-body', 'No runs yet')
    for tile in ('#overview-pipelines', '#overview-jobs', '#overview-runs'):
        assert page.locator(tile).is_visible(), tile
    ### No captions: a title and the actions.
    assert page.locator('#overview-empty p').count() == 0
    assert page.locator('#pages-instance-select').input_value() == 'sql:w7empty'
    assert _errors(tab) == [], _errors(tab)


def test_every_console_page_marks_its_section(server, tab):
    tab.login(path='/dash')
    for path, section in SECTIONS.items():
        tab.goto(path, '#pages-navbar-div')
        tab.page.wait_for_selector(f'#console-nav-{section}.active', state='attached', timeout=30000)
        assert tab.page.locator('#pages-navbar-div .active').count() >= 1, path
        assert tab.page.locator('#pages-instance-select').count() == 1, path
    assert _errors(tab) == [], _errors(tab)


def test_old_dashboard_links_open_the_terminal_page(server, tab):
    tab.login(path='/dash')
    tab.page.goto(f"{server.base_url}/dash?action=show&subaction=pipes")
    tab.page.wait_for_function("location.pathname === '/dash/terminal'", timeout=30000)
    assert 'action=show' in tab.page.url
    tab.wait_for_dashboard()
    ### `/dash/webterm` (the terminal's old URL) is the Terminal page too.
    tab.goto('/dash/webterm', '#go-button')
    assert tab.page.locator('#instance-select').count() == 1
    assert _errors(tab) == [], _errors(tab)


def test_connectors_page_masks_secrets_and_tests_connections(server, context):
    tab = Tab(context.new_page(), server.base_url)
    bodies = []

    def _record(response):
        if response.url.startswith(server.base_url):
            try:
                bodies.append(response.text())
            except Exception:
                pass

    tab.login(path='/dash')
    tab.page.on('response', _record)
    tab.goto('/dash/connectors', '#connectors-table')
    page = tab.page
    row = page.locator('#connectors-table tr[data-keys="sql:w7secret"]')
    assert 'Environment' in row.inner_text() and 'MRSM_SQL_W7SECRET' in row.inner_text()
    assert '127.0.0.1' in row.inner_text() and 'postgresql' in row.inner_text()
    row.locator('summary').click()
    assert 'w7user:' in row.inner_text()
    ### The mask reads as dots, not as `•` escapes.
    assert 'postgresql://w7user:••••••@127.0.0.1:1/w7' in row.inner_text()
    assert '\\u2022' not in row.inner_text()
    ### An administrator tests it: the failure is shown, the password never is.
    row.locator('.connectors-test-button').click()
    page.wait_for_function(
        "document.querySelector('#connectors-table tr[data-keys=\"sql:w7secret\"] "
        ".connectors-test-result').innerText.length > 0",
        timeout=60000,
    )
    assert 'Connected' not in row.locator('.connectors-test-result').inner_text()
    local = page.locator('#connectors-table tr[data-keys="sql:w7console"]')
    local.locator('.connectors-test-button').click()
    _wait_text(page, '#connectors-table tr[data-keys="sql:w7console"] .connectors-test-result', 'Connected')
    ### The filter keeps the matching rows.
    page.fill('#connectors-filter', 'w7secret')
    page.wait_for_function(
        "[...document.querySelectorAll('#connectors-table tbody tr')].filter(r => !r.hidden).length === 1"
    )
    assert CONNECTOR_SECRET not in page.content()
    assert not [body for body in bodies if CONNECTOR_SECRET in body]
    assert _errors(tab) == [], _errors(tab)


def test_connectors_page_is_read_only_for_other_users(server, context):
    result = server.python("""
import meerschaum as mrsm
from meerschaum.core import User
conn = mrsm.get_connector('sql:w7console')
user = User('carol', 'correct-horse-3', instance=conn)
if conn.get_user_id(user) is None:
    success, msg = conn.register_user(user)
    assert success, msg
""")
    assert result.returncode == 0, result.stderr[-2000:]
    tab = Tab(context.new_page(), server.base_url)
    tab.login('carol', 'correct-horse-3', path='/dash')
    tab.goto('/dash/connectors', '#connectors-table')
    assert tab.page.locator('#connectors-table tr[data-keys="sql:w7secret"]').count() == 1
    assert tab.page.locator('.connectors-test-button').count() == 0
    assert CONNECTOR_SECRET not in tab.page.content()
    assert _errors(tab) == [], _errors(tab)


#: The visible links, buttons, summaries and inputs under `scope` shorter than a 44 px target.
SMALL_TARGETS = """(scope) => [...document.querySelectorAll(
    `${scope} a, ${scope} button, ${scope} summary, ${scope} input`
)].filter(e => e.getClientRects().length && getComputedStyle(e).visibility !== 'hidden')
  .map(e => [e.id || e.className || e.tagName, Math.round(e.getBoundingClientRect().height)])
  .filter(([, height]) => height < 44)"""


def test_overview_and_connectors_fit_a_phone(server, browser):
    """
    390 px: every link and button of the Overview and the Connectors page is a 44 px target, and a
    connector's card wraps a long value instead of cutting it off.
    """
    setup = server.python(SHELL_SETUP + OVERVIEW_PIPELINE)
    assert setup.returncode == 0, setup.stderr[-2000:]
    sync = server.mrsm('sync', 'pipelines', 'overview-line', '-i', 'sql:w7console', '--nopretty')
    assert sync.returncode == 0, sync.stdout[-1500:] + sync.stderr[-1500:]
    context = browser.new_context(viewport={'width': 390, 'height': 844}, has_touch=True)
    try:
        tab = Tab(context.new_page(), server.base_url)
        tab.login(path='/dash')
        page = tab.page
        page.wait_for_selector('#overview-pipes-total')
        _wait_text(page, '#overview-pipelines-list', "'overview-line'")
        _wait_text(page, '#overview-runs-list', "'overview-line'")
        page.wait_for_selector('#overview-jobs-total')
        ### An opened run of several pipes lists its pipes' links (the markup of
        ### `overview.build_runs_body`, which this server's runs do not need).
        page.evaluate("""() => {
            const details = document.createElement('details');
            details.className = 'overview-run-pipes';
            details.open = true;
            details.innerHTML = '<summary>2 pipes</summary><ul class="overview-run-targets">'
                + '<li><a class="overview-pipe-link" href="#"><code>Pipe(1, 2)</code></a></li></ul>';
            document.querySelector('#overview-runs-list .overview-run-what').appendChild(details);
        }""")
        small = page.evaluate(SMALL_TARGETS, '#overview-page')
        assert small == [], small

        tab.goto('/dash/connectors', '#connectors-table')
        page.locator('#connectors-table tr[data-keys="sql:w7secret"] summary').click()
        ### A long variable name (or path) wraps beside its label inside the card.
        page.evaluate("""() => {
            document.querySelector('#connectors-table tr[data-keys="sql:w7secret"] .connectors-source-detail')
                .textContent = 'MRSM_SQL_W7SECRET_WITH_A_RATHER_LONG_LABEL_FOR_A_PHONE';
        }""")
        overflowing = page.evaluate("""() => [...document.querySelectorAll('#connectors-table td')]
            .filter(td => td.getClientRects().length && td.scrollWidth > td.clientWidth + 1)
            .map(td => [td.closest('tr').dataset.keys, td.className, td.scrollWidth, td.clientWidth])""")
        assert overflowing == [], overflowing
        small = page.evaluate(SMALL_TARGETS, '#connectors-page')
        assert small == [], small
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()


def test_shell_at_phone_width(server, browser):
    """390 px: no page scrolls sideways, the navigation opens behind the toggler, 44 px targets."""
    context = browser.new_context(viewport={'width': 390, 'height': 844}, has_touch=True)
    try:
        tab = Tab(context.new_page(), server.base_url)
        tab.login(path='/dash')
        page = tab.page
        for path in ('/dash', '/dash/terminal', '/dash/connectors', '/dash/jobs', '/dash/pipelines',
                     '/dash/w7-dash'):
            tab.goto(path, '#pages-navbar-div')
            page.wait_for_load_state('networkidle')
            widths = page.evaluate(
                "[document.documentElement.scrollWidth, document.documentElement.clientWidth]"
            )
            assert widths[0] <= widths[1], (path, widths)
        tab.goto('/dash/connectors', '#connectors-table')
        assert not page.locator('#console-nav-jobs').is_visible()
        page.click('#navbar-toggler')
        page.locator('#console-nav-jobs').wait_for(state='visible')
        heights = page.evaluate(
            "[...document.querySelectorAll('#pages-navbar-div .console-nav-link, #navbar-toggler')]"
            ".filter(e => e.offsetParent).map(e => e.getBoundingClientRect().height)"
        )
        assert heights and min(heights) >= 44, heights
        assert page.locator('#pages-instance-select').is_visible()
        page.click('#console-nav-user .dropdown-toggle')
        page.locator('#sign-out-button').wait_for(state='visible')
        page.click('#console-nav-jobs')
        page.wait_for_function("location.pathname === '/dash/jobs'")
        ### The new page starts with the menu closed.
        page.wait_for_timeout(500)
        assert not page.locator('#console-nav-pipes').is_visible()
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()
