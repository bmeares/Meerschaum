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
    ### The pipes by sync status, each linked to the Pipes page filtered by it.
    page.wait_for_selector('#overview-pipes-ok')
    assert int(page.inner_text('#overview-pipes-ok .overview-job-count')) >= 2
    assert page.get_attribute('#overview-pipes-failing', 'href') == '/dash/pipes?status=failing'
    ### The latest syncs link to their pipes, and each connector to the pipes it feeds.
    page.wait_for_selector('#overview-pipes-latest .overview-pipe-link')
    assert page.get_attribute('#overview-pipes-latest .overview-pipe-link', 'href').startswith('/dash/pipes/')
    chips = page.locator('#overview-pipes-connectors a:not(.overview-chip-more)')
    assert chips.count() >= 1
    assert all(href.startswith('/dash/pipes?connector_keys=') for href in chips.evaluate_all(
        'links => links.map(link => link.getAttribute("href"))'))
    ### The saved Pipelines by their last run, without a list of them; the run is in Recent runs.
    ### The session's server is shared with other files' tests, whose Pipelines count too.
    page.wait_for_function(
        "() => parseInt(document.querySelector('#overview-pipelines-succeeded .overview-job-count')"
        "?.textContent.replace(/,/g, '') || '0', 10) >= 1",
        timeout=30000,
    )
    assert page.locator('#overview-pipelines-list').count() == 0
    _wait_text(page, '#overview-pipelines-runs', 'run')
    assert page.locator('#overview-pipelines-history .overview-hour').count() == 24
    _wait_text(page, '#overview-pipelines-last-success', 'overview-line')
    _wait_text(page, '#overview-runs-list', "'overview-line'")
    page.wait_for_selector('#overview-jobs-total')
    for state in ('running', 'stalled', 'failing', 'stopped'):
        assert page.locator(f'#overview-jobs-{state}').count() == 1, state
    assert page.get_attribute('#overview-jobs-stalled', 'href') == '/dash/jobs?status=stalled'
    ### Not an empty instance: the tiles, not the empty state.
    assert not page.locator('#overview-empty').is_visible()

    ### Each tile opens its page; the last success opens its Pipeline on that run.
    link = page.locator('#overview-pipelines-last-success a')
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
    _wait_text(page, '#overview-runs-body', 'No recent runs')
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
    assert '127.0.0.1' not in row.inner_text()
    row.get_by_role('button',name='sql:w7secret',exact=True).click()
    modal = page.locator('.connector-info-modal.show')
    modal.wait_for()
    assert '127.0.0.1' in modal.inner_text() and 'postgresql' in modal.inner_text()
    ### The settings are shown at once (RC25: no Inspect button).
    modal.locator('.connectors-attributes').wait_for()
    assert modal.get_by_role('button',name='Inspect',exact=True).count() == 0
    assert 'w7user:' in modal.inner_text()
    ### The mask reads as dots, not as `•` escapes.
    assert 'postgresql://w7user:••••••@127.0.0.1:1/w7' in modal.inner_text()
    assert '\\u2022' not in modal.inner_text()
    ### An administrator tests it: the failure is shown, the password never is.
    modal.locator('.connectors-test-button').click()
    page.wait_for_function(
        "document.querySelector('.connector-info-modal.show "
        ".connectors-test-result').innerText.length > 0",
        timeout=60000,
    )
    assert 'Connected' not in modal.locator('.connectors-test-result').inner_text()
    modal.get_by_role('button',name='Close',exact=True).last.click()
    modal.wait_for(state='hidden')
    local = page.locator('#connectors-table tr[data-keys="sql:w7console"]')
    local.get_by_role('button',name='sql:w7console',exact=True).click()
    modal.wait_for()
    modal.locator('.connectors-test-button').click()
    _wait_text(page, '.connector-info-modal.show .connectors-test-result', 'Connected')
    modal.get_by_role('button',name='Close',exact=True).last.click()
    modal.wait_for(state='hidden')
    ### The filter keeps the matching rows.
    page.fill('#connectors-filter', 'w7secret')
    page.wait_for_function(
        "[...document.querySelectorAll('#connectors-table tr[data-keys]')].filter(r => !r.hidden).length === 1"
    )
    ### RC28: and the groups of the other connector types.
    page.wait_for_function(
        "[...document.querySelectorAll('#connectors-table tbody.connectors-group')].filter(g => !g.hidden)"
        ".map(g => g.dataset.type).join() === 'sql'"
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
        _wait_text(page, '#overview-pipelines-last-success', 'overview-line')
        _wait_text(page, '#overview-runs-list', "'overview-line'")
        page.wait_for_selector('#overview-jobs-total')
        page.wait_for_selector('#overview-pipes-latest')
        ### Nothing is wider than the phone.
        assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
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
        page.locator('#connectors-table tr[data-keys="sql:w7secret"]').get_by_role(
            'button',name='sql:w7secret',exact=True).click()
        modal = page.locator('.connector-info-modal.show')
        modal.wait_for()
        modal.locator('.connectors-attributes').wait_for()
        small = page.evaluate(SMALL_TARGETS, '.connector-info-modal.show')
        assert small == [], small
        assert modal.evaluate('element => element.scrollWidth <= element.clientWidth + 1')
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


#: A Pipeline whose name is longer than the Recent runs column (RC31 cut it off with an ellipsis).
LONG_PIPELINE = 'overview-line-with-a-name-longer-than-the-recent-runs-column-holds'


def test_overview_shows_next_runs_and_whole_pipeline_names(server, tab):
    import os
    import pathlib
    setup = server.python(SHELL_SETUP + OVERVIEW_PIPELINE.replace("'overview-line'", repr(LONG_PIPELINE)))
    assert setup.returncode == 0, setup.stderr[-2000:]
    sync = server.mrsm('sync', 'pipelines', LONG_PIPELINE, '-i', 'sql:w7console', '--nopretty')
    assert sync.returncode == 0, sync.stdout[-1500:] + sync.stderr[-1500:]
    ### A job waiting ten minutes between laps.
    name = 'w32-next-run'
    started = server.mrsm(
        'show', 'version', '--loop', '--min-seconds', '600', '-d', '--name', name, '--yes',
        '--nopretty', '--no-stagger',
    )
    assert started.returncode == 0, started.stdout + started.stderr
    try:
        tab.login(path='/dash')
        page = tab.page
        page.wait_for_selector('#overview-jobs-total')
        ### The tiles refresh every 30 s: reload until the job's first lap has ended.
        for _ in range(12):
            if page.locator('#overview-next-runs .overview-next-job', has_text=name).count():
                break
            page.wait_for_timeout(2500)
            page.reload()
            page.wait_for_selector('#overview-jobs-total')
        row = page.locator('#overview-next-runs .overview-next-run', has_text=name)
        assert row.count() == 1
        assert row.locator('a').get_attribute('href') == f'/dash/jobs/{name}'
        assert row.locator('.overview-when').inner_text() in ('in 9 min', 'in 10 min')
        assert 'UTC' in row.locator('.overview-when').get_attribute('title')

        ### Recent runs: the whole name, wrapped, not cut off.
        _wait_text(page, '#overview-runs-list', LONG_PIPELINE)
        link = page.locator('#overview-runs-list .overview-pipeline-link', has_text=LONG_PIPELINE)
        assert link.count() == 1
        clipped = link.evaluate('el => el.scrollWidth > el.clientWidth + 1')
        assert not clipped
        assert link.evaluate('el => getComputedStyle(el).textOverflow') != 'ellipsis'
        ### The last success names its Pipeline in full too.
        _wait_text(page, '#overview-pipelines-last-success', LONG_PIPELINE)
        fact = page.locator('#overview-pipelines-last-success .overview-fact-name')
        assert not fact.evaluate('el => el.scrollWidth > el.clientWidth + 1')
        shots = os.environ.get('MRSM_TEST_CONSOLE_SHOTS')
        if shots:
            pathlib.Path(shots).mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(pathlib.Path(shots) / 'rc32-overview.png'), full_page=True)
            page.set_viewport_size({'width': 390, 'height': 844})
            page.wait_for_timeout(500)
            page.screenshot(path=str(pathlib.Path(shots) / 'rc32-overview-phone.png'), full_page=True)
        assert _errors(tab) == [], _errors(tab)
    finally:
        server.mrsm('stop', 'jobs', name, '--yes', '--nopretty')
        server.mrsm('delete', 'jobs', name, '--yes', '--nopretty')
