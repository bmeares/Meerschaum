"""
The Performance page's Engine section, against an instance with real engine history: runs by
outcome, freshness with a failing pipe and a child behind its parent, the dirty-range backlog,
failed nodes with the planner's reason, and a run linked to its saved Pipeline. It refreshes in
place. Opt-in (`MRSM_TEST_CONSOLE=1`); see `tests/console/conftest.py`.
"""

from __future__ import annotations

import pathlib

import pytest

from tests.console.conftest import Server, Tab
from tests.console.test_rc28_performance_connectors import SCROLL_TO_BOTTOM

pytestmark = pytest.mark.console

#: A saved Pipeline `engine-demo` (`clean` with its parent `raw`), a run of `broken` that fails,
#: and new source rows synced only into `raw`, so `clean` is behind its parent.
SEED = '''
from datetime import datetime, timedelta, timezone
import meerschaum as mrsm
from meerschaum import _core
from meerschaum.core.Pipeline._saved import save_pipeline
c = mrsm.get_connector('sql:w7console')
c.exec("CREATE TABLE src (ts TIMESTAMP, id INTEGER, v REAL)")
now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
def add(n, start):
    c.exec_queries([f"INSERT INTO src VALUES ('{start + timedelta(minutes=i)}', {i % 3}, {i})" for i in range(n)])
cols = {'datetime': 'ts', 'id': 'id'}
add(30, now - timedelta(hours=3))
raw = mrsm.Pipe('sql:w7console', 'raw', instance=c, parameters={'sql': 'SELECT ts, id, v FROM src', 'columns': cols})
clean = mrsm.Pipe('sql:w7console', 'clean', instance=c, parameters={'columns': cols, 'sql': (
    "SELECT ts, id, v FROM {{ Pipe('sql:w7console', 'raw', instance='sql:w7console') }} WHERE v >= 0")})
broken = mrsm.Pipe('sql:w7console', 'broken', instance=c,
                   parameters={'fetch': {'definition': 'SELECT * FROM missing_source'}})
for pipe in (raw, clean, broken):
    assert pipe.register()[0]
save_pipeline('engine-demo', ['sql:w7console|sql:w7console|clean|None'], c, parents=True,
              access={'visibility': 'public'})
assert mrsm.Pipeline('engine-demo', instance=c).sync()[0]
assert not broken.sync(parents=True)[0]
add(10, now - timedelta(minutes=20))
assert raw.sync(parents=True)[0]
_core.flush_sync_statuses(timeout=10.0)
'''


def artifacts() -> pathlib.Path:
    path = pathlib.Path.home() / '.cache' / 'mrsm-performance-engine-ui' / 'screenshots'
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture(scope='module')
def engine_server():
    server = Server('engine', native=False)
    try:
        server.start()
        result = server.python(SEED)
        assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
        yield server
    finally:
        server.stop()


def _stats(page) -> dict:
    return page.evaluate("""() => Object.fromEntries(
        [...document.querySelectorAll('#performance-engine .performance-stat')]
            .map(stat => [stat.querySelector('dt').textContent, stat.querySelector('dd').textContent]))""")


@pytest.mark.parametrize('width', [1440, 390])
def test_engine_section_shows_runs_freshness_backlog_and_nodes(engine_server, browser, width):
    context = browser.new_context(viewport={'width': width, 'height': 900}, has_touch=width < 768)
    try:
        tab = Tab(context.new_page(), engine_server.base_url)
        tab.login(path='/dash')
        tab.goto('/dash/performance', '#performance-page')
        page = tab.page
        section = page.locator('#performance-engine')
        section.locator('#performance-engine-tabs').wait_for()
        page.wait_for_function("!document.getElementById('performance-refresh').disabled")
        stats = _stats(page)
        assert stats['Runs'] == '3' and stats['Failed runs'] == '1', stats
        assert stats['Unfinished runs'] == '0' and stats['P95 run duration'] != 'n/a', stats
        assert stats['Pipes failing'] == '1' and stats['Never synced'] == '1', stats
        assert stats['Children behind'] == '1', stats
        section.locator('#performance-engine-runs .js-plotly-plot').wait_for()
        section.locator('#performance-engine-duration .js-plotly-plot').wait_for()

        ### Freshness: the failing pipe first, as `Pipe(...)` linking to its page.
        freshness = section.locator('[id*="engine-freshness"] tbody tr')
        assert freshness.count() == 3
        first = freshness.first
        assert first.locator('td').first.inner_text() == "Pipe('sql:w7console', 'broken')"
        assert first.locator('a').get_attribute('href') == (
            '/dash/pipes/sql%3Aw7console/broken?instance=sql%3Aw7console')
        assert 'Never' in first.inner_text() and 'Failed' in first.inner_text()
        behind = section.locator('tbody tr', has_text="'clean'").first
        assert 'Caught up' not in behind.inner_text()
        ### Phone: each row is a card, its pipe first and every value labeled.
        if width < 768:
            assert page.evaluate("""() => {
                const cell = document.querySelector('#performance-engine tbody tr td');
                return getComputedStyle(cell.parentElement).display === 'grid'
                    && getComputedStyle(document.querySelector('#performance-engine thead')).position === 'absolute';
            }""")
        page.locator('#performance-engine').screenshot(
            path=str(artifacts() / f'performance-engine-freshness-{width}.png'))

        ### Backlog: the child behind its parent.
        page.get_by_role('tab', name='Backlog', exact=True).click()
        backlog = section.locator('[id*="engine-backlog"] tbody tr')
        backlog.first.wait_for()
        assert backlog.count() == 1
        text = backlog.first.inner_text()
        assert "'clean'" in text and "'raw'" in text, text
        page.locator('#performance-engine').screenshot(
            path=str(artifacts() / f'performance-engine-backlog-{width}.png'))

        ### Nodes: the failed node with the planner's reason.
        page.get_by_role('tab', name='Nodes', exact=True).click()
        nodes = section.locator('[id*="engine-nodes"] tbody tr')
        nodes.first.wait_for()
        assert nodes.count() == 1
        text = nodes.first.inner_text()
        assert "'broken'" in text and 'Failed' in text and 'no parents in this run' in text, text
        page.locator('#performance-engine').screenshot(
            path=str(artifacts() / f'performance-engine-nodes-{width}.png'))

        ### Runs: the saved Pipeline's run links to it on the Pipeline's page.
        page.get_by_role('tab', name='Runs', exact=True).click()
        runs = section.locator('[id*="engine-recent"] tbody tr')
        runs.first.wait_for()
        assert runs.count() == 3
        link = section.locator('[id*="engine-recent"] a[href^="/dash/pipelines/engine-demo?run_id=engine-demo-"]')
        assert link.count() == 1
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        page.screenshot(path=str(artifacts() / f'performance-engine-page-{width}.png'), full_page=True)

        ### Refresh updates the section in place: the tab, the charts and the scroll stay.
        mounted = ['#performance-engine-tabs', '#performance-engine-runs .js-plotly-plot',
                   '#performance-engine .performance-engine-stats']
        page.evaluate("""selectors => {
            for (const selector of selectors) document.querySelector(selector).dataset.engine = 'mounted';
        }""", mounted)
        ### `instant`: Bootstrap makes the page scroll smoothly, and a position read during that
        ### animation is not where the page comes to rest.
        page.evaluate(SCROLL_TO_BOTTOM)
        before = page.evaluate('Math.round(window.scrollY)')
        assert page.evaluate("getComputedStyle(document.documentElement).scrollBehavior") == 'smooth'
        page.evaluate("""() => {
            window.engineShortest = document.documentElement.scrollHeight;
            new MutationObserver(() => {
                window.engineShortest = Math.min(window.engineShortest, document.documentElement.scrollHeight);
            }).observe(document.getElementById('performance-page'), {childList: true, subtree: true, characterData: true});
        }""")
        page.evaluate("document.getElementById('performance-refresh').click()")
        page.wait_for_function("document.getElementById('performance-refresh').disabled")
        page.wait_for_function("!document.getElementById('performance-refresh').disabled")
        assert page.evaluate('window.engineShortest') >= before + page.evaluate('innerHeight') - 1
        assert abs(page.evaluate('Math.round(window.scrollY)') - before) <= 1
        kept = page.evaluate("[...document.querySelectorAll('[data-engine]')].map(e => e.dataset.engine)")
        assert kept == ['mounted'] * 3, kept
        assert 'active' in page.get_by_role('tab', name='Runs', exact=True).get_attribute('class')
        assert not tab.errors, tab.errors
    finally:
        context.close()
