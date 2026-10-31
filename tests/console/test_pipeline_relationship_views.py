"""Pipeline context, compact history, and execution view switching in real browsers."""
import pytest

from tests.console.test_console import SETUP_PIPES, _errors, _open_lineage
from tests.console.test_pipelines_mobile_and_lazy_runs import PARENT, CHILD

pytestmark = pytest.mark.console


@pytest.mark.parametrize('width', [390, 1440])
def test_related_pipes_are_added_only_after_edit_and_save(server, browser, width):
    from tests.console.conftest import Tab
    name = f'related-{width}'
    setup = server.python(SETUP_PIPES + f'''
from meerschaum.core.Pipeline._saved import save_pipeline
assert save_pipeline({name!r}, [{CHILD!r}], 'sql:w7console')['targets'] == [{CHILD!r}]
assert save_pipeline('source-owner', [{PARENT!r}], 'sql:w7console')['targets'] == [{PARENT!r}]
''')
    assert setup.returncode == 0, setup.stderr[-2000:]
    context = browser.new_context(viewport={'width': width, 'height': 900}, has_touch=width < 600)
    try:
        tab = Tab(context.new_page(), server.base_url)
        tab.login()
        page = tab.page
        _open_lineage(tab, '/dash/pipes/sql:w7console/w7child')
        explore = page.locator('#pipe-detail-content a:has-text("Explore graph")').get_attribute('href')
        tab.goto(explore, '#pipeline-explore-page')
        for ref in (PARENT, CHILD):
            page.locator(f'#pipeline-explore-graph .pipeline-stage[data-ref="{ref}"]').wait_for()
        tab.goto('/dash/pipelines/' + name, '#pipelines-page')
        page.wait_for_selector('#pipeline-canvas-world[data-mounted=true]')
        parent = page.locator(f'.pipeline-stage[data-ref="{PARENT}"]')
        assert parent.count() == 0
        page.locator('.pipeline-view-options summary').click()
        page.locator('#pipeline-show-related').check()
        parent.wait_for()
        page.wait_for_function('(ref) => document.querySelector(`[data-ref="${ref}"]`)?.dataset.context === "true"', arg=PARENT)
        assert 'source-owner' in parent.locator('.pipeline-shared-badge').get_attribute('title')
        assert float(parent.evaluate('node => getComputedStyle(node).opacity')) < 1
        before = server.python(f'''
import meerschaum as mrsm
from meerschaum.core.Pipeline._saved import list_pipelines
assert list_pipelines(mrsm.get_connector('sql','w7console'))[{name!r}]['targets'] == [{CHILD!r}]
''')
        assert before.returncode == 0, before.stderr[-2000:]
        page.locator('#pipeline-edit-toggle').click()
        page.wait_for_function('document.getElementById("pipelines-page").dataset.edit === "true"')
        parent.click()
        page.locator('#pipeline-add-related').click()
        page.locator('#pipeline-inspector-close').click()
        page.wait_for_function('(ref) => document.querySelector(`[data-ref="${ref}"]`)?.dataset.context === "false"', arg=PARENT)
        page.locator('#pipeline-draft-save').click()
        page.wait_for_function('!document.getElementById("pipeline-draft-notice").textContent.trim()')
        after = server.python(f'''
import meerschaum as mrsm
from meerschaum.core.Pipeline._saved import list_pipelines
assert set(list_pipelines(mrsm.get_connector('sql','w7console'))[{name!r}]['targets']) == {{{CHILD!r},{PARENT!r}}}
''')
        assert after.returncode == 0, after.stderr[-2000:]
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()


@pytest.mark.parametrize('width', [390, 1440])
def test_runs_use_one_lane_and_switch_between_timeline_and_table(server, browser, width):
    from tests.console.conftest import Tab
    name = f'execution-views-{width}'
    setup = server.python(SETUP_PIPES + f'''
import meerschaum as mrsm
from meerschaum.core.Pipeline._saved import save_pipeline
assert set(save_pipeline({name!r}, [{PARENT!r}, {CHILD!r}], 'sql:w7console')['targets']) == {{{PARENT!r}, {CHILD!r}}}
for _ in range(3):
    result = mrsm.Pipeline({name!r},instance='sql:w7console').sync()
    assert result.success, str(result)
''')
    assert setup.returncode == 0, setup.stderr[-2000:]
    context = browser.new_context(viewport={'width': width, 'height': 900})
    try:
        tab = Tab(context.new_page(), server.base_url)
        tab.login()
        tab.goto('/dash/pipelines/' + name, '#pipelines-page')
        page = tab.page
        page.locator('#pipeline-view-runs').click()
        overview = page.locator('#pipeline-runs-overview .js-plotly-plot')
        overview.wait_for()
        assert overview.evaluate('plot => new Set(plot.data[0].y).size') == 1
        assert overview.evaluate('plot => plot.data[0].customdata.length') == 3
        timeline = page.locator('.pipeline-run-detail .pipeline-timeline-scroll')
        table = page.locator('#pipelines-nodes-table')
        assert timeline.is_visible() and not table.is_visible()
        page.locator('.pipeline-run-detail button[data-run-view=table]').click()
        assert table.is_visible() and not timeline.is_visible()
        assert 'Action' not in table.locator('th').all_text_contents()
        if width < 600:
            assert table.locator('tbody tr').first.locator('td').nth(1).evaluate('cell => cell.getBoundingClientRect().right <= innerWidth')
        page.locator('.pipeline-run-detail button[data-run-view=timeline]').click()
        assert timeline.is_visible() and not table.is_visible()
        page.locator('.pipeline-run-detail button[data-run-view=table]').click()
        refresh = server.mrsm('sync', 'pipelines', name, '-i', 'sql:w7console', '--nopretty')
        assert refresh.returncode == 0, refresh.stderr[-2000:]
        page.evaluate("window.dash_clientside.set_props('pipelines-interval',{n_intervals:Date.now()})")
        page.wait_for_function("document.querySelector('#pipeline-runs-overview .js-plotly-plot')?.data[0].customdata.length === 4")
        assert table.is_visible() and not timeline.is_visible()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        tab.goto('/dash/pipes', 'tr.pipes-table-row')
        link = page.locator('.pipes-col-pipe a').first
        link.wait_for()
        link.hover()
        assert link.evaluate('node => getComputedStyle(node).textDecorationLine') == 'none'
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()
