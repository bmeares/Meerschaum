"""Pipelines pages on a phone (dark dropdowns) and lazily loaded Runs that never flash the page."""
import pytest
from tests.console.test_console import SETUP_PIPES, _wait_text, _errors

pytestmark = pytest.mark.console

PARENT = 'sql:w7console|sql:w7console|w7parent|None'
CHILD = 'sql:w7console|sql:w7console|w7child|None'
WHITE = 'rgb(255, 255, 255)'

#: The background of every dropdown on the page: its trigger, open menu and their wrappers.
DROPDOWN_BACKGROUNDS = '''() => {
    const nodes = [];
    for (const root of document.querySelectorAll('.dash-dropdown, .dash-dropdown-content')) {
        nodes.push(root, ...root.querySelectorAll('.dash-dropdown-trigger, .dash-dropdown-search-container,'
            + ' .dash-dropdown-options, .dash-options-list, .dash-dropdown-search'));
    }
    return nodes.filter(node => node.getBoundingClientRect().height > 0).map(node => [
        (node.className || '').toString().slice(0, 60), getComputedStyle(node).backgroundColor]);
}'''


def _save(*names):
    code = SETUP_PIPES + "\nfrom meerschaum.core.Pipeline._saved import save_pipeline\n"
    for name, targets in names:
        code += f"save_pipeline({name!r}, {targets!r}, 'sql:w7console')\n"
    return code


def _assert_dark(page, where):
    backgrounds = page.evaluate(DROPDOWN_BACKGROUNDS)
    assert backgrounds, where
    light = [entry for entry in backgrounds if entry[1] == WHITE]
    assert not light, (where, light)


def test_pipelines_dropdowns_are_dark_on_a_phone(server, browser):
    from tests.console.conftest import Tab
    setup = server.python(_save(('phone-test', [PARENT, CHILD])))
    assert setup.returncode == 0, setup.stderr[-2000:]
    context = browser.new_context(viewport={'width': 390, 'height': 844}, has_touch=True, is_mobile=True)
    try:
        tab = Tab(context.new_page(), server.base_url)
        tab.login()
        page = tab.page
        tab.goto('/dash/pipelines', '#pipelines-list')
        page.locator('.pipeline-list-entry').first.wait_for(timeout=60000)
        _assert_dark(page, 'list, closed')
        page.locator('#pipelines-find-pipe').tap()
        page.locator('.dash-dropdown-content').wait_for()
        _assert_dark(page, 'Find Pipe, open')
        page.screenshot(path=str(_artifacts() / 'find-pipe-390.png'))
        page.keyboard.press('Escape')
        tab.goto('/dash/pipelines/phone-test', '#pipelines-page')
        page.wait_for_function("document.getElementById('pipelines-page')?.dataset.loaded === 'true'", timeout=60000)
        page.wait_for_selector('.pipeline-stage', timeout=60000)
        page.locator('.pipeline-node-output-icon').first.tap()
        page.locator('#pipeline-output-tab').tap()
        page.locator('.dash-dropdown-content').wait_for()
        _assert_dark(page, 'Pipe output, open')
        page.screenshot(path=str(_artifacts() / 'pipe-output-390.png'))
        page.keyboard.press('Escape')
        page.locator('#pipeline-output-close').tap()
        page.locator('#pipeline-view-runs').tap()
        page.locator('#pipeline-log-stage').tap()
        page.locator('.dash-dropdown-content').wait_for()
        _assert_dark(page, 'Runs output, open')
        tab.goto('/dash/pipelines/new', '#pipeline-new-page')
        page.wait_for_selector('#pipeline-new-stepper li[data-state=current]')
        page.locator('#pipeline-new-name').fill('phone-new')
        page.locator('#pipeline-new-next').tap()
        page.wait_for_function("document.getElementById('pipeline-new-page').dataset.step === 'members'")
        page.locator('#pipeline-new-draft-members').tap()
        page.locator('.dash-dropdown-content').wait_for()
        _assert_dark(page, 'New pipeline members, open')
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()


def _artifacts():
    from pathlib import Path
    path = Path.home() / '.cache' / 'mrsm-rc15-review'
    path.mkdir(parents=True, exist_ok=True)
    return path


#: Tag nodes, then sample every frame whether they stay mounted, visible and in place.
WATCH = '''(selectors) => {
    window._mrsmWatch = {samples: 0, problems: []};
    const watched = selectors.map(selector => {
        const node = document.querySelector(selector);
        node._mrsmTag = selector;
        return {selector, node, top: node.getBoundingClientRect().top};
    });
    const sample = () => {
        if (!window._mrsmWatch) return;
        window._mrsmWatch.samples += 1;
        for (const item of watched) {
            const current = document.querySelector(item.selector);
            const style = current && getComputedStyle(current);
            if (current !== item.node) window._mrsmWatch.problems.push(item.selector + ' replaced');
            else if (style.visibility === 'hidden' || style.display === 'none' || Number(style.opacity) < 1)
                window._mrsmWatch.problems.push(item.selector + ' hidden');
            else if (Math.abs(current.getBoundingClientRect().top - item.top) > 1)
                window._mrsmWatch.problems.push(item.selector + ' moved');
        }
        requestAnimationFrame(sample);
    };
    requestAnimationFrame(sample);
}'''


def test_expanding_runs_loads_in_place_without_flashing(server, tab):
    setup = server.python(_save(('lazy-a', [PARENT]), ('lazy-b', [CHILD]), ('lazy-c', [PARENT, CHILD])))
    assert setup.returncode == 0, setup.stderr[-2000:]
    for name in ('lazy-a', 'lazy-b', 'lazy-c'):
        sync = server.mrsm('sync', 'pipelines', name, '-i', 'sql:w7console', '--nopretty')
        assert sync.returncode == 0, sync.stderr[-2000:]
    tab.login()
    page = tab.page
    page.set_viewport_size({'width': 1280, 'height': 1000})
    tab.goto('/dash/pipelines', '#pipelines-list')
    page.locator('.pipeline-list-entry[data-pipeline=lazy-c]').wait_for(timeout=60000)
    ### Expand the last card: the cards above it, the header and the list stay mounted, visible
    ### and in place, and the Runs swap into the card's placeholder.
    page.evaluate(WATCH, ['.pipelines-list-header', '#pipelines-list',
                          '.pipeline-list-entry[data-pipeline=lazy-a]', '.pipeline-list-entry[data-pipeline=lazy-b]'])
    card = page.locator('.pipeline-list-entry[data-pipeline=lazy-c]')
    card.get_by_role('button', name='Details for lazy-c', exact=True).click()
    card.locator('.pipeline-run-link').first.wait_for(timeout=30000)
    page.wait_for_timeout(500)
    watch = page.evaluate('window._mrsmWatch')
    page.evaluate('window._mrsmWatch = null')
    assert watch['samples'] > 5 and watch['problems'] == [], watch
    assert card.locator('.pipeline-runs-placeholder').count() == 0
    ### The detail page's Runs tab: switching to it and polling never replace the page or its views.
    tab.goto('/dash/pipelines/lazy-c', '#pipelines-page')
    page.wait_for_function("document.getElementById('pipelines-page')?.dataset.loaded === 'true'", timeout=60000)
    page.wait_for_selector('.pipeline-stage', timeout=60000)
    page.locator('#pipeline-view-runs').click()
    page.wait_for_selector('#pipelines-runs-table', timeout=30000)
    page.wait_for_selector('#pipelines-gantt .barlayer .point', timeout=30000)
    page.evaluate("document.getElementById('pipeline-canvas-world')._mrsmTag = 'canvas'")
    page.evaluate(WATCH, ['.pipeline-detail-header', '#pipelines-runs-table', '#pipelines-gantt'])
    for _ in range(2):
        page.evaluate("window.dash_clientside.set_props('pipelines-interval', {n_intervals: Date.now()})")
        page.wait_for_timeout(1500)
    watch = page.evaluate('window._mrsmWatch')
    page.evaluate('window._mrsmWatch = null')
    assert watch['samples'] > 5 and watch['problems'] == [], watch
    assert page.evaluate("document.getElementById('pipeline-canvas-world')._mrsmTag") == 'canvas'
    assert _errors(tab) == [], _errors(tab)
