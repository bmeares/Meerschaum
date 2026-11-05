"""
RC28 console review: the Performance page refreshes in place (no jump to the top, the open tab and
its page kept), and the Connectors page groups its connectors by type and names a plugin once.
Opt-in (`MRSM_TEST_CONSOLE=1`); see `tests/console/conftest.py`.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from tests.console.conftest import Server, Tab
from tests.console.test_console_shell import SMALL_TARGETS

pytestmark = pytest.mark.console
SECRET = 'rc28-password-never-show'
PLUGINS = {
    'rc28_weather': 'Synthetic weather readings.',
    'rc28_tides': 'Synthetic tide gauges.',
}

SYNCS = '''
import meerschaum as mrsm
from meerschaum import _core
c = mrsm.get_connector('sql:w7console')
for index in range(%(start)d, %(stop)d):
    p = mrsm.Pipe('rc28', f'perf_{index:02d}', instance=c, columns={'primary': 'id'})
    assert p.sync([{'id': i} for i in range(index + 1)])[0]
_core.flush_sync_statuses(timeout=10.0)
'''


def artifacts() -> pathlib.Path:
    path = pathlib.Path.home() / '.cache' / 'mrsm-rc28-ui' / 'screenshots'
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture(scope='module')
def rc28_server():
    config = {'meerschaum': {'connectors': {
        'sql': {'rc28_reports': {
            'flavor': 'postgresql', 'host': 'db.invalid', 'database': 'warehouse',
            'username': 'reader', 'password': SECRET,
        }},
        'api': {'rc28_remote': {'protocol': 'https', 'host': 'api.invalid', 'port': 8443}},
        'valkey': {'rc28_cache': {'host': 'cache.invalid', 'port': 6379}},
        'rc28custom': {'main': {'host': 'custom.invalid'}},
    }}}
    server = Server('rc28', config=config, native=False)
    for name, doc in PLUGINS.items():
        (server.plugins / f'{name}.py').write_text(
            f'"""{doc}"""\n\ndef fetch(pipe, **kwargs):\n    return [{{"value": 28}}]\n'
        )
    try:
        server.start()
        result = server.python(SYNCS % {'start': 0, 'stop': 40})
        assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
        yield server
    finally:
        server.stop()


#: Scroll to the bottom at once, not with the page's smooth scrolling.
SCROLL_TO_BOTTOM = "window.scrollTo({top: document.documentElement.scrollHeight, behavior: 'instant'})"


def _syncs(page) -> str:
    return page.evaluate("""() => {
        const label = [...document.querySelectorAll('#performance-page .performance-stat dt')]
            .find(dt => dt.textContent === 'Syncs');
        return label ? label.nextElementSibling.textContent : null;
    }""")


def _wait_for_refresh(page, trigger, timeout=30000):
    """Wait for the server's answer to one Performance refresh and for the page to show it."""
    def answered(response):
        if '_dash-update-component' not in response.url:
            return False
        try:
            body = json.loads(response.request.post_data or '{}')
        except ValueError:
            return False
        return any(trigger in prop for prop in body.get('changedPropIds', []))
    return page.expect_response(answered, timeout=timeout)


@pytest.mark.parametrize('width', [1440, 390])
def test_performance_refresh_keeps_scroll_tab_and_components(rc28_server, browser, width):
    context = browser.new_context(viewport={'width': width, 'height': 800}, has_touch=width < 768)
    try:
        tab = Tab(context.new_page(), rc28_server.base_url)
        tab.login(path='/dash')
        tab.goto('/dash/performance', '#performance-page')
        page = tab.page
        page.locator('#performance-pipes tbody tr').first.wait_for()
        page.wait_for_function("!document.getElementById('performance-refresh').disabled")
        ### The Pipes breakdown's second page, scrolled down to it.
        pipes = page.locator('#performance-pipes')
        pipes.locator('.page-item:nth-last-child(2) .page-link').click()
        page.wait_for_function(
            "parseInt(document.querySelector('#performance-pipes .page-item.active')?.textContent) === 2"
        )
        ### `instant`: Bootstrap scrolls the page smoothly (`scroll-behavior: smooth`), and a
        ### position read during that animation is short of where the page comes to rest.
        page.evaluate(SCROLL_TO_BOTTOM)
        before = page.evaluate('Math.round(window.scrollY)')
        assert before > 150, before
        ### A refresh updates the mounted components: nothing below the filters is rebuilt.
        mounted = ['#performance-results .js-plotly-plot', '#performance-detail-tabs',
                   '#performance-pipes .performance-group-table-wrap',
                   '#performance-pipes .performance-group-table tbody']
        # Charts mount after the tables (later still on a phone): wait for all of them first.
        page.wait_for_function('selectors => selectors.every(s => document.querySelector(s))',
                               arg=mounted)
        page.evaluate("""selectors => {
            for (const selector of selectors) {
                document.querySelector(selector).dataset.rc28 = 'mounted';
            }
        }""", mounted)
        rows = pipes.locator('tbody tr').count()
        ### The shortest the page gets while it refreshes (measured at every change, which also
        ### lays it out there): a page that collapses, even for a moment, loses its scroll position.
        page.evaluate("""() => {
            window.rc28Shortest = document.documentElement.scrollHeight;
            new MutationObserver(() => {
                window.rc28Shortest = Math.min(window.rc28Shortest, document.documentElement.scrollHeight);
            }).observe(document.getElementById('performance-page'),
                       {childList: true, subtree: true, characterData: true});
        }""")
        syncs = _syncs(page)
        result = rc28_server.python(SYNCS % {'start': 40, 'stop': 43})
        assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]

        def unchanged(name):
            shortest = page.evaluate('window.rc28Shortest')
            assert shortest >= before + page.evaluate('innerHeight') - 1, (shortest, before)
            assert abs(page.evaluate('Math.round(window.scrollY)') - before) <= 1, (
                page.evaluate('Math.round(window.scrollY)'), before)
            assert 'active' in page.get_by_role('tab', name=name, exact=True).get_attribute('class')
            kept = page.evaluate("[...document.querySelectorAll('[data-rc28]')].map(e => e.dataset.rc28)")
            assert kept == ['mounted'] * 4, kept

        ### The 30 s poll, shortened for the test; it brings 3 more syncs.
        with _wait_for_refresh(page, 'performance-interval.n_intervals'):
            page.evaluate("dash_clientside.set_props('performance-interval', {interval: 1000})")
        page.evaluate("dash_clientside.set_props('performance-interval', {interval: 600000})")
        page.wait_for_function(f"""() => {{
            const label = [...document.querySelectorAll('#performance-page .performance-stat dt')]
                .find(dt => dt.textContent === 'Syncs');
            return label && label.nextElementSibling.textContent !== {json.dumps(syncs)};
        }}""")
        page.wait_for_function("!document.getElementById('performance-refresh').disabled")
        page.wait_for_function(
            "document.querySelector('#performance-pipes .performance-group-tools + p')?.textContent.includes('of 43')"
        )
        unchanged('Pipes')
        assert page.evaluate("parseInt(document.querySelector('#performance-pipes .page-item.active').textContent)") == 2
        assert pipes.locator('tbody tr').count() == rows
        page.screenshot(path=str(artifacts() / f'performance-pipes-after-refresh-{width}.png'))

        ### Recent syncs on its second page of 25: Refresh keeps it (clicked without scrolling up).
        page.get_by_role('tab', name='Recent syncs', exact=True).click()
        page.locator('#performance-history-controls').wait_for(state='visible')
        page.select_option('#performance-history-page-size', '25')
        page.wait_for_function("document.querySelectorAll('#performance-history tbody tr').length === 25")
        page.locator('#performance-history-controls .page-item:nth-last-child(2) .page-link').click()
        page.wait_for_function(
            "document.querySelector('#performance-history-range')?.textContent.startsWith('26')"
        )
        page.wait_for_function("!document.getElementById('performance-refresh').disabled")
        page.evaluate(SCROLL_TO_BOTTOM)
        before = page.evaluate('Math.round(window.scrollY)')
        assert before > 150, before
        page.evaluate('window.rc28Shortest = document.documentElement.scrollHeight')
        with _wait_for_refresh(page, 'performance-refresh.n_clicks'):
            page.evaluate("document.getElementById('performance-refresh').click()")
        page.wait_for_function("!document.getElementById('performance-refresh').disabled")
        unchanged('Recent syncs')
        assert page.inner_text('#performance-history-range').startswith('26')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        page.screenshot(path=str(artifacts() / f'performance-history-after-refresh-{width}.png'))

        ### A new selection still rebuilds the views for it.
        page.select_option('#performance-window', '7d')
        page.wait_for_function(
            "document.querySelector('#performance-pipes tbody a')?.getAttribute('href').includes('window=7d')"
        )
        page.wait_for_function("!document.getElementById('performance-refresh').disabled")
        assert 'all recorded syncs' in page.inner_text('#performance-coverage')
        assert not tab.errors, tab.errors
    finally:
        context.close()


#: No transition (a modal fading in or out) is running.
SETTLED = "document.getAnimations().every(animation => animation.playState !== 'running')"


def _groups(page):
    return page.evaluate("""() => [...document.querySelectorAll('#connectors-table tbody.connectors-group')]
        .filter(group => !group.hidden)
        .map(group => [group.dataset.type,
                       [...group.querySelectorAll('tr[data-keys]')].filter(row => !row.hidden)
                           .map(row => row.dataset.keys)])""")


@pytest.mark.parametrize('width', [1440, 390])
def test_connectors_are_grouped_by_type_and_name_a_plugin_once(rc28_server, browser, width):
    context = browser.new_context(viewport={'width': width, 'height': 900}, has_touch=width < 768)
    try:
        tab = Tab(context.new_page(), rc28_server.base_url)
        tab.login(path='/dash')
        tab.goto('/dash/connectors', '#connectors-table')
        page = tab.page
        groups = _groups(page)
        types = [typ for typ, _ in groups]
        assert types[:3] == ['sql', 'api', 'valkey'] and types[-1] == 'plugin', groups
        assert 'rc28custom' in types, groups
        assert len(types) == len(set(types)), groups
        for typ, keys in groups:
            assert keys and all(key.startswith(typ + ':') for key in keys), (typ, keys)
            heading = page.locator(f'#connectors-table tbody[data-type="{typ}"] .connectors-group-heading')
            assert heading.is_visible(), typ
            assert heading.inner_text().split()[-1] == str(len(keys)), heading.inner_text()
        assert {'plugin:rc28_weather', 'plugin:rc28_tides'} <= set(dict(groups)['plugin'])

        ### The keys already say `plugin:<name>`: the Source column only links to the plugin.
        row = page.locator('#connectors-table tr[data-keys="plugin:rc28_weather"]')
        assert row.inner_text().count('rc28_weather') == 1, row.inner_text()
        source = row.locator('td.connectors-col-source')
        link = source.get_by_role('link', name='View source')
        assert link.get_attribute('href') == '/dash/plugins/installed/rc28_weather'
        ### RC31: the link is its icon; "View source" is only its accessible name and tooltip.
        assert link.get_attribute('title') == 'View source'
        assert link.locator('.fa-code').is_visible()
        label_box = link.locator('.visually-hidden').bounding_box()
        assert label_box is None or label_box['width'] <= 1, label_box

        ### The modal names the connector once (its title) and links to the plugin's page once.
        row.get_by_role('button', name='plugin:rc28_weather', exact=True).click()
        modal = page.locator('.connector-info-modal.show')
        modal.wait_for()
        modal.get_by_role('link', name='View source').wait_for()
        text = modal.inner_text()
        assert text.count('rc28_weather') == 1, text
        assert modal.get_by_role('link', name='View source').count() == 1
        assert 'Connection settings' not in text and '{}' not in text, text
        page.wait_for_function(SETTLED)
        page.screenshot(path=str(artifacts() / f'connector-plugin-modal-{width}.png'))
        modal.locator('.modal-header .btn-close').click()
        page.locator('.connector-info-modal').wait_for(state='detached')

        ### A SQL connector's modal shows each setting once, the password masked.
        page.locator('#connectors-table tr[data-keys="sql:rc28_reports"]').get_by_role(
            'button', name='sql:rc28_reports', exact=True).click()
        modal.wait_for()
        modal.get_by_text('db.invalid').first.wait_for()
        text = modal.inner_text()
        for value in ('db.invalid', 'warehouse', 'reader', 'postgresql'):
            assert text.count(value) == 1, (value, text)
        assert '5432' in text and '••••••' in text and SECRET not in page.content()
        assert modal.evaluate('element => element.scrollWidth <= element.clientWidth + 1')
        if width < 768:
            assert page.evaluate(SMALL_TARGETS, '.connector-info-modal.show') == []
        page.wait_for_function(SETTLED)
        page.screenshot(path=str(artifacts() / f'connector-sql-modal-{width}.png'))
        modal.locator('.modal-header .btn-close').click()
        page.locator('.connector-info-modal').wait_for(state='detached')
        page.wait_for_function(SETTLED)

        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        if width < 768:
            assert page.evaluate(SMALL_TARGETS, '#connectors-page') == []
            assert row.evaluate('e => getComputedStyle(e).display') == 'block'
        page.screenshot(path=str(artifacts() / f'connectors-{width}.png'), full_page=True)

        ### The filter hides the groups without a match.
        page.fill('#connectors-filter', 'rc28_tides')
        page.wait_for_function("""() => [...document.querySelectorAll('#connectors-table tbody.connectors-group')]
            .filter(group => !group.hidden).length === 1""")
        assert _groups(page) == [['plugin', ['plugin:rc28_tides']]]
        page.fill('#connectors-filter', '')
        page.wait_for_function("""() => [...document.querySelectorAll('#connectors-table tbody.connectors-group')]
            .every(group => !group.hidden)""")

        link.click()
        page.wait_for_url('**/dash/plugins/installed/rc28_weather')
        page.get_by_text(PLUGINS['rc28_weather']).first.wait_for()
        assert not tab.errors, tab.errors
    finally:
        context.close()
