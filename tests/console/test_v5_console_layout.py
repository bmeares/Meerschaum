"""Final v5 runbook, full-width pages, fixed canvas tools and real mobile gestures."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.console.conftest import Server, Tab
from tests.console.test_console import SETUP_PIPES

pytestmark = pytest.mark.console
PARENT = 'sql:w7console|sql:w7console|w7parent|None'
CHILD = 'sql:w7console|sql:w7console|w7child|None'
VIEWER_PASSWORD = 'final-v5-viewer-password'


def artifacts():
    path = Path.home() / '.cache' / 'mrsm-v5-console-ui' / 'browser'
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture(scope='module')
def layout_server():
    server = Server('final-v5-layout', native=False)
    try:
        server.start()
        setup = server.python(SETUP_PIPES + f'''
from meerschaum.core import User
from meerschaum.core.Pipeline._saved import save_pipeline
assert conn.register_user(User('v5-viewer', {VIEWER_PASSWORD!r}, instance=conn,
    attributes={{'role':'viewer'}}))[0]
private = mrsm.Pipe('layout', 'PRIVATE_MEMBER_V5', instance=conn,
    parameters={{'access':{{'visibility':'private','owner':'alice'}}}})
assert private.register()[0]
private_ref = 'sql:w7console|layout|PRIVATE_MEMBER_V5|None'
for name, refs in [('alpha', [{PARENT!r}, {CHILD!r}]), ('zebra', [{PARENT!r}]),
                   ('partially-visible', [{PARENT!r}, private_ref])]:
    assert save_pipeline(name, refs, conn, access={{'visibility':'public','owner':'alice'}})['name'] == name
assert save_pipeline('PRIVATE_PIPELINE_V5', [private_ref], conn,
    access={{'visibility':'private','owner':'alice'}})['name'] == 'PRIVATE_PIPELINE_V5'
assert mrsm.Pipeline('alpha', instance=conn).sync().success
''')
        assert setup.returncode == 0, setup.stdout[-2000:] + setup.stderr[-4000:]
        yield server
    finally:
        if server.log_path.exists():
            (artifacts() / 'server.log').write_text(server.log(), encoding='utf-8')
        server.stop()
        assert not server.leaked_ports, server.leaked_ports


def open_canvas(tab):
    tab.goto('/dash/pipelines/alpha', '#pipelines-page')
    tab.page.wait_for_function("document.getElementById('pipeline-canvas-world')?.dataset.mounted === 'true'",
                               timeout=60000)


@pytest.mark.parametrize('width', [375, 768, 1440, 1920, 2560])
def test_full_width_pages_and_view_options_never_move(layout_server, browser, width):
    context = browser.new_context(viewport={'width': width, 'height': 1100})
    try:
        tab = Tab(context.new_page(), layout_server.base_url)
        tab.login(path='/dash')
        page = tab.page
        geometry = {'width': width}
        for route, selector in [('/dash/pipes', '#pipes-page'),
                                ('/dash/pipelines', '#pipelines-list-page')]:
            tab.goto(route, selector)
            box = page.locator(selector).bounding_box()
            assert abs(box['width'] - width) <= 1, (route, box)
            geometry[route] = box
            if route.endswith('pipelines'):
                table = page.locator('.pipelines-runbook')
                table.wait_for()
                table_box = table.bounding_box()
                padding = page.locator(selector).evaluate(
                    'n => parseFloat(getComputedStyle(n).paddingLeft) + parseFloat(getComputedStyle(n).paddingRight)')
                assert abs(table_box['width'] - (box['width'] - padding)) <= 2, table_box
                geometry['runbook'] = table_box
                page.screenshot(path=str(artifacts() / f'runbook-admin-{width}.png'), full_page=True)
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        open_canvas(tab)
        measure = '''() => Object.fromEntries([
            '.pipeline-canvas-toolbar', '.pipeline-canvas-controls', '#pipeline-canvas-search',
            '.pipeline-view-options > summary', '#pipeline-canvas'
        ].map(selector => {const r=document.querySelector(selector).getBoundingClientRect();
            return [selector, {x:r.x,y:r.y,width:r.width,height:r.height}];}))'''
        for edit in (False, True):
            if edit:
                page.locator('#pipeline-edit-toggle').click()
                page.wait_for_function("document.getElementById('pipelines-page').dataset.edit === 'true'")
            before = page.evaluate(measure)
            trigger = page.locator('.pipeline-view-options > summary')
            trigger.click()
            panel = page.locator('#pipeline-view-options-panel')
            panel.wait_for()
            after = page.evaluate(measure)
            for selector, box in before.items():
                assert all(abs(after[selector][key] - value) <= 1 for key, value in box.items()), (selector, before, after)
            bounds = panel.bounding_box()
            assert bounds['x'] >= 0 and bounds['x'] + bounds['width'] <= width, bounds
            assert panel.evaluate('n => n.scrollWidth <= n.clientWidth + 1')
            if width < 1000:
                assert trigger.bounding_box()['height'] >= 44
                for button in panel.locator('button').all():
                    assert button.bounding_box()['height'] >= 44
            geometry['edit' if edit else 'view'] = {'toolbar': before, 'panel': bounds}
            page.screenshot(path=str(artifacts() / f'options-{width}-edit-{edit}.png'), full_page=True)
            page.keyboard.press('Escape')
            assert not panel.is_visible()
            assert trigger.evaluate('n => n === document.activeElement')
            trigger.press('Enter')
            panel.wait_for()
            page.locator('#pipeline-canvas-search').click()
            assert not panel.is_visible()
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        (artifacts() / f'geometry-{width}.json').write_text(json.dumps(geometry, indent=2) + '\n')
        assert not tab.errors, tab.errors
    finally:
        context.close()


@pytest.mark.parametrize('width', [390, 1440])
def test_runbook_search_sort_details_navigation_and_viewer_acl(layout_server, browser, width):
    context = browser.new_context(viewport={'width': width, 'height': 1000})
    try:
        tab = Tab(context.new_page(), layout_server.base_url)
        tab.login('v5-viewer', VIEWER_PASSWORD, path='/dash')
        page = tab.page
        tab.goto('/dash/pipelines?instance=sql%3Aw7console', '.pipelines-runbook')
        rows = page.locator('.pipeline-list-entry')
        assert rows.count() == 3
        assert 'PRIVATE_MEMBER_V5' not in page.content()
        assert 'PRIVATE_PIPELINE_V5' not in page.content()
        assert page.get_by_role('table', name='Pipelines runbook').count() == 1
        assert page.locator('[id*="pipelines-list-sync"]').count() == 0
        assert rows.get_by_text('Manage', exact=True).count() == 0
        assert page.locator('.pipeline-list-entry .pipeline-run-link').count() == 0
        requests = []
        page.on('request', lambda request: requests.append(request.url) if request.method == 'POST' else None)
        page.locator('#pipelines-search').fill('w7child')
        page.wait_for_function("document.getElementById('pipelines-visible-count').textContent === '1 of 3 Pipelines'")
        assert page.locator('.pipeline-list-entry:visible').get_attribute('data-pipeline') == 'alpha'
        page.locator('#pipelines-search').fill('no such pipeline')
        page.locator('#pipelines-no-matches').wait_for()
        assert page.locator('.pipeline-list-entry:visible').count() == 0
        page.locator('#pipelines-search').fill('')
        page.locator('#pipelines-sort').select_option('name-desc')
        page.wait_for_function("document.querySelector('.pipeline-list-entry').dataset.pipeline === 'zebra'")
        page.locator('#pipelines-sort').select_option('members')
        page.wait_for_function("document.querySelector('.pipeline-list-entry').dataset.pipeline === 'alpha'")
        page.locator('#pipelines-sort').select_option('recent')
        assert page.locator('.pipeline-list-entry').first.get_attribute('data-pipeline') == 'alpha'
        assert not requests, requests  # Search and sort do not issue host / SDK reads.
        entry = page.locator('.pipeline-list-entry[data-pipeline=alpha]')
        toggle = entry.get_by_role('button', name='Details for alpha', exact=True)
        toggle.click()
        entry.locator('.pipeline-run-link').first.wait_for(timeout=30000)
        assert toggle.get_attribute('aria-expanded') == 'true'
        assert 'w7parent' in entry.locator('.pipeline-member-preview').inner_text()
        assert 'w7child' in entry.locator('.pipeline-member-preview').inner_text()
        assert entry.locator('.pipeline-runs-placeholder').count() == 0
        assert 'PRIVATE_MEMBER_V5' not in page.content()
        page.screenshot(path=str(artifacts() / f'runbook-viewer-details-{width}.png'), full_page=True)
        toggle.click()
        entry.locator('.pipeline-member-preview').wait_for(state='hidden')
        for link in entry.locator('.pipeline-list-actions a').all():
            assert 'instance=sql%3Aw7console' in link.get_attribute('href')
        entry.locator('a:text-is("Open")').click()
        page.wait_for_url('**/dash/pipelines/alpha?instance=sql%3Aw7console')
        page.locator('.pipeline-stage').first.wait_for(timeout=30000)
        assert not page.locator('#pipeline-edit-toggle').is_visible()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        assert not tab.errors, tab.errors
    finally:
        context.close()


def matrix(page):
    return page.locator('#pipeline-canvas-world').evaluate('''n => {
        const m=new DOMMatrix(getComputedStyle(n).transform);return {scale:m.a,x:m.e,y:m.f};
    }''')


def test_runbook_history_link_opens_the_selected_run(layout_server, browser):
    context = browser.new_context(viewport={'width': 1440, 'height': 1000})
    try:
        tab = Tab(context.new_page(), layout_server.base_url)
        tab.login('v5-viewer', VIEWER_PASSWORD, path='/dash')
        page = tab.page
        tab.goto('/dash/pipelines', '.pipelines-runbook')
        entry = page.locator('.pipeline-list-entry[data-pipeline=alpha]')
        entry.get_by_role('button', name='Details for alpha', exact=True).click()
        link = entry.locator('.pipeline-run-link').first
        link.wait_for(timeout=30000)
        from urllib.parse import urlparse, parse_qs
        href = link.get_attribute('href')
        run_id = link.get_attribute('title')
        assert parse_qs(urlparse(href).query)['run_id'] == [run_id], href
        link.click()
        page.wait_for_url(lambda url: urlparse(url).path == '/dash/pipelines/alpha'
                          and parse_qs(urlparse(url).query).get('run_id') == [run_id], timeout=5000)
        page.wait_for_selector('#pipelines-gantt .barlayer .point', timeout=30000)
        assert run_id in page.locator('#pipelines-gantt .gtitle').text_content()
        assert not tab.errors, tab.errors
    finally:
        context.close()


def touch(cdp, kind, points):
    cdp.send('Input.dispatchTouchEvent', {'type': kind, 'touchPoints': [
        {'id': i + 1, 'x': x, 'y': y, 'radiusX': 5, 'radiusY': 5, 'force': 1}
        for i, (x, y) in enumerate(points)
    ]})


def lift_touch(page, cdp):
    # Let the fingers stop before lifting. Instant CDP moves otherwise create an artificial
    # inertial fling which consumes the following tap, including on a plain HTML page.
    page.wait_for_timeout(180)
    touch(cdp, 'touchEnd', [])


def test_real_touch_pinch_tracks_midpoint_and_keeps_pan_and_buttons(layout_server, browser):
    context = browser.new_context(viewport={'width': 390, 'height': 1000}, has_touch=True, is_mobile=True)
    try:
        tab = Tab(context.new_page(), layout_server.base_url)
        tab.login(path='/dash')
        page = tab.page
        open_canvas(tab)
        box = page.locator('#pipeline-canvas').bounding_box()
        cdp = context.new_cdp_session(page)
        mx, my = box['x'] + box['width'] * .55, box['y'] + box['height'] * .6
        before = matrix(page)
        world_x = (mx - box['x'] - before['x']) / before['scale']
        world_y = (my - box['y'] - before['y']) / before['scale']
        touch(cdp, 'touchStart', [(mx - 35, my), (mx + 35, my)])
        touch(cdp, 'touchMove', [(mx - 50, my + 20), (mx + 70, my + 20)])
        after = matrix(page)
        assert after['scale'] == pytest.approx(before['scale'] * 120 / 70, abs=.015), (before, after)
        assert after['x'] + world_x * after['scale'] == pytest.approx(mx + 10 - box['x'], abs=2)
        assert after['y'] + world_y * after['scale'] == pytest.approx(my + 20 - box['y'], abs=2)
        lift_touch(page, cdp)
        assert page.evaluate('!window._mrsmCanvasGesture.pinch && !window._mrsmCanvasGesture.drag && !window._mrsmCanvasGesture.touches.size')
        # A fresh one-finger pan starts at the current viewport.
        start = (box['x'] + 25, box['y'] + box['height'] - 50)
        touch(cdp, 'touchStart', [start])
        touch(cdp, 'touchMove', [(start[0] + 35, start[1] - 20)])
        lift_touch(page, cdp)
        panned = matrix(page)
        assert panned['x'] == pytest.approx(after['x'] + 35, abs=2)
        assert panned['y'] == pytest.approx(after['y'] - 20, abs=2)
        page.locator('.pipeline-view-options > summary').tap()
        page.locator('#pipeline-view-options-panel').wait_for(timeout=3000)
        page.get_by_role('button', name='Zoom out', exact=True).tap()
        assert matrix(page)['scale'] == pytest.approx(panned['scale'] * .8, abs=.01)
        page.get_by_role('button', name='Zoom in', exact=True).tap()
        assert matrix(page)['scale'] == pytest.approx(panned['scale'], abs=.01)
        page.keyboard.press('Escape')
        touch(cdp, 'touchStart', [(mx - 30, my), (mx + 30, my)])
        touch(cdp, 'touchMove', [(mx - 40, my), (mx + 40, my)])
        lift_touch(page, cdp)
        page.locator('#pipeline-fit').tap()
        # A hybrid device may switch from touch to a mouse after a pinch.
        page.locator('.pipeline-stage').first.click()
        page.locator('#pipeline-inspector').wait_for()
        assert page.locator('#pipeline-inspector').is_visible()
        page.locator('#pipeline-inspector-close').click()
        page.locator('#pipeline-inspector').wait_for(state='hidden')
        page.locator('.pipeline-stage').last.tap()
        page.locator('#pipeline-inspector').wait_for()
        query = page.locator('#pipeline-query-view')
        query.wait_for()
        assert 'SELECT' in query.inner_text().upper()
        page.screenshot(path=str(artifacts() / 'pinch-390.png'))
        assert not tab.errors, tab.errors
    finally:
        context.close()


def test_cancelled_touch_node_move_is_never_saved(layout_server, browser):
    context = browser.new_context(viewport={'width': 768, 'height': 1100}, has_touch=True, is_mobile=True)
    try:
        tab = Tab(context.new_page(), layout_server.base_url)
        tab.login(path='/dash')
        page = tab.page
        open_canvas(tab)
        page.locator('#pipeline-edit-toggle').click()
        page.wait_for_function("document.getElementById('pipelines-page').dataset.edit === 'true'")
        page.wait_for_function("document.getElementById('pipeline-canvas-world')?.dataset.mounted === 'true'")
        node = page.locator(f'.pipeline-stage[data-ref="{CHILD}"]')
        original = node.evaluate('n => [n.style.left,n.style.top]')
        box = node.bounding_box()
        cdp = context.new_cdp_session(page)
        point = (box['x'] + 40, box['y'] + 40)
        touch(cdp, 'touchStart', [point])
        touch(cdp, 'touchMove', [(point[0] + 30, point[1] + 20)])
        assert node.evaluate('n => [n.style.left,n.style.top]') != original
        touch(cdp, 'touchCancel', [])
        assert node.evaluate('n => [n.style.left,n.style.top]') == original
        assert page.locator('#pipelines-page').get_attribute('data-dirty') == 'false'
        assert not page.evaluate('window._mrsmLayoutDraft')
        # Starting a pinch while moving a node also abandons that node edit.
        touch(cdp, 'touchStart', [point])
        touch(cdp, 'touchMove', [(point[0] + 25, point[1] + 15)])
        touch(cdp, 'touchStart', [(point[0] + 25, point[1] + 15), (point[0] + 110, point[1] + 15)])
        touch(cdp, 'touchMove', [(point[0] + 10, point[1] + 15), (point[0] + 125, point[1] + 15)])
        touch(cdp, 'touchEnd', [])
        assert node.evaluate('n => [n.style.left,n.style.top]') == original
        assert page.locator('#pipelines-page').get_attribute('data-dirty') == 'false'
        assert not page.evaluate('window._mrsmLayoutDraft')
        assert not tab.errors, tab.errors
    finally:
        context.close()
