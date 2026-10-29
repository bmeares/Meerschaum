"""RC17 query-only drilldown, fresh history paging, and source callback revocation in Chromium."""

from __future__ import annotations

import pathlib
import re
from urllib.parse import urlencode

import pytest

from tests.console.conftest import Tab
from tests.console.test_rc16_inspection import (  # noqa: F401
    INSPECTION_PASSWORD, inspection_server,
)

pytestmark = pytest.mark.console


def artifacts():
    path = pathlib.Path.home() / '.cache' / 'mrsm-rc17-console' / 'browser'
    path.mkdir(parents=True, exist_ok=True)
    return path


def wait_visible(page, locator, label):
    """Keep an isolated browser failure reviewable without reducing its assertion."""
    try:
        locator.wait_for()
    except Exception:
        (artifacts() / f'{label}.html').write_text(page.content())
        page.screenshot(path=str(artifacts() / f'{label}.png'), full_page=True)
        print('Performance failure state:', page.inner_text('body')[-10000:])
        raise


def test_performance_drilldown_and_back_work_without_a_document_reload(inspection_server, browser):
    context = browser.new_context(viewport={'width': 390, 'height': 844})
    try:
        tab = Tab(context.new_page(), inspection_server.base_url)
        tab.login('rc16-ops', INSPECTION_PASSWORD, path='/dash')
        tab.goto('/dash/performance?window=7d', '#performance-page')
        page = tab.page
        page.wait_for_function("document.getElementById('performance-body')?.textContent.includes('rc16_failure')")
        page.wait_for_function("document.querySelector('#performance-history') !== null")
        page.evaluate('window.reviewDocument = 17')
        link = page.locator('#performance-body a[href*="window=7d"]', has_text='rc16_parent').first
        link.click()
        page.wait_for_url('**/dash/performance?*pipe=*')
        page.wait_for_function("document.getElementById('performance-pipe')?.textContent.includes('rc16_parent')")
        page.wait_for_function("!document.getElementById('performance-body')?.textContent.includes('rc16_failure')")
        assert page.evaluate('window.reviewDocument') == 17
        assert page.input_value('#performance-window') == '7d'
        assert 'rc16_child' not in page.inner_text('#performance-body')
        page.go_back()
        page.wait_for_function("document.getElementById('performance-body')?.textContent.includes('rc16_failure')")
        assert page.input_value('#performance-window') == '7d'
        assert page.evaluate('window.reviewDocument') == 17
        # Query-only forged private selections must replace data with a generic refusal.
        hidden = 'sql:w7console|rc16|HIDDEN_RC16_METRIC|None'
        page.evaluate('url => window.mrsmNavigate(url)', '/dash/performance?' + urlencode({'pipe': hidden}))
        wait_visible(page, page.get_by_text('Performance is not available for this selection.', exact=True),
                     'performance-private-filter-failed')
        assert page.locator('#performance-history').count() == 0
        assert page.locator('#performance-avg_duration_seconds').count() == 0
        assert not tab.errors, tab.errors
        page.screenshot(path=str(artifacts() / 'performance-denied-390.png'))
    finally:
        context.close()


def test_performance_history_can_page_and_resets_on_filter_changes(inspection_server, browser):
    setup = inspection_server.python('''
import time
import meerschaum as mrsm
c=mrsm.get_connector('sql:w7console')
p=mrsm.Pipe('review','history',instance=c,columns={'primary':'id'})
assert p.register()[0]
for index in range(65):
    assert p.sync([{'id':index,'value':index}])[0]
ref=str(c)+'|review|history|None'
deadline=time.monotonic()+30
while time.monotonic()<deadline:
    result=mrsm.get_engine_metrics(instance=c,pipe_ref=ref)
    if result['summary']['syncs']>=65: break
    time.sleep(0.1)
else: raise AssertionError('History did not reach the expected65 outcomes.')
''')
    assert setup.returncode == 0, setup.stdout[-2000:] + setup.stderr[-4000:]
    context = browser.new_context(viewport={'width': 375, 'height': 844}, has_touch=True)
    try:
        tab = Tab(context.new_page(), inspection_server.base_url)
        tab.login('rc16-ops', INSPECTION_PASSWORD, path='/dash')
        tab.goto('/dash/performance?' + urlencode({'pipe':'sql:w7console|review|history|None'}),
                 '#performance-page')
        page = tab.page
        wait_visible(page, page.get_by_text('1–50 of 65 syncs in this sample', exact=True),
                     'performance-initial-history-failed')
        page.locator('#performance-history-page .page-link').filter(has_text=re.compile('^2$')).click()
        page.get_by_text('51–65 of 65 syncs in this sample', exact=True).wait_for()
        assert page.locator('#performance-history tbody tr').count() == 15
        page.click('#performance-refresh')
        page.get_by_text('51–65 of 65 syncs in this sample', exact=True).wait_for()
        page.select_option('#performance-window', '7d')
        page.get_by_text('1–50 of 65 syncs in this sample', exact=True).wait_for()
        assert page.locator('#performance-history tbody tr').count() == 50
        targets = page.locator('#performance-history-controls .page-link').evaluate_all(
            'nodes => nodes.map(node => {const r=node.getBoundingClientRect();return [r.width,r.height]})')
        assert targets and all(width >= 44 and height >= 44 for width, height in targets), targets
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        assert not tab.errors, tab.errors
        page.screenshot(path=str(artifacts() / 'performance-history-375.png'), full_page=True)
    finally:
        context.close()


def test_repository_source_is_literal_and_revocation_applies_to_the_next_file(inspection_server, browser):
    setup = inspection_server.python('''
import io,pathlib,tarfile
import meerschaum as mrsm
from meerschaum.config import paths
from meerschaum.core import User
c=mrsm.get_connector('sql:w7console')
assert c.register_user(User('review-source-reader', 'correct-horse-review-17',instance=c,
                               attributes={'role':'viewer'}))[0]
archive=pathlib.Path(paths.PLUGINS_ARCHIVES_RESOURCES_PATH)/'rc16_repository.tar.gz'
with tarfile.open(archive,'w:gz') as stream:
    for name,content in [('rc16_repository/__init__.py','<script>window.reviewExecuted=true</script>'),
                         ('rc16_repository/helper.py','PRIVATE_AFTER_REVOKE = 42')]:
        body=content.encode(); info=tarfile.TarInfo(name); info.size=len(body)
        stream.addfile(info,io.BytesIO(body))
''')
    assert setup.returncode == 0, setup.stdout[-2000:] + setup.stderr[-4000:]
    context = browser.new_context(viewport={'width': 375, 'height': 844})
    try:
        tab = Tab(context.new_page(), inspection_server.base_url)
        tab.login('review-source-reader', 'correct-horse-review-17', path='/dash')
        tab.goto('/dash/plugins/rc16_repository', '#plugin-source-content')
        page = tab.page
        page.wait_for_function("document.getElementById('plugin-source-content')?.textContent.includes('<script>')")
        assert page.evaluate('window.reviewExecuted === undefined')
        revoke = inspection_server.python('''
import meerschaum as mrsm
from meerschaum.core import User
c=mrsm.get_connector('sql:w7console')
assert c.set_user_attributes(User('review-source-reader',instance=c),{'role':'custom','scopes':[]})[0]
''')
        assert revoke.returncode == 0, revoke.stderr[-3000:]
        page.get_by_role('button', name='rc16_repository/helper.py', exact=True).click()
        page.get_by_text('Plugin source is not available to your account.', exact=True).wait_for()
        assert 'PRIVATE_AFTER_REVOKE' not in page.inner_text('body')
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        assert not tab.errors, tab.errors
        page.screenshot(path=str(artifacts() / 'source-revoked-375.png'))
    finally:
        context.close()
