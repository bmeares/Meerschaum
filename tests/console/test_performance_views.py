"""Window coverage and locally searchable, pageable Performance views at both widths."""

import pathlib

import pytest

from tests.console.conftest import Tab
from tests.console.test_rc16_inspection import INSPECTION_PASSWORD, inspection_server  # noqa: F401

pytestmark = pytest.mark.console


@pytest.fixture(scope='module')
def populated_performance(inspection_server):
    result = inspection_server.python('''
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os, sqlite3, uuid
import meerschaum as mrsm
from meerschaum import _core
c = mrsm.get_connector('sql:w7console')
for index in range(24):
    p = mrsm.Pipe('review', f'rc23_perf_{index:02d}', instance=c, columns={'primary':'id'})
    assert p.sync([{'id': i} for i in range(index + 1)])[0]
_core.flush_sync_statuses(timeout=10.0)
uri = os.environ['MRSM_SQL_W7CONSOLE']
assert uri.startswith('sqlite:///')
database = Path(uri.removeprefix('sqlite:///')).resolve()
assert database.is_relative_to(Path(__import__('os').environ['MRSM_ROOT_DIR']).resolve())
with sqlite3.connect(database) as db:
    db.row_factory = sqlite3.Row
    tables = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%engine_metrics%'").fetchall()
    assert len(tables) == 1
    table = tables[0]['name']
    assert all(char.isalnum() or char == '_' for char in table)
    template = dict(db.execute(f'SELECT * FROM "{table}" WHERE metric_key=? LIMIT 1', ('rc23_perf_00',)).fetchone())
    start = datetime.now(timezone.utc) - timedelta(hours=23, minutes=50)
    rows = []
    for index in range(1050):
        row = dict(template)
        when = start + timedelta(seconds=index * 81)
        row.update(event_id=str(uuid.uuid4()), ts_us=int(when.timestamp()*1000000),
                   ts=when.isoformat(), started=(when-timedelta(seconds=0.25)).isoformat(),
                   duration_seconds=0.25)
        rows.append(tuple(row[name] for name in template))
    columns = ','.join('"'+name+'"' for name in template)
    placeholders = ','.join('?' for _ in template)
    db.executemany(f'INSERT INTO "{table}" ({columns}) VALUES ({placeholders})', rows)
''')
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
    return inspection_server


@pytest.mark.parametrize('width', [390, 1440])
def test_performance_complete_window_and_responsive_breakdowns(populated_performance, browser, width):
    context = browser.new_context(viewport={'width': width, 'height': 844}, has_touch=width < 768)
    try:
        tab = Tab(context.new_page(), populated_performance.base_url)
        tab.login('rc16-ops', INSPECTION_PASSWORD, path='/dash')
        tab.goto('/dash/performance', '#performance-page')
        page = tab.page
        page.locator('#performance-pipes tbody tr').first.wait_for()
        page.wait_for_function("!document.getElementById('performance-refresh').disabled")
        page.evaluate("dash_clientside.set_props('performance-interval', {disabled:true})")
        coverage = page.inner_text('#performance-coverage')
        assert 'all recorded syncs' in coverage and 'latest 1,000' in coverage, coverage
        assert 'Sampled syncs' not in page.inner_text('.performance-stats')
        counts = page.locator('#performance-results .js-plotly-plot').evaluate('graph => graph.data.map(t => t.y)')
        assert sum(sum(values) for values in counts) > 1000
        assert sum(1 for values in zip(*counts) if sum(values) > 0) >= 40
        search = page.get_by_role('searchbox', name='Find a Pipe')
        search.fill('rc23_perf_')
        page.wait_for_function("document.querySelector('#performance-pipes tbody')?.textContent.includes('rc23_perf_')")
        expected = 5 if width < 768 else 15
        assert page.locator('#performance-pipes tbody tr').count() == expected
        page.get_by_label('Rank Pipes').select_option('name')
        section = page.locator('#performance-pipes')
        if width < 768:
            section.locator('.page-item:nth-last-child(2) .page-link').click()
        else:
            section.locator('.page-link').filter(has_text='2').click()
        first = section.locator('tbody tr').first.inner_text()
        assert ('rc23_perf_05' if width < 768 else 'rc23_perf_15') in first, first
        page.click('#performance-refresh')
        page.wait_for_function("!document.getElementById('performance-refresh').disabled")
        page.wait_for_function("document.querySelector('#performance-pipes tbody')?.textContent.includes('rc23_perf_')")
        assert search.input_value() == 'rc23_perf_'
        assert first == section.locator('tbody tr').first.inner_text()
        output = pathlib.Path.home() / '.cache/mrsm-5.0.0rc24/screenshots'
        output.mkdir(parents=True, exist_ok=True)
        section.screenshot(path=str(output / f'performance-pipes-{width}.png'))
        # Recent history stays a distinct view, with adjustable page size and mobile cards.
        page.get_by_role('tab', name='Recent syncs', exact=True).click()
        page.locator('#performance-history-controls').wait_for(state='visible')
        assert page.locator('#performance-history tbody tr').count() == 10
        page.select_option('#performance-history-page-size', '25')
        page.wait_for_function("document.querySelectorAll('#performance-history tbody tr').length===25")
        assert 'active' in page.get_by_role('tab', name='Recent syncs', exact=True).get_attribute('class').split()
        assert page.locator('#performance-history').is_visible()
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth + 1')
        if width < 768:
            assert page.locator('#performance-history tbody tr').first.evaluate("el => getComputedStyle(el).display") == 'grid'
        assert not tab.errors, tab.errors
        page.screenshot(path=str(output / f'performance-history-{width}.png'), full_page=True)
    finally:
        if context.pages:
            output = pathlib.Path.home() / '.cache/mrsm-5.0.0rc24/screenshots'
            output.mkdir(parents=True, exist_ok=True)
            context.pages[0].screenshot(path=str(output / f'performance-last-state-{width}.png'), full_page=True)
        context.close()
