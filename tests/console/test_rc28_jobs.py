#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Jobs page of rc28 in a browser: the jobs grouped by state (running first), a one-word Status
and its own Last Run column, a wide page whose Command column wraps and can be selected, and the
refresh that updates the table in place (no scroll back to the top, no rebuild, an open menu and
a selection left alone, a filter kept, a new job listed). Opt-in (`MRSM_TEST_CONSOLE=1`); see
`tests/console/conftest.py`. All data is synthetic.
"""

from __future__ import annotations

import os
import pathlib
import re
import time

import pytest

from tests.console.conftest import Tab

pytestmark = pytest.mark.console

SHOTS_ENV = 'MRSM_TEST_CONSOLE_SHOTS'

LONG_COMMAND = [
    'show', 'pipes', '-c', 'sql:w28_source_with_a_rather_long_label_for_wrapping', 'plugin:w28_other',
    '-m', 'alpha_metric', 'beta_metric', 'gamma_metric', 'delta_metric', 'epsilon_metric',
    '-l', 'station_one', 'station_two', '-t', 'daily', 'hourly', 'backfill', '-i', 'sql:w7console',
]
JOBS = {
    'w28-run-a': ['show', 'version', '--loop', '--min-seconds', '2'],
    'w28-run-b': ['show', 'version', '--loop', '--min-seconds', '2'],
    'w28-pause': ['show', 'version', '--loop', '--min-seconds', '2'],
    'w28-once': ['show', 'version'],
    'w28-fail': ['show', 'pipes', '-i', 'sql:w28undefined'],
    'w28-long': LONG_COMMAND,
    ### Past eight jobs the page has its filter box.
    'w28-once-2': ['show', 'version'],
    'w28-once-3': ['show', 'version'],
    'w28-once-4': ['show', 'version'],
}
ROW = '#jobs-table tr.job-row:has(.job-name-link:text-is("{}"))'
STATUS_WORDS = {'Running', 'Stalled', 'Failing', 'Paused', 'Stopped', 'Failed'}
LAST_RUN = re.compile(r'^(just now|\d+ min ago|\d+ h ago|\d+ days? ago|Never)$')


def _shot(page, name: str) -> None:
    out = os.environ.get(SHOTS_ENV)
    if out:
        pathlib.Path(out).mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(pathlib.Path(out) / f'rc28-jobs-{name}.png'), full_page=True)


def _cleanup(server, *names: str) -> None:
    for name in names:
        server.mrsm('stop', 'jobs', name, '--yes', '--nopretty')
        server.mrsm('delete', 'jobs', name, '--yes', '--nopretty')


@pytest.fixture(scope='module')
def mixed_jobs(server):
    """Jobs in every state: two running, one paused, one done, one failed, a long command."""
    for name, sysargs in JOBS.items():
        started = server.mrsm(*sysargs, '-d', '--name', name, '--yes', '--nopretty')
        assert started.returncode == 0, started.stdout + started.stderr
    deadline = time.time() + 60
    while time.time() < deadline:
        paused = server.mrsm('pause', 'jobs', 'w28-pause', '--yes', '--nopretty')
        if paused.returncode == 0:
            break
        time.sleep(1)
    yield list(JOBS)
    _cleanup(server, *JOBS, 'w28-new')


def _states(page) -> dict:
    return page.evaluate("""() => Object.fromEntries([...document.querySelectorAll('#jobs-table tr.job-row')]
        .map(tr => [tr.querySelector('.job-name-link').textContent, {
            status: tr.querySelector('.job-status').innerText,
            last: tr.querySelector('.job-last-run').innerText,
            group: [...tr.classList].find(c => c.startsWith('job-group-')),
            y: tr.getBoundingClientRect().y,
            visible: tr.offsetParent !== null,
        }]))""")


def _groups(page) -> dict:
    return page.evaluate("""() => Object.fromEntries([...document.querySelectorAll('#jobs-table tr.jobs-group-row')]
        .map(tr => [tr.id && JSON.parse(tr.id).index, {
            y: tr.getBoundingClientRect().y, count: tr.querySelector('.jobs-group-count').innerText,
            visible: tr.offsetParent !== null}]))""")


def _wait_for_states(page, expected: dict, timeout: int = 60000) -> None:
    page.wait_for_function(
        """(expected) => Object.entries(expected).every(([name, status]) => {
            const tr = [...document.querySelectorAll('#jobs-table tr.job-row')]
                .find(tr => tr.querySelector('.job-name-link').textContent === name);
            return tr && tr.querySelector('.job-status').innerText === status;
        })""",
        arg=expected, timeout=timeout,
    )


class Refreshes:
    """The table refreshes a page asked the host for (`refresh_jobs_table`)."""

    def __init__(self, page):
        self.done: list[float] = []
        page.on('requestfinished', self._finished)

    def _finished(self, request):
        if '_dash-update-component' in request.url and 'jobs-structure-changed' in (request.post_data or ''):
            self.done.append(time.time())

    def wait_for_one(self, page, after: float, timeout: float = 25.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if any(t > after for t in self.done):
                page.wait_for_timeout(300)
                return
            page.wait_for_timeout(250)
        raise AssertionError('The table did not refresh.')


def test_jobs_are_grouped_wrapped_and_refreshed_in_place(server, context, mixed_jobs):
    page = context.new_page()
    page.set_viewport_size({'width': 1280, 'height': 600})
    tab = Tab(page, server.base_url)
    refreshes = Refreshes(page)
    tab.login(path='/dash/jobs')
    page.wait_for_selector(ROW.format('w28-long'), timeout=30000)
    _wait_for_states(page, {
        'w28-run-a': 'Running', 'w28-run-b': 'Running', 'w28-pause': 'Paused',
        'w28-once': 'Stopped', 'w28-fail': 'Failed',
    })
    _shot(page, 'desktop')

    ### Groups: Running, then Paused, then Stopped, each job under its own group's header.
    states, groups = _states(page), _groups(page)
    assert groups['running']['y'] < states['w28-run-a']['y'] < groups['paused']['y']
    assert groups['paused']['y'] < states['w28-pause']['y'] < groups['stopped']['y']
    assert groups['stopped']['y'] < states['w28-once']['y'] and groups['stopped']['y'] < states['w28-fail']['y']
    assert states['w28-run-b']['group'] == 'job-group-running'
    ### Status is one word; Last Run is a column of its own.
    headers = page.locator('#jobs-table thead th').all_inner_texts()
    assert [h.strip().lower() for h in headers[:4]] == ['job', 'status', 'last run', 'command'], headers
    for name, state in states.items():
        assert state['status'] in STATUS_WORDS, (name, state)
        assert LAST_RUN.match(state['last']), (name, state)

    ### The long command wraps, whole, and its text is selectable without following a link.
    code = page.locator(ROW.format('w28-long')).locator('.job-command')
    assert code.inner_text().split()[:len(LONG_COMMAND)] == LONG_COMMAND
    box = code.bounding_box()
    line = float(code.evaluate("el => parseFloat(getComputedStyle(el).lineHeight)"))
    assert box['height'] > 1.8 * line, (box, line)
    assert code.evaluate("el => getComputedStyle(el).userSelect") in ('text', 'auto')
    url = page.url
    page.mouse.move(box['x'] + 2, box['y'] + line / 2)
    page.mouse.down()
    page.mouse.move(box['x'] + box['width'] - 4, box['y'] + box['height'] - line / 2, steps=8)
    page.mouse.up()
    selected = page.evaluate("getSelection().toString()")
    assert len(selected) > 80 and 'w28_source_with_a_rather_long_label' in selected, selected
    assert page.url == url

    ### A selection in the table holds the refresh back (it would lose it).
    before = time.time()
    page.wait_for_timeout(16500)
    assert not [t for t in refreshes.done if t > before], refreshes.done
    assert page.evaluate("getSelection().toString()") == selected
    page.evaluate("getSelection().removeAllRanges()")

    ### A refresh updates the rows in place: the same table and rows, the scroll position kept.
    page.evaluate("""() => { document.getElementById('jobs-table')._w28 = 1;
        document.querySelectorAll('#jobs-table tr.job-row').forEach(tr => tr._w28 = 1); }""")
    page.evaluate("document.scrollingElement.scrollTop = document.scrollingElement.scrollHeight")
    page.wait_for_timeout(500)
    scrolled = page.evaluate("document.scrollingElement.scrollTop")
    assert scrolled > 0
    refreshes.wait_for_one(page, time.time())
    assert page.evaluate("document.scrollingElement.scrollTop") == scrolled
    assert page.evaluate("""() => document.getElementById('jobs-table')._w28 === 1
        && [...document.querySelectorAll('#jobs-table tr.job-row')].every(tr => tr._w28 === 1)""")
    page.evaluate("document.scrollingElement.scrollTop = 0")

    ### An open menu stays open (the refresh waits for it).
    page.locator(ROW.format('w28-run-b')).locator('.job-row-menu-toggle').click()
    page.wait_for_selector('#jobs-table .dropdown-menu.show')
    items = page.locator('#jobs-table .dropdown-menu.show .dropdown-item').all_inner_texts()
    assert items == ['Pause', 'Stop', 'Restart', 'View logs', 'Download logs', 'Delete'], items
    _shot(page, 'menu')
    before = time.time()
    page.wait_for_timeout(16500)
    assert page.locator('#jobs-table .dropdown-menu.show').count() == 1
    assert not [t for t in refreshes.done if t > before], refreshes.done

    ### Pause from the menu: the row moves to Paused in place, and back with Resume.
    page.click('#jobs-table .dropdown-menu.show .dropdown-item:text-is("Pause")')
    _wait_for_states(page, {'w28-run-b': 'Paused'}, timeout=20000)
    states, groups = _states(page), _groups(page)
    assert states['w28-run-b']['group'] == 'job-group-paused'
    assert groups['paused']['y'] < states['w28-run-b']['y'] < groups['stopped']['y']
    assert page.evaluate("""() => [...document.querySelectorAll('#jobs-table tr.job-row')]
        .find(tr => tr.innerText.includes('w28-run-b'))._w28 === 1""")
    _shot(page, 'moved')
    page.locator(ROW.format('w28-run-b')).locator('.job-row-menu-toggle').click()
    page.click('#jobs-table .dropdown-menu.show .dropdown-item:text-is("Resume")')
    _wait_for_states(page, {'w28-run-b': 'Running'}, timeout=20000)

    ### The filter is kept, and a job that appears is listed by the next refresh (here the first
    ### one matching the filter: the tab had no table).
    page.fill('#jobs-filter-input', 'w28-new')
    page.press('#jobs-filter-input', 'Enter')
    page.wait_for_selector('.jobs-empty:has-text("No jobs match")', timeout=30000)
    started = server.mrsm('show', 'version', '-d', '--name', 'w28-new', '--yes', '--nopretty')
    assert started.returncode == 0, started.stdout + started.stderr
    page.wait_for_selector(ROW.format('w28-new'), timeout=40000)
    assert page.input_value('#jobs-filter-input') == 'w28-new'
    assert list(_states(page)) == ['w28-new']
    assert tab.errors == [], tab.errors


def test_jobs_table_on_a_phone(server, context, mixed_jobs):
    page = context.new_page()
    page.set_viewport_size({'width': 390, 'height': 844})
    tab = Tab(page, server.base_url)
    tab.login(path='/dash/jobs')
    page.wait_for_selector(ROW.format('w28-long'), timeout=30000)
    _wait_for_states(page, {'w28-run-a': 'Running', 'w28-pause': 'Paused'})
    _shot(page, 'phone')
    assert page.evaluate('document.documentElement.scrollWidth') <= 390
    ### Each job: its name and menu, its Status and Last Run, then its command (wrapped).
    geometry = page.evaluate("""() => {
        const tr = [...document.querySelectorAll('#jobs-table tr.job-row')].find(tr => tr.innerText.includes('w28-long'));
        const box = sel => { const r = tr.querySelector(sel).getBoundingClientRect(); return [r.x, r.y, r.width, r.height]; };
        return {name: box('.job-name-link'), status: box('.job-status'), last: box('.job-last-run'),
                command: box('.job-command'), menu: box('.job-row-menu-toggle')};
    }""")
    assert geometry['status'][1] >= geometry['name'][1] + geometry['name'][3] - 1, geometry
    assert abs(geometry['status'][1] - geometry['last'][1]) < 8, geometry
    assert geometry['command'][1] >= geometry['status'][1] + geometry['status'][3] - 1, geometry
    assert geometry['command'][0] + geometry['command'][2] <= 390, geometry
    assert geometry['menu'][2] >= 44 and geometry['menu'][3] >= 44, geometry
    assert tab.errors == [], tab.errors
