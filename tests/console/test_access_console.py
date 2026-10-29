#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's access model in a browser (rc15 wave 2: AC1, AC2, AC4, AC5, AC7, AC11, AC13):

- a new instance's login page creates the first account as an administrator;
- each role (Viewer, Editor, Operator, a team's manager and member, another team's member, an
  administrator) gets the navigation, buttons and pipes it may use, and a hidden pipe's keys are
  in no page and no callback response;
- Access… and Locks… on a pipe's page (badges, lock icon, a locked Sync refused with how to
  unlock), the Pipes table's Team column and Mine / My teams / All filter;
- Users & Teams: emails for administrators and managers only, teams and their members.

Opt-in (`MRSM_TEST_CONSOLE=1`); see `tests/console/conftest.py`. All data is synthetic. With
`MRSM_TEST_CONSOLE_SHOTS=<dir>`, `test_role_screenshots` saves each role's pages at desktop and
phone widths there.
"""

from __future__ import annotations

import os
import pathlib

import pytest

from tests.console.conftest import USERNAME, PASSWORD, Server, Tab, free_port

pytestmark = pytest.mark.console

PASS = 'correct-horse-5'

#: The users, teams, pipes and Pipeline of the access server.
ACCESS_SETUP = '''
import meerschaum as mrsm
from meerschaum.core import User
from meerschaum.core.Pipeline._saved import save_pipeline, list_pipelines
conn = mrsm.get_connector('sql:w7console')

def user(name, role=None, email=None):
    account = User(name, %(password)r, email=email, instance=conn,
                   attributes=({'role': role} if role else None))
    if conn.get_user_id(account) is None:
        success, msg = conn.register_user(account)
        assert success, msg

for name, role, email in (
    ('w8-viewer', 'viewer', None), ('w8-editor', None, None), ('w8-operator', 'operator', None),
    ('w8-manager', None, 'manager@example.com'), ('w8-member', None, 'member@example.com'),
    ('w8-other', None, 'other@example.com'),
):
    user(name, role, email)

def team(name, members):
    group = mrsm.Team(name, instance=conn)
    if not group.exists():
        success, msg = group.register()
        assert success, msg
    for username, role in members.items():
        success, msg = group.add_members(username, role=role)
        assert success, msg

team('w8data', {'w8-manager': 'manager', 'w8-member': 'member'})
team('w8ops', {'w8-other': 'member'})

def pipe(mk, access=None):
    parameters = {'columns': {'datetime': 'dt'}}
    if access:
        parameters['access'] = access
    candidate = mrsm.Pipe('sql:w7console', mk, instance=conn, parameters=parameters)
    if candidate.get_id() is None:
        success, msg = candidate.register()
        assert success, msg

pipe('w8every')
pipe('w8teamdata', {'team': 'w8data'})
pipe('w8secret', {'team': 'w8data', 'visibility': 'private', 'owner': 'w8-member'})
pipe('w8opsonly', {'team': 'w8ops'})
pipe('w8lockme', {'team': 'w8data', 'owner': 'w8-member'})
if 'w8-mixed' not in list_pipelines(conn):
    save_pipeline('w8-mixed', ['sql:w7console|sql:w7console|w8every|None',
                               'sql:w7console|sql:w7console|w8opsonly|None'], conn)
''' % {'password': PASS}

#: Who sees which of the access server's pipes (by metric).
SEES = {
    'w8-viewer': {'w8every'},
    'w8-editor': {'w8every'},
    'w8-operator': {'w8every'},
    'w8-manager': {'w8every', 'w8teamdata', 'w8secret', 'w8lockme'},
    'w8-member': {'w8every', 'w8teamdata', 'w8secret', 'w8lockme'},
    'w8-other': {'w8every', 'w8opsonly'},
    USERNAME: {'w8every', 'w8teamdata', 'w8secret', 'w8opsonly', 'w8lockme'},
}
ALL_PIPES = set().union(*SEES.values())


@pytest.fixture(scope='module')
def access_server():
    """A server of its own (with the web terminal on) holding the access model's users and pipes."""
    port = free_port()
    instance = Server(
        'access',
        config={'api': {'webterm': {'port': port, 'tmux': {'enabled': False}}}},
        native=False,
        webterm=True,
    )
    instance.watch_ports = [port]
    try:
        instance.start()
        result = instance.python(ACCESS_SETUP)
        assert result.returncode == 0, result.stderr[-3000:]
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()


class EmptyServer(Server):
    """A server whose instance has no users yet (the first run)."""

    def prepare(self) -> None:
        return None


@pytest.fixture(scope='module')
def firstrun_server():
    instance = EmptyServer('firstrun', native=False)
    try:
        instance.start()
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()


def _errors(tab: Tab) -> list:
    return list(tab.errors)


def _wait_text(page, selector: str, text: str, timeout: int = 30000) -> None:
    page.wait_for_function(
        "([selector, text]) => { const el = document.querySelector(selector); "
        "return el && el.innerText.includes(text); }",
        arg=[selector, text],
        timeout=timeout,
    )


def _signed_in(browser, server, username: str, password: str, path: str = '/dash', viewport=None):
    """A new browser context signed in as `username` on `path`, recording every response body."""
    context = browser.new_context(**({'viewport': viewport} if viewport else {}))
    tab = Tab(context.new_page(), server.base_url)
    tab.bodies = []

    def _record(response):
        if response.url.startswith(server.base_url):
            try:
                tab.bodies.append(response.text())
            except Exception:
                pass

    tab.page.on('response', _record)
    tab.login(username, password, path=path)
    return context, tab


def _table_metrics(page) -> set:
    page.wait_for_selector('#pipes-table-body', timeout=30000)
    page.wait_for_function(
        "!document.querySelector('#pipes-table-body .spinner-border')", timeout=30000,
    )
    refs = page.eval_on_selector_all('tr.pipes-table-row', 'rows => rows.map(r => r.dataset.ref)')
    return {ref.split('|')[2] for ref in refs if ref and ref.split('|')[1] == 'sql:w7console'} & ALL_PIPES


def test_first_run_creates_the_admin_account_once(firstrun_server, browser):
    context = browser.new_context()
    try:
        tab = Tab(context.new_page(), firstrun_server.base_url)
        tab.goto('/dash/login', '#first-admin-username')
        page = tab.page
        assert page.locator('#username-input').count() == 0
        assert page.locator('#login-register-link').count() == 0
        page.fill('#first-admin-username', 'w8-first')
        page.fill('#first-admin-password', 'correct-horse-9')
        page.fill('#first-admin-confirm', 'different-horse-9')
        page.click('#first-admin-button')
        _wait_text(page, '#first-admin-alert', 'do not match')
        page.fill('#first-admin-confirm', 'correct-horse-9')
        page.click('#first-admin-button')
        ### Signed in as the new administrator, on the Overview.
        page.wait_for_selector('#overview-page', timeout=30000)
        assert 'w8-first' in page.inner_text('#pages-navbar-div')
        check = firstrun_server.python(
            "import meerschaum as mrsm\nfrom meerschaum.core import User\n"
            "conn = mrsm.get_connector('sql:w7console')\n"
            "print(conn.get_user_type(User('w8-first', instance=conn)), sorted(conn.get_users()))"
        )
        assert check.stdout.strip().endswith("admin ['w8-first']"), check.stdout + check.stderr
        ### From now on the login form (no self-registration by default).
        other = browser.new_context()
        try:
            visitor = Tab(other.new_page(), firstrun_server.base_url)
            visitor.goto('/dash/login', '#username-input')
            assert visitor.page.locator('#first-admin-button').count() == 0
            assert visitor.page.locator('#login-register-link').count() == 0
            visitor.goto('/dash/register', '#register-closed')
            assert visitor.page.locator('#register-button').count() == 0
            assert _errors(visitor) == [], _errors(visitor)
        finally:
            other.close()
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()


@pytest.mark.parametrize('username', [
    'w8-viewer', 'w8-editor', 'w8-operator', 'w8-manager', 'w8-member', 'w8-other', USERNAME,
])
def test_each_role_gets_its_pages_and_pipes_and_nothing_hidden_leaks(access_server, browser, username):
    password = PASSWORD if username == USERNAME else PASS
    context, tab = _signed_in(browser, access_server, username, password)
    try:
        page = tab.page
        nav = page.inner_text('#pages-navbar-div')
        ### The Terminal is for `webterm:use` (Operators and administrators); every role reads.
        terminal = page.locator('#console-nav-terminal').count() == 1
        assert terminal is (username in ('w8-operator', USERNAME)), (username, nav)
        for section in ('pipes', 'pipelines', 'jobs', 'connectors', 'plugins'):
            assert page.locator(f'#console-nav-{section}').count() == 1, (username, section)
        tab.goto('/dash/pipes', '#pipes-table')
        seen = _table_metrics(page)
        assert seen == SEES[username], (username, seen)
        ### New pipe is for `pipes:register` (not a Viewer).
        assert (page.locator('#pipes-new-button').count() == 1) is (username != 'w8-viewer')
        ### A Pipeline with a member they may not see says how many.
        tab.goto('/dash/pipelines', '#pipelines-list')
        page.wait_for_selector('.pipeline-list-entry', timeout=30000)
        facts = page.inner_text('#pipelines-list')
        if 'w8opsonly' not in SEES[username]:
            assert '1 hidden pipe' in facts, (username, facts)
        else:
            assert 'hidden pipe' not in facts, (username, facts)
        tab.goto('/dash', '#overview-page')
        page.wait_for_selector('#overview-pipes-body', timeout=30000)
        page.wait_for_timeout(1500)
        ### Nothing a page or a callback sent names a pipe this viewer may not see.
        hidden = ALL_PIPES - SEES[username]
        content = page.content()
        assert not [name for name in hidden if name in content], (username, hidden)
        leaked = sorted({name for name in hidden for body in tab.bodies if name in body})
        assert leaked == [], (username, leaked)
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()


def test_a_hidden_pipes_page_reads_as_not_registered_and_the_terminal_is_absent(access_server, browser):
    context, tab = _signed_in(browser, access_server, 'w8-other', PASS)
    try:
        page = tab.page
        tab.goto('/dash/pipes/sql:w7console/w8secret', '#pipe-detail-tabs')
        page.wait_for_selector('#pipe-detail-unregistered', timeout=30000)
        for button in ('#pipe-action-sync', '#pipe-edit-toggle', '#pipe-manage-menu'):
            assert page.locator(button).count() == 0, button
        ### The Terminal page is not theirs (an Editor: no `webterm:use`).
        tab.goto('/dash/terminal', '#page-not-available')
        assert page.locator('#go-button').count() == 0
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()


def _manage(page, item: str) -> None:
    page.click('#pipe-manage-menu .dropdown-toggle')
    page.click(f'.dropdown-menu.show .dropdown-item:text-is("{item}")')
    page.wait_for_selector('.modal.show', timeout=15000)


def test_access_and_locks_dialogs_and_a_locked_sync(access_server, browser):
    """
    The owner of `w8lockme` sets its visibility and locks it against syncs: the badges and the
    lock icon follow; Sync is refused with how to unlock; the team's manager unlocks it.
    """
    context, tab = _signed_in(browser, access_server, 'w8-member', PASS)
    try:
        page = tab.page
        tab.goto('/dash/pipes/sql:w7console/w8lockme', '#pipe-detail-tabs')
        page.wait_for_selector('#pipe-detail-team', timeout=30000)
        assert 'w8data' in page.inner_text('#pipe-detail-team')
        assert 'w8-member' in page.inner_text('#pipe-detail-owner')
        _manage(page, 'Access…')
        page.click('#pipe-access-modal label:text-is("Everyone")')
        page.click('#pipe-access-save')
        page.wait_for_selector('.modal.show', state='detached', timeout=15000)
        page.wait_for_selector('#pipe-detail-visibility', timeout=15000)
        assert 'Everyone' in page.inner_text('#pipe-detail-visibility')
        _manage(page, 'Locks…')
        page.click('#pipe-locks-modal label:text-is("Sync")')
        page.click('#pipe-locks-save')
        page.wait_for_selector('.modal.show', state='detached', timeout=15000)
        page.wait_for_selector('#pipe-detail-locks', timeout=15000)
        page.click('#pipe-action-sync')
        _wait_text(page, '#pipe-detail-alert', 'Manage > Locks')
        assert 'locked' in page.inner_text('#pipe-detail-alert')
        ### Visible to everyone now: another team's member sees it in the table, with its lock.
        other_context, other = _signed_in(browser, access_server, 'w8-other', PASS, path='/dash/pipes')
        try:
            other.goto('/dash/pipes', '#pipes-table')
            assert 'w8lockme' in _table_metrics(other.page)
            row = other.page.locator('tr.pipes-table-row[data-ref="sql:w7console|sql:w7console|w8lockme|None"]')
            assert row.locator('.pipes-lock').count() == 1
            ### Not their team's: no Access… or Locks… for them.
            other.goto('/dash/pipes/sql:w7console/w8lockme', '#pipe-detail-tabs')
            other.page.wait_for_selector('#pipe-detail-locks', timeout=30000)
            assert other.page.locator('#pipe-manage-access').count() == 0
            assert other.page.locator('#pipe-manage-locks').count() == 0
        finally:
            other_context.close()
        ### The team's manager unlocks it (and gives it back to the team).
        manager_context, manager = _signed_in(browser, access_server, 'w8-manager', PASS)
        try:
            mpage = manager.page
            manager.goto('/dash/pipes/sql:w7console/w8lockme', '#pipe-detail-tabs')
            mpage.wait_for_selector('#pipe-detail-locks', timeout=30000)
            _manage(mpage, 'Locks…')
            mpage.click('#pipe-locks-modal label:text-is("Sync")')
            mpage.click('#pipe-locks-save')
            mpage.wait_for_selector('.modal.show', state='detached', timeout=15000)
            mpage.wait_for_selector('#pipe-detail-locks', state='detached', timeout=15000)
            _manage(mpage, 'Access…')
            mpage.click('#pipe-access-modal label:text-is("Its team")')
            mpage.click('#pipe-access-save')
            mpage.wait_for_selector('#pipe-detail-visibility', state='detached', timeout=15000)
            assert _errors(manager) == [], _errors(manager)
        finally:
            manager_context.close()
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()


def test_pipes_table_team_column_and_owner_filter(access_server, browser):
    context, tab = _signed_in(browser, access_server, 'w8-member', PASS, path='/dash/pipes')
    try:
        page = tab.page
        tab.goto('/dash/pipes', '#pipes-table')
        assert _table_metrics(page) == SEES['w8-member']
        assert page.locator('#pipes-header-team').count() == 1
        row = page.locator('tr.pipes-table-row[data-ref="sql:w7console|sql:w7console|w8teamdata|None"]')
        assert 'w8data' in row.locator('.pipes-col-team').inner_text()
        page.click('#pipes-scope-select label:text-is("Mine")')
        page.wait_for_function("location.search.includes('scope=mine')", timeout=15000)
        page.wait_for_function(
            "[...document.querySelectorAll('tr.pipes-table-row')].every(r => /w8secret|w8lockme/.test(r.dataset.ref))",
            timeout=15000,
        )
        assert _table_metrics(page) == {'w8secret', 'w8lockme'}
        page.click('#pipes-scope-select label:text-is("My teams")')
        page.wait_for_function("location.search.includes('scope=teams')", timeout=15000)
        page.wait_for_function(
            "[...document.querySelectorAll('tr.pipes-table-row')].length === 3", timeout=15000,
        )
        assert _table_metrics(page) == {'w8teamdata', 'w8secret', 'w8lockme'}
        page.click('#pipes-scope-select label:text-is("All")')
        page.wait_for_function("!location.search.includes('scope=')", timeout=15000)
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()


def test_users_and_teams_emails_and_members(access_server, browser):
    ### An administrator: every user with Type, Role and Email; teams with their menus.
    context, tab = _signed_in(browser, access_server, USERNAME, PASSWORD, path='/dash/users')
    try:
        page = tab.page
        page.wait_for_selector('#users-table', timeout=30000)
        _wait_text(page, '#users-table', 'member@example.com')
        headers = page.locator('#users-table thead').inner_text()
        assert 'Role' in headers and 'Email' in headers and 'Teams' in headers
        assert 'Operator' in page.inner_text('#users-table')
        page.click('#users-tabs a.nav-link:text-is("Teams")')
        page.wait_for_selector('#teams-table', timeout=30000)
        page.wait_for_function("location.pathname === '/dash/teams'", timeout=15000)
        assert 'w8data' in page.inner_text('#teams-table') and 'Everyone' in page.inner_text('#teams-table')
        page.click('#teams-new-button')
        page.wait_for_selector('.modal.show #teams-new-name', timeout=15000)
        page.fill('#teams-new-name', 'w8crew')
        page.click('#teams-new-submit')
        page.wait_for_selector('.modal.show', state='detached', timeout=15000)
        page.wait_for_selector('#teams-table tr:has(.team-name:text-is("w8crew"))', timeout=15000)
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()
    ### A team's manager: their team's members with their emails; Members for their team only.
    context, tab = _signed_in(browser, access_server, 'w8-manager', PASS, path='/dash/users')
    try:
        page = tab.page
        page.wait_for_selector('#users-table', timeout=30000)
        text = page.inner_text('#users-table')
        assert 'member@example.com' in text and 'other@example.com' not in text
        assert 'w8-other' not in text
        assert page.locator('#users-add-button').is_hidden()
        tab.goto('/dash/teams', '#teams-table')
        data_row = page.locator('#teams-table tr:has(.team-name:text-is("w8data"))')
        data_row.locator('.job-row-menu-toggle').click()
        page.click('.dropdown-menu.show .dropdown-item:text-is("Members")')
        page.wait_for_selector('.modal.show #team-members-table', timeout=15000)
        assert 'w8-member' in page.inner_text('#team-members-table')
        page.click('#teams-members-close')
        page.wait_for_selector('.modal.show', state='detached', timeout=15000)
        assert page.locator('#teams-new-button').is_hidden()
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()
    ### A member: no emails but their own.
    context, tab = _signed_in(browser, access_server, 'w8-member', PASS, path='/dash/users')
    try:
        page = tab.page
        page.wait_for_selector('#users-table', timeout=30000)
        text = page.inner_text('#users-table')
        assert 'manager@example.com' not in text and 'w8-manager' in text
        assert 'Email' not in page.locator('#users-table thead').inner_text()
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()


def test_role_presets_custom_permissions_and_explicit_everyone_primary_team(access_server, browser):
    setup = access_server.python('''
import meerschaum as mrsm
from meerschaum.core import User
conn = mrsm.get_connector('sql:w7console')
user = User('w8-permissions', 'correct-horse-9', instance=conn,
            attributes={'role': 'editor', 'primary_team': 'Everyone'})
assert conn.register_user(user)[0]
assert mrsm.Team('w8data', instance=conn).add_members('w8-permissions')[0]
''')
    assert setup.returncode == 0, setup.stderr[-3000:]
    context, tab = _signed_in(browser, access_server, USERNAME, PASSWORD, path='/dash/users')
    try:
        page = tab.page
        row = page.locator('#users-table tbody tr:has(a:text-is("w8-permissions"))')
        row.wait_for(timeout=30000)
        row.locator('.job-row-menu-toggle').click()
        page.click('.dropdown-menu.show .dropdown-item:text-is("Edit")')
        page.wait_for_selector('.modal.show #users-edit-role', timeout=15000)
        page.select_option('#users-edit-role', 'viewer')
        pipes = page.locator('.users-permissions-group').filter(has=page.locator('legend:text-is("Pipes")'))
        write = pipes.locator('.form-check').filter(has_text='Edit and sync').locator('input')
        page.wait_for_function('(input) => !input.checked', arg=write.element_handle(), timeout=10000)
        jobs = page.locator('.users-permissions-group').filter(has=page.locator('legend:text-is("Jobs")'))
        jobs.locator('label:text-is("Read logs")').click()
        page.wait_for_function("document.getElementById('users-edit-role').value === 'custom'", timeout=10000)
        assert page.input_value('#users-edit-primary') == 'Everyone'
        page.click('#users-edit-submit')
        page.wait_for_selector('.modal.show', state='detached', timeout=15000)
        stored = access_server.python('''
import meerschaum as mrsm
from meerschaum.core import User
conn = mrsm.get_connector('sql:w7console')
attributes = conn.get_user_attributes(User('w8-permissions', instance=conn))
assert attributes['role'] == 'custom', attributes
assert 'logs:read' not in attributes['scopes'], attributes
assert attributes['primary_team'] == 'Everyone', attributes
''')
        assert stored.returncode == 0, stored.stderr[-3000:]
        assert _errors(tab) == [], _errors(tab)
    finally:
        context.close()


SHOTS_ENV = 'MRSM_TEST_CONSOLE_SHOTS'


@pytest.mark.skipif(not os.environ.get(SHOTS_ENV), reason=f"Set {SHOTS_ENV}=<dir> to save screenshots.")
@pytest.mark.parametrize('viewport', [
    {'width': 1280, 'height': 900},
    {'width': 390, 'height': 844},
], ids=['desktop', 'phone'])
def test_role_screenshots(access_server, browser, viewport):
    """Each role's Pipes table, a team pipe's page and Users & Teams (for the release notes)."""
    out = pathlib.Path(os.environ[SHOTS_ENV])
    out.mkdir(parents=True, exist_ok=True)
    width = 'desktop' if viewport['width'] > 600 else 'phone'
    for label, username in (('viewer', 'w8-viewer'), ('editor', 'w8-editor'), ('operator', 'w8-operator'),
                            ('admin', USERNAME), ('manager', 'w8-manager')):
        password = PASSWORD if username == USERNAME else PASS
        context, tab = _signed_in(browser, access_server, username, password, viewport=viewport)
        try:
            page = tab.page
            tab.goto('/dash/pipes', '#pipes-table')
            _table_metrics(page)
            page.screenshot(path=str(out / f'{label}-{width}-pipes.png'), full_page=True)
            tab.goto('/dash/pipes/sql:w7console/w8every', '#pipe-detail-tabs')
            page.wait_for_selector('#pipe-detail-content', timeout=30000)
            page.wait_for_timeout(800)
            if page.locator('#pipe-manage-menu').count():
                page.click('#pipe-manage-menu .dropdown-toggle')
                page.wait_for_selector('.dropdown-menu.show', timeout=10000)
            page.screenshot(path=str(out / f'{label}-{width}-pipe.png'), full_page=True)
            tab.goto('/dash/users', '#users-table')
            page.wait_for_timeout(800)
            page.screenshot(path=str(out / f'{label}-{width}-users.png'), full_page=True)
            if label in ('admin', 'manager'):
                tab.goto('/dash/teams', '#teams-table')
                page.wait_for_timeout(800)
                page.screenshot(path=str(out / f'{label}-{width}-teams.png'), full_page=True)
            ### Phone width: no horizontal page scroll.
            if width == 'phone':
                overflow = page.evaluate('document.documentElement.scrollWidth - window.innerWidth')
                assert overflow <= 1, (label, overflow)
            if label == 'admin':
                ### The Edit dialog: type, role and the permissions grid, teams, primary team.
                tab.goto('/dash/users', '#users-table')
                row = page.locator('#users-table tbody tr:has(a:text-is("w8-member"))')
                row.locator('.job-row-menu-toggle').click()
                page.click('.dropdown-menu.show .dropdown-item:text-is("Edit")')
                page.wait_for_selector('.modal.show #users-edit-role', timeout=15000)
                page.wait_for_timeout(500)
                page.screenshot(path=str(out / f'{label}-{width}-edit-user.png'), full_page=True)
                tab.goto('/dash/pipelines', '#pipelines-list')
                page.wait_for_selector('.pipeline-list-entry', timeout=30000)
                page.screenshot(path=str(out / f'{label}-{width}-pipelines.png'), full_page=True)
            if label == 'manager':
                ### Access… and Locks… of their team's pipe, and New pipe's Team and Visibility.
                tab.goto('/dash/pipes/sql:w7console/w8teamdata', '#pipe-detail-tabs')
                page.wait_for_selector('#pipe-detail-team', timeout=30000)
                page.screenshot(path=str(out / f'{label}-{width}-team-pipe.png'), full_page=True)
                _manage(page, 'Access…')
                page.wait_for_timeout(400)
                page.screenshot(path=str(out / f'{label}-{width}-access.png'), full_page=False)
                page.click('#pipe-access-cancel')
                page.wait_for_selector('.modal.show', state='detached', timeout=15000)
                _manage(page, 'Locks…')
                page.wait_for_timeout(400)
                page.screenshot(path=str(out / f'{label}-{width}-locks.png'), full_page=False)
                page.click('#pipe-locks-cancel')
                tab.goto('/dash/pipes/new', '#new-pipe-team')
                page.screenshot(path=str(out / f'{label}-{width}-new-pipe.png'), full_page=True)
                tab.goto('/dash/pipelines', '#pipelines-list')
                page.wait_for_selector('.pipeline-list-entry', timeout=30000)
                page.screenshot(path=str(out / f'{label}-{width}-pipelines.png'), full_page=True)
            if label == 'editor':
                tab.goto('/dash/terminal', '#page-not-available')
                page.screenshot(path=str(out / f'{label}-{width}-terminal.png'), full_page=False)
        finally:
            context.close()


@pytest.mark.skipif(not os.environ.get(SHOTS_ENV), reason=f"Set {SHOTS_ENV}=<dir> to save screenshots.")
@pytest.mark.parametrize('viewport', [
    {'width': 1280, 'height': 900},
    {'width': 390, 'height': 844},
], ids=['desktop', 'phone'])
def test_first_run_screenshots(firstrun_server, browser, viewport):
    """The login page of an instance without users (run before the first-run test creates one)."""
    out = pathlib.Path(os.environ[SHOTS_ENV])
    out.mkdir(parents=True, exist_ok=True)
    width = 'desktop' if viewport['width'] > 600 else 'phone'
    context = browser.new_context(viewport=viewport)
    try:
        tab = Tab(context.new_page(), firstrun_server.base_url)
        tab.goto('/dash/login', '#username-input, #first-admin-username')
        if tab.page.locator('#first-admin-username').count():
            tab.page.screenshot(path=str(out / f'first-run-{width}-login.png'), full_page=True)
    finally:
        context.close()
