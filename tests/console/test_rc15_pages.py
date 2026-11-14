#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's Jobs, Users, Plugins and Tokens pages in a browser (rc15): New job, Restart and
Delete, job health in the Status and Last Run columns, the jobs table, the users' admin actions and the
last-admin guard, the Installed and Repository tabs with Uninstall and Install, and phone width.
Opt-in (`MRSM_TEST_CONSOLE=1`); see `tests/console/conftest.py`. All data is synthetic.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import time

import pytest

from tests.console.conftest import USERNAME, PASSWORD, Server, Tab, mrsm_binary
from tests.console.test_console import SETUP_PIPES

pytestmark = pytest.mark.console


def _errors(tab: Tab) -> list:
    return list(tab.errors)


def _wait_text(page, selector: str, text: str, timeout: int = 30000) -> None:
    page.wait_for_function(
        "([selector, text]) => { const el = document.querySelector(selector); "
        "return el && el.innerText.includes(text); }",
        arg=[selector, text],
        timeout=timeout,
    )


def _job_exists(server, name: str) -> bool:
    result = server.python(
        "from meerschaum.jobs import Job\n"
        f"print(Job({name!r}, executor_keys='local').exists())"
    )
    return result.stdout.strip().endswith('True')


def _cleanup_jobs(server, *names: str) -> None:
    for name in names:
        server.mrsm('stop', 'jobs', name, '--yes', '--nopretty')
        server.mrsm('delete', 'jobs', name, '--yes', '--nopretty')


def _job_unit(page, name: str):
    """A job's row of the jobs table."""
    return page.locator(f'#jobs-table tbody tr.job-row:has(.job-name-link:text-is("{name}"))')


def _job_action(page, name: str, label: str) -> None:
    """Click the item of a job's row menu."""
    _job_unit(page, name).locator('.job-row-menu-toggle').click()
    page.click(f'.dropdown-menu.show .dropdown-item:text-is("{label}")')


def _status(page, name: str) -> str:
    return _job_unit(page, name).locator('.job-status').first.inner_text()


def _dropdown_options(page, dropdown_id: str) -> list:
    """Open a dropdown of the New job form and return its options' labels."""
    page.click(f'#{dropdown_id}')
    options = page.locator('[role=option]')
    options.first.wait_for(timeout=15000)
    return [text.strip() for text in options.all_inner_texts()]


def _close_dropdown(page) -> None:
    """Close an open dropdown by clicking the form's title (Escape would close the form)."""
    page.click('#new-job-modal .modal-title')
    page.wait_for_selector('[role=option]', state='detached', timeout=15000)


def test_new_job_creates_starts_restarts_and_deletes_a_job(server, tab):
    name = 'w7-new-job'
    setup = server.python(SETUP_PIPES)
    assert setup.returncode == 0, setup.stderr[-2000:]
    try:
        tab.login(path='/dash/jobs')
        tab.page.wait_for_selector('#jobs-new-button', timeout=30000)
        tab.page.click('#jobs-new-button')
        tab.page.wait_for_selector('.modal.show #new-job-action', timeout=15000)
        ### The form starts on a schedule, and says what is missing instead of a command.
        _wait_text(tab.page, '#new-job-preview', 'Enter a schedule')
        assert tab.page.locator('#new-job-create-button').is_disabled()
        ### The selection lists the instance's pipes (never the internal `mrsm` ones).
        connectors = _dropdown_options(tab.page, 'new-job-connector-keys')
        assert 'sql:w7console' in connectors and 'mrsm' not in connectors, connectors
        _close_dropdown(tab.page)
        metrics = _dropdown_options(tab.page, 'new-job-metric-keys')
        assert {'w7parent', 'w7child'} <= set(metrics), metrics
        tab.page.locator('[role=option]', has_text='w7parent').first.click()
        _close_dropdown(tab.page)
        _wait_text(tab.page, '#new-job-preview', 'Enter a schedule')
        tab.page.click('#new-job-modal label:text-is("Continuously")')
        tab.page.fill('#new-job-min-seconds', '2')
        tab.page.press('#new-job-min-seconds', 'Tab')
        tab.page.fill('#new-job-name', name)
        tab.page.press('#new-job-name', 'Tab')
        _wait_text(
            tab.page, '#new-job-preview',
            f'mrsm sync pipes -m w7parent -i sql:w7console --loop --min-seconds 2 --name {name} -d',
        )
        tab.page.click('#new-job-create-button')
        tab.page.wait_for_selector('#new-job-modal.show', state='detached', timeout=30000)
        _wait_text(tab.page, '#jobs-alert-div', f"Created job '{name}'.")
        assert _job_exists(server, name)

        ### It is listed, running; with job health its Last Run says when it last synced.
        _job_unit(tab.page, name).wait_for(timeout=30000)
        tab.page.wait_for_function(
            "(name) => [...document.querySelectorAll('#jobs-table tr.job-row')].some("
            "tr => tr.innerText.includes(name) && tr.querySelector('.job-status').innerText === 'Running')",
            arg=name, timeout=30000,
        )
        ### Once the job's health reports a good lap, its Last Run says when.
        last_success = None
        deadline = time.time() + 45
        while time.time() < deadline and not last_success:
            probe = server.python(
                "import json\nfrom meerschaum.jobs import Job\n"
                f"try:\n    print('HEALTH=' + json.dumps(Job({name!r}, executor_keys='local').health))\n"
                "except Exception:\n    print('HEALTH=null')\n"
            )
            lines = [line for line in probe.stdout.splitlines() if line.startswith('HEALTH=')]
            health = json.loads(lines[-1][len('HEALTH='):]) if lines else None
            last_success = (health or {}).get('last_success')
            if health is None:
                break
            time.sleep(2)
        if last_success:
            tab.page.wait_for_function(
                "(name) => [...document.querySelectorAll('#jobs-table tr.job-row')].some("
                "tr => tr.innerText.includes(name)"
                " && /(just now|min ago)$/.test(tr.querySelector('.job-last-run').innerText)"
                " && tr.querySelector('.job-last-run').title.startsWith('Last sync ended'))",
                arg=name, timeout=60000,
            )

        _job_action(tab.page, name, 'Restart')
        _wait_text(tab.page, '#job-output-div', f"Restarted job '{name}'.", timeout=60000)

        ### Delete asks for the job's name.
        for attempt in range(3):
            _job_action(tab.page, name, 'Delete')
            try:
                tab.page.wait_for_selector('.modal.show input', timeout=15000)
                break
            except Exception:
                if attempt == 2:
                    raise
        confirm = tab.page.locator('.modal.show button:text-is("Delete")')
        assert confirm.is_disabled()
        tab.page.fill('.modal.show input', 'not-the-name')
        time.sleep(0.5)
        assert confirm.is_disabled()
        tab.page.fill('.modal.show input', name)
        tab.page.wait_for_function(
            "() => !document.querySelector('.modal.show button.btn-danger').disabled", timeout=10000,
        )
        confirm.click()
        tab.page.wait_for_selector('.modal.show', state='detached', timeout=30000)
        ### Gone from the server, then from the page (its refresh hides jobs that went away).
        deadline = time.time() + 30
        while time.time() < deadline and _job_exists(server, name):
            time.sleep(1)
        assert not _job_exists(server, name)
        _job_unit(tab.page, name).wait_for(state='hidden', timeout=30000)
        assert _errors(tab) == [], _errors(tab)
    finally:
        _cleanup_jobs(server, name)


def _row_geometry(page, name: str) -> dict:
    """A job row's name link, Status and its dot on the page (boxes are `[x, y, w, h]`)."""
    return page.evaluate(
        """(name) => {
            const link = [...document.querySelectorAll('#jobs-table .job-name-link')]
                .find(a => a.textContent === name);
            const row = link.closest('tr');
            const status = row.querySelector('.job-status');
            const box = el => { const r = el.getBoundingClientRect(); return [r.x, r.y, r.width, r.height]; };
            return {
                link: box(link), status: box(status),
                dot: getComputedStyle(status, '::before').backgroundColor,
                color: getComputedStyle(status).color,
                text: status.innerText,
            };
        }""",
        name,
    )


def test_jobs_page_is_a_table_with_a_filter(server, context):
    names = [f'w7-bulk-{i}' for i in range(9)]
    failed_name = 'w7-bulk-failed'
    try:
        for name in names:
            started = server.mrsm('show', 'version', '-d', '--name', name, '--yes', '--nopretty')
            assert started.returncode == 0, started.stdout + started.stderr
        ### A job whose run fails (its instance is not defined).
        started = server.mrsm(
            'show', 'pipes', '-i', 'sql:w7undefined', '-d', '--name', failed_name, '--yes', '--nopretty',
        )
        assert started.returncode == 0, started.stdout + started.stderr
        context.set_default_timeout(30000)
        page = context.new_page()
        page.set_viewport_size({'width': 390, 'height': 844})
        tab = Tab(page, server.base_url)
        tab.login(path='/dash/jobs')
        page.wait_for_selector('#jobs-table', timeout=30000)
        assert page.locator('#jobs-table tbody tr.job-row').count() >= len(names)
        ### On a phone each row is the job's name on one line (a 44 px link), its status under it.
        for name in names:
            row = _row_geometry(page, name)
            x, y, width, height = row['link']
            assert 44 <= height < 48 and width >= 44, (name, row)
            assert row['status'][1] >= y + height - 1, (name, row)
        ### A failed run's Status is "Failed" with a red dot, as on the Overview.
        page.wait_for_function(
            "(name) => [...document.querySelectorAll('#jobs-table tr.job-row')].some(tr => "
            "tr.innerText.includes(name) && tr.querySelector('.job-status').innerText === 'Failed')",
            arg=failed_name, timeout=60000,
        )
        failed = _row_geometry(page, failed_name)
        assert failed['dot'] == 'rgb(255, 123, 114)' and failed['color'] == 'rgb(255, 123, 114)', failed
        ### The filter box narrows the table.
        page.fill('#jobs-filter-input', 'w7-bulk-3')
        page.press('#jobs-filter-input', 'Enter')
        page.wait_for_function(
            "document.querySelectorAll('#jobs-table tbody tr.job-row').length === 1", timeout=30000,
        )
        ### Each row has a menu with the card's actions.
        page.locator('#jobs-table .job-row-menu-toggle').first.click()
        items = page.locator('.dropdown-menu.show .dropdown-item').all_inner_texts()
        assert items == ['Start', 'View logs', 'Download logs', 'Delete'], items
        box = page.locator('#jobs-table .job-row-menu-toggle').first.bounding_box()
        assert box['height'] >= 44 and box['width'] >= 44, box
        assert page.evaluate('document.documentElement.scrollWidth') <= 390
        page.keyboard.press('Escape')
        ### The Overview's Jobs tile links to the jobs in one state.
        tab.goto('/dash/jobs?status=stopped', '#jobs-table')
        page.wait_for_selector('#jobs-status-filter-clear:visible', timeout=30000)
        assert 'Stopped' in page.inner_text('#jobs-status-filter-clear')
        assert page.locator('#jobs-table tbody tr.job-row').count() >= len(names)
        page.click('#jobs-status-filter-clear')
        page.wait_for_selector('#jobs-status-filter-clear', state='hidden', timeout=30000)
        assert _errors(tab) == [], _errors(tab)
    finally:
        _cleanup_jobs(server, *names, failed_name)


@pytest.fixture(scope='module')
def pages_server():
    """A server of its own for the flows that change users and plugins."""
    instance = Server('pages', native=False)
    try:
        instance.start()
        registered = instance.python(
            "import meerschaum as mrsm\n"
            "from meerschaum.core import User\n"
            "conn = mrsm.get_connector('sql:w7console')\n"
            "for name, email in (('w7-bob', 'bob@example.com'), ('w7-carol', None)):\n"
            "    print(conn.register_user(User(name, 'correct-horse-2', email=email, instance=conn)))\n"
        )
        assert registered.returncode == 0, registered.stderr[-2000:]
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()


def _menu(page, username: str, item: str) -> None:
    row = page.locator(f'#users-table tbody tr:has(a:text-is("{username}"))')
    row.locator('.job-row-menu-toggle').click()
    page.click(f'.dropdown-menu.show .dropdown-item:text-is("{item}")')
    page.wait_for_selector('.modal.show', timeout=15000)


def _close_modal(page, cancel_id: str) -> None:
    page.click(f'#{cancel_id}')
    page.wait_for_selector('.modal.show', state='detached', timeout=15000)


def test_users_admin_flows_and_the_last_admin(pages_server, context):
    page = context.new_page()
    tab = Tab(page, pages_server.base_url)
    tab.login(path='/dash/users')
    page.wait_for_selector('#users-table', timeout=30000)
    _wait_text(page, '#users-table', 'bob@example.com')

    ### Add user.
    page.click('#users-add-button')
    page.wait_for_selector('.modal.show #users-add-username')
    page.fill('#users-add-username', 'w7-dave')
    page.fill('#users-add-password', 'abc')
    page.fill('#users-add-confirm', 'abc')
    page.click('#users-add-submit')
    _wait_text(page, '#users-add-alert', 'at least')
    page.fill('#users-add-password', 'correct-horse-4')
    page.fill('#users-add-confirm', 'correct-horse-4')
    page.fill('#users-add-email', 'dave@example.com')
    page.click('#users-add-submit')
    page.wait_for_selector('.modal.show', state='detached', timeout=15000)
    page.wait_for_selector('#users-table a:text-is("w7-dave")', timeout=15000)

    ### The only admin is neither demoted nor deleted.
    _menu(page, USERNAME, 'Edit')
    page.click('.modal.show label:text-is("User")')
    page.click('#users-edit-submit')
    _wait_text(page, '#users-edit-alert', 'only admin')
    _close_modal(page, 'users-edit-cancel')
    _menu(page, USERNAME, 'Delete')
    page.fill('#users-delete-input', USERNAME)
    page.click('#users-delete-submit')
    _wait_text(page, '#users-delete-alert', 'only admin')
    _close_modal(page, 'users-delete-cancel')

    ### Edit (the type), Reset password, Delete (confirmed by typing the username).
    _menu(page, 'w7-dave', 'Edit')
    page.click('.modal.show label:text-is("Admin")')
    page.click('#users-edit-submit')
    page.wait_for_selector('.modal.show', state='detached', timeout=15000)
    _wait_text(page, '#users-alert-div', "'w7-dave' is now an admin.")
    page.wait_for_selector('#users-table tbody tr:has(a:text-is("w7-dave")) .badge:text-is("admin")', timeout=15000)
    _menu(page, 'w7-bob', 'Reset password')
    page.fill('#users-password-value', 'brand-new-pass-1')
    page.fill('#users-password-confirm', 'brand-new-pass-1')
    page.click('#users-password-submit')
    page.wait_for_selector('.modal.show', state='detached', timeout=15000)
    _wait_text(page, '#users-alert-div', "Reset the password of 'w7-bob'.")
    _menu(page, 'w7-carol', 'Delete')
    assert page.locator('#users-delete-submit').is_disabled()
    page.fill('#users-delete-input', 'w7-carol')
    page.wait_for_function("!document.getElementById('users-delete-submit').disabled", timeout=10000)
    page.click('#users-delete-submit')
    page.wait_for_selector('.modal.show', state='detached', timeout=15000)
    page.wait_for_selector('#users-table a:text-is("w7-carol")', state='detached', timeout=15000)
    assert _errors(tab) == [], _errors(tab)

    ### A regular user signs in with the new password and sees the users read-only.
    other = context.browser.new_context()
    try:
        bob = Tab(other.new_page(), pages_server.base_url)
        bob.login('w7-bob', 'brand-new-pass-1', path='/dash/users')
        bob.page.wait_for_selector('#users-table', timeout=30000)
        assert bob.page.locator('#users-table .job-row-menu-toggle').count() == 0
        ### Email addresses are the administrators' to see.
        assert '@example.com' not in bob.page.inner_text('#users-table')
        assert bob.page.locator('#users-add-button-div').get_attribute('hidden') is not None
        assert _errors(bob) == [], _errors(bob)
    finally:
        other.close()


def _publish(server, plugin: str) -> None:
    """Publish a plugin to the server's repository from a terminal, as a plugin author does."""
    port = server.base_url.rsplit(':', 1)[1]
    published = subprocess.run(
        [str(mrsm_binary()), 'register', 'plugin', plugin, '-r', 'api:w7self', '--yes', '--noask', '--nopretty'],
        env={**server.env, 'MRSM_API_W7SELF': f"http://{USERNAME}:{PASSWORD}@127.0.0.1:{port}"},
        cwd=str(server.root), capture_output=True, text=True, timeout=300, stdin=subprocess.DEVNULL,
    )
    assert published.returncode == 0, published.stdout[-2000:] + published.stderr[-2000:]


def _publish_version(server, plugin: str, version: str) -> None:
    """
    Publish `version` of a plugin to the server's repository from a copy outside its plugins
    directory, so the installed plugin keeps its older version (RC25: Update shows only then).
    """
    import re
    source = server.root / 'publish' / version
    shutil.rmtree(source, ignore_errors=True)
    shutil.copytree(server.plugins / plugin, source / plugin, ignore=shutil.ignore_patterns('__pycache__'))
    init = source / plugin / '__init__.py'
    init.write_text(re.sub(r"__version__ = '[^']*'", f"__version__ = '{version}'", init.read_text()))
    port = server.base_url.rsplit(':', 1)[1]
    published = subprocess.run(
        [str(mrsm_binary()), 'register', 'plugin', plugin, '-r', 'api:w7self', '--yes', '--noask', '--nopretty'],
        env={**server.env, 'MRSM_PLUGINS_DIR': str(source),
             'MRSM_API_W7SELF': f"http://{USERNAME}:{PASSWORD}@127.0.0.1:{port}"},
        cwd=str(server.root), capture_output=True, text=True, timeout=300, stdin=subprocess.DEVNULL,
    )
    assert published.returncode == 0, published.stdout[-2000:] + published.stderr[-2000:]


def _plugin_run_result(page, *prefixes: str) -> str:
    """Wait until the plugin action's alert starts with one of `prefixes`, and return its text."""
    page.wait_for_function(
        "(prefixes) => { const el = document.querySelector('#plugins-run-alert'); "
        "return !!el && prefixes.some(prefix => el.innerText.startsWith(prefix)); }",
        arg=list(prefixes), timeout=180000,
    )
    return page.inner_text('#plugins-run-alert')


def test_plugins_installed_tab_uninstall_and_install(pages_server, context):
    server = pages_server
    _publish(server, 'w7_pyfig')

    page = context.new_page()
    tab = Tab(page, server.base_url)
    tab.login(path='/dash/plugins')
    page.wait_for_selector('#installed-plugins-table', timeout=30000)
    for name in ('w7_pyfig', 'w7_dashapp'):
        _wait_text(page, '#installed-plugins-table', name)
    row = page.locator('#installed-plugins-table tbody tr:has-text("w7_pyfig")')
    assert 'Python' in row.inner_text() and 'API routes' in row.inner_text()

    ### Uninstall asks first, runs as a job, and the table follows.
    ### RC25: Uninstall and Update are icon buttons named for their plugin.
    page.get_by_role('button', name='Uninstall w7_dashapp', exact=True).click()
    page.wait_for_selector('.modal.show #plugins-uninstall-confirm', timeout=15000)
    page.click('#plugins-uninstall-confirm')
    page.wait_for_function(
        "document.querySelector('#plugins-run-alert') && "
        "document.querySelector('#plugins-run-alert').innerText.startsWith('Uninstalled plugin')",
        timeout=180000,
    )
    page.wait_for_selector('#installed-plugins-table tbody tr:has-text("w7_dashapp")', state='detached', timeout=60000)

    ### The Repository tab installs a published plugin again (from this server).
    page.click('#plugins-tabs .nav-link:text-is("Repository")')
    page.wait_for_selector('#w7_pyfig_card button.plugin-card-install', timeout=30000)
    assert page.inner_text('#w7_pyfig_card button.plugin-card-install') == 'Reinstall'
    page.click('#w7_pyfig_card button.plugin-card-install')
    result = _plugin_run_result(page, 'Reinstalled plugin', 'Failed to reinstall plugin')
    assert result.startswith("Reinstalled plugin 'w7_pyfig'."), result
    ### Its output is one click away.
    page.click('#plugins-run-alert button:text-is("View output")')
    page.wait_for_function(
        "document.querySelector('#job-logs-view') && "
        "document.querySelector('#job-logs-view').textContent.includes('w7_pyfig')",
        timeout=30000,
    )
    page.keyboard.press('Escape')
    ### Installed from this repository, it offers Update only once the repository has a newer version.
    page.click('#plugins-tabs .nav-link:text-is("Installed")')
    update = page.get_by_role('button', name='Update w7_pyfig', exact=True)
    ### The update check has answered (a `dcc.Store` has no element to wait for).
    page.wait_for_selector('#installed-plugins-table .plugin-update-checked', state='attached', timeout=60000)
    assert not update.is_visible()
    _publish_version(server, 'w7_pyfig', '0.0.2')
    page.reload()
    update.wait_for(state='visible', timeout=60000)
    assert update.get_attribute('title') == 'Update w7_pyfig to 0.0.2'
    assert _errors(tab) == [], _errors(tab)


@pytest.fixture(scope='module')
def private_server():
    """A `--private` server: every route, plugin downloads included, asks for credentials."""
    instance = Server('private', args=['--private'], native=False)
    try:
        instance.start()
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()


def test_plugins_install_and_update_from_a_private_server(private_server, context):
    """
    On a `--private` server the install job reads this server's repository as the administrator
    who clicked (a short-lived token that may only read plugins): Reinstall and Update work.
    """
    server = private_server
    _publish(server, 'w7_pyfig')
    page = context.new_page()
    tab = Tab(page, server.base_url)
    tab.login(path='/dash/plugins')
    page.wait_for_selector('#installed-plugins-table', timeout=30000)
    page.click('#plugins-tabs .nav-link:text-is("Repository")')
    page.wait_for_selector('#w7_pyfig_card button.plugin-card-install', timeout=30000)
    page.click('#w7_pyfig_card button.plugin-card-install')
    result = _plugin_run_result(page, 'Reinstalled plugin', 'Failed to reinstall plugin')
    assert result.startswith("Reinstalled plugin 'w7_pyfig'."), result
    _publish_version(server, 'w7_pyfig', '0.0.2')
    page.click('#plugins-tabs .nav-link:text-is("Installed")')
    update = page.get_by_role('button', name='Update w7_pyfig', exact=True)
    update.wait_for(state='visible', timeout=60000)
    update.click()
    result = _plugin_run_result(page, 'Updated plugin', 'Failed to update plugin')
    assert result.startswith("Updated plugin 'w7_pyfig'."), result
    assert _errors(tab) == [], _errors(tab)


@pytest.mark.parametrize('path', ['/dash/jobs', '/dash/users', '/dash/plugins', '/dash/tokens'])
def test_pages_fit_a_phone(server, context, path):
    page = context.new_page()
    page.set_viewport_size({'width': 390, 'height': 844})
    tab = Tab(page, server.base_url)
    tab.login(path=path)
    page.wait_for_load_state('networkidle')
    time.sleep(1)
    assert page.evaluate('document.documentElement.scrollWidth') <= 390, path
    primary = {
        '/dash/jobs': '#jobs-new-button',
        '/dash/users': '#users-add-button',
        '/dash/tokens': '#tokens-create-button',
        '/dash/plugins': '#plugins-tabs .nav-link',
    }[path]
    box = page.locator(primary).first.bounding_box()
    assert box is not None and box['height'] >= 44, (path, box)
    if path == '/dash/users':
        page.wait_for_selector('#users-table .console-name-link', timeout=30000)
        link = page.locator('#users-table .console-name-link').first.bounding_box()
        assert link['height'] >= 44 and link['width'] >= 44, link
    assert _errors(tab) == [], _errors(tab)


def test_new_token_dialog_fits_a_phone(server, context):
    page = context.new_page()
    page.set_viewport_size({'width': 390, 'height': 844})
    tab = Tab(page, server.base_url)
    tab.login(path='/dash/tokens')
    page.click('#tokens-create-button')
    page.wait_for_selector('.modal.show #tokens-name-input', timeout=15000)
    label = page.inner_text('.modal.show .tokens-grant-all')
    assert '`' not in label and label.startswith('Grant all scopes'), label
    ### An administrator starts with "Grant all scopes" on; turn it off to see the scopes.
    page.click('#tokens-toggle-scopes-switch')
    page.wait_for_selector('.modal.show #tokens-scopes-checklist', state='visible', timeout=15000)
    geometry = page.evaluate("""() => {
        const box = el => { const r = el.getBoundingClientRect();
            return {x: r.x, y: r.y, w: r.width, h: r.height, right: r.right, bottom: r.bottom}; };
        const modal = document.querySelector('.modal.show');
        const body = modal.querySelector('.modal-body');
        return {
            dialog: box(modal.querySelector('.modal-dialog')),
            name: box(document.getElementById('tokens-name-input')),
            expiration: box(modal.querySelector('.tokens-expiration')),
            picker: box(modal.querySelector('.tokens-expiration .dash-datepicker')),
            clear: box(modal.querySelector('.tokens-expiration .dash-datepicker-clear')),
            body: [body.scrollWidth, body.clientWidth],
            items: [...modal.querySelectorAll('#tokens-scopes-checklist .form-check')].map(box),
            parts: [...modal.querySelectorAll(
                '#tokens-scopes-checklist .form-check-input, #tokens-scopes-checklist .form-check-label'
            )].map(el => ({text: el.textContent, ...box(el)})),
        };
    }""")
    assert geometry['dialog']['w'] <= 390 and geometry['name']['w'] >= 300, geometry
    assert geometry['expiration']['right'] <= 390, geometry['expiration']
    assert geometry['picker']['h'] >= 44 and geometry['clear']['w'] >= 44, geometry
    assert geometry['body'][0] <= geometry['body'][1], geometry['body']
    items, parts = geometry['items'], geometry['parts']
    assert len(items) > 10 and all(item['right'] <= 390 and item['h'] >= 44 for item in items), items
    ### No scope's label runs into another column's checkbox or label.
    for i, a in enumerate(parts):
        assert a['right'] <= 390, a
        for b in parts[i + 1:]:
            overlap = a['x'] < b['right'] - 1 and b['x'] < a['right'] - 1 and a['y'] < b['bottom'] - 1 and b['y'] < a['bottom'] - 1
            assert not overlap, (a, b)
    assert _errors(tab) == [], _errors(tab)


def test_token_menu_closes_when_edit_opens_on_a_phone(server, context):
    ### The token exists before the page loads (the page refreshes its table after the New token
    ### dialogs close, which would replace an open menu).
    registered = server.python(
        "import meerschaum as mrsm\n"
        "from meerschaum.core import Token, User\n"
        "conn = mrsm.get_connector('sql:w7console')\n"
        f"user = User({USERNAME!r}, instance=conn)\n"
        "token = Token(label='w7-phone-token', user=user, scopes=['pipes:read'], instance=conn)\n"
        "success, message = token.register()\n"
        "assert success, message\n"
    )
    assert registered.returncode == 0, registered.stderr[-2000:]
    page = context.new_page()
    page.set_viewport_size({'width': 390, 'height': 844})
    tab = Tab(page, server.base_url)
    tab.login(path='/dash/tokens')
    ### Its menu's Edit opens the dialog and the menu goes away.
    row = page.locator('#tokens-output-div tr:has-text("w7-phone-token")')
    row.wait_for(timeout=30000)
    row.locator('[id*="tokens-context-button"]').click()
    page.wait_for_selector('.popover button:text-is("Edit")', timeout=15000)
    page.locator('.popover button:text-is("Edit")').click()
    page.wait_for_selector('.modal.show [id*="tokens-name-input"]', timeout=15000)
    page.wait_for_selector('.popover', state='hidden', timeout=15000)
    dialog = page.locator('.modal.show .modal-dialog').bounding_box()
    name = page.locator('.modal.show [id*="tokens-name-input"]').bounding_box()
    assert dialog['width'] <= 390 and name['width'] >= 300, (dialog, name)
    assert page.evaluate('document.documentElement.scrollWidth') <= 390
    assert _errors(tab) == [], _errors(tab)
