#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The plugin SDK's browser-side behavior (rc15), in Chromium against the Python API host served
directly by uvicorn (no Rust server needed: the host serves `/dash`, `/static` and `/_pages`):

- `@web_page(theme=..., head=[...])`: the page's stylesheet replaces the console's theme while the
  page is open (on load and by in-app navigation), its meta tags come and go with it, its script
  runs once;
- a callback that needs a session answers `401`, and the browser goes to the login page and,
  after signing in, back to the page, where the callback now runs.

Opt-in like the rest of `tests/console` (`MRSM_TEST_CONSOLE=1`).
"""

from __future__ import annotations

import json
import os
import pathlib
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import textwrap
import time
import urllib.request

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]
USERNAME = 'sdkuser'
PASSWORD = 'sdk-password-1'
CUSTOM_THEME = '/static/css/bootstrap_light.min.css?sdk=1'

PLUGIN = '''
from meerschaum.plugins import web_page, web_callback
from dash import Input, Output, State, html, dcc


@web_page(
    'sdk/public',
    login_required=False,
    theme=%(theme)r,
    head=[
        {'tag': 'meta', 'name': 'sdk-page', 'content': '1'},
        '<script>window.sdkScriptRuns = (window.sdkScriptRuns || 0) + 1;</script>',
    ],
)
def public_page():
    return html.Div([
        html.H3('SDK public page', id='sdk-title'),
        dcc.Link('Plain page', href='/dash/sdk/plain', id='sdk-to-plain'),
        html.Div(id='sdk-public-out'),
    ])


@web_page('sdk/plain', login_required=False)
def plain_page():
    return html.Div([
        html.H3('SDK plain page', id='sdk-plain-title'),
        dcc.Link('Back', href='/dash/sdk/public', id='sdk-to-public'),
    ])


@web_callback(Output('sdk-public-out', 'children'), Input('sdk-title', 'children'), public=True)
def public_callback(title):
    return 'public callback ran'


@web_page('sdk/needs-session', login_required=False)
def needs_session():
    return html.Div([html.H3('Needs a session'), html.Div(id='sdk-private-out')])


@web_callback(
    Output('sdk-private-out', 'children'),
    Input('sdk-private-out', 'id'),
    State('session-store', 'data'),
)
def private_callback(_component_id, session_store):
    return 'private callback ran'
''' % {'theme': CUSTOM_THEME}

SERVER = '''
import sys
import uvicorn
from meerschaum.api.host import build_app
from meerschaum.api import get_api_connector
from meerschaum.core import User

app = build_app()
conn = get_api_connector()
conn.register_user(User(%(username)r, %(password)r, instance=conn))
uvicorn.run(app, host='127.0.0.1', port=int(sys.argv[1]), log_level='warning')
''' % {'username': USERNAME, 'password': PASSWORD}


def _free_port() -> int:
    ports = list(range(19000, 20000))
    random.shuffle(ports)
    for port in ports:
        with socket.socket() as probe:
            try:
                probe.bind(('127.0.0.1', port))
            except OSError:
                continue
        return port
    raise RuntimeError("No free port in 19000-19999.")


@pytest.fixture(scope='module')
def sdk_server():
    for module in ('dash', 'dash_bootstrap_components', 'dash_ace', 'a2wsgi', 'uvicorn', 'fastapi'):
        pytest.importorskip(module)
    scratch = pathlib.Path.home() / '.cache' / 'mrsm-console-sdk-tests'
    scratch.mkdir(parents=True, exist_ok=True)
    base = pathlib.Path(tempfile.mkdtemp(prefix='sdk-', dir=scratch))
    root, plugins = base / 'root', base / 'plugins'
    (plugins / 'sdkplugin').mkdir(parents=True)
    root.mkdir()
    (plugins / 'sdkplugin' / '__init__.py').write_text(textwrap.dedent(PLUGIN))
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE', 'PYTHONPATH')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_API_HOST': '1',
        'MRSM_SQL_SDKTEST': f"sqlite:///{root / 'sdk.db'}",
        'MRSM_API_HOST_CONFIG': json.dumps({'mrsm_instance': 'sql:sdktest'}),
    })
    port = _free_port()
    log = open(base / 'server.log', 'w+', encoding='utf-8')
    process = subprocess.Popen(
        [sys.executable, '-c', SERVER, str(port)],
        env=env, cwd=str(base), stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
    )
    base_url = f'http://127.0.0.1:{port}'
    try:
        deadline = time.time() + 120
        while True:
            if process.poll() is not None:
                log.seek(0)
                pytest.fail(f"The host exited:\n{log.read()[-4000:]}")
            try:
                with urllib.request.urlopen(base_url + '/dash/', timeout=2) as response:
                    if response.status == 200:
                        break
            except Exception:
                pass
            if time.time() > deadline:
                pytest.fail("The host did not start in time.")
            time.sleep(0.5)
        yield base_url
    finally:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
        log.close()
        shutil.rmtree(base, ignore_errors=True)


def _theme_hrefs(page):
    return page.evaluate(
        "['mrsm-theme-dark', 'mrsm-theme-light'].map(id => document.getElementById(id).getAttribute('href'))"
    )


def test_page_theme_and_head_follow_navigation(sdk_server, browser):
    context = browser.new_context()
    page = context.new_page()
    try:
        page.goto(sdk_server + '/dash/sdk/public')
        page.wait_for_selector('#sdk-title', timeout=30000)
        page.wait_for_function(
            "document.getElementById('sdk-public-out') && "
            "document.getElementById('sdk-public-out').textContent === 'public callback ran'",
            timeout=30000,
        )
        assert _theme_hrefs(page) == [CUSTOM_THEME, CUSTOM_THEME]
        assert page.evaluate("document.querySelectorAll('meta[name=sdk-page]').length") == 1
        assert page.evaluate("window.sdkScriptRuns") == 1

        page.click('#sdk-to-plain')
        page.wait_for_selector('#sdk-plain-title', timeout=30000)
        assert _theme_hrefs(page) == [
            '/static/css/bootstrap.min.css', '/static/css/bootstrap_light.min.css',
        ]
        assert page.evaluate("document.querySelectorAll('meta[name=sdk-page]').length") == 0

        page.click('#sdk-to-public')
        page.wait_for_selector('#sdk-title', timeout=30000)
        assert _theme_hrefs(page) == [CUSTOM_THEME, CUSTOM_THEME]
        assert page.evaluate("document.querySelectorAll('meta[name=sdk-page]').length") == 1
        assert page.evaluate("window.sdkScriptRuns") == 1
    finally:
        context.close()


def test_a_callback_without_a_session_sends_the_browser_to_the_login_page(sdk_server, browser):
    context = browser.new_context()
    page = context.new_page()
    try:
        page.goto(sdk_server + '/dash/sdk/needs-session')
        page.wait_for_url('**/dash/login?next=*', timeout=30000)
        assert page.url.endswith('/dash/login?next=%2Fdash%2Fsdk%2Fneeds-session')
        page.wait_for_selector('#username-input', timeout=30000)
        page.fill('#username-input', USERNAME)
        page.fill('#password-input', PASSWORD)
        page.click('#login-button')
        page.wait_for_url('**/dash/sdk/needs-session', timeout=30000)
        page.wait_for_function(
            "document.getElementById('sdk-private-out') && "
            "document.getElementById('sdk-private-out').textContent === 'private callback ran'",
            timeout=30000,
        )
        cookies = {cookie['name']: cookie for cookie in context.cookies()}
        assert cookies['mrsm-session-id']['httpOnly'] is True
    finally:
        context.close()
