#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
End-to-end checks of the web terminal (opt-in, see `conftest.py`).

The Rust server serves the terminal itself (`meerschaum_server::webterm`): the console embeds it at
`/dash/webterm`, the standalone page is `/webterm/<session>`, and both talk to
`/websocket/<session>`, which bridges the tab to a PTY running `mrsm -i <instance>`. The terminal
tests run twice when `tmux` is installed: once on the tmux path (`api.webterm.tmux.enabled`, the
default; reloads and second tabs reattach to the user's tmux session) and once on the plain path
(one shell per tab). `tmux` runs on a private socket directory, never the user's own server.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
import tempfile

import pytest

from tests.console.conftest import REPO, Tab, Server, USERNAME, PASSWORD

pytestmark = pytest.mark.console

ASSETS = REPO / 'crates' / 'meerschaum-web' / 'assets'
VERSIONS = ASSETS / 'versions.json'
HAS_TMUX = shutil.which('tmux') is not None
VERSION_PATTERN = re.compile(r'\d+\.\d+\.\d+\S*')


def _vendored_version(asset: str, package: str) -> str:
    url = json.loads(VERSIONS.read_text())[asset]
    return url.split(f'{package}@', 1)[1].split('/', 1)[0]


@pytest.fixture(scope='module', params=['tmux', 'plain', 'no-tmux-binary'])
def term_server(request):
    """A server with the terminal on, on the tmux path or the plain path."""
    use_tmux = request.param == 'tmux'
    ### `no-tmux-binary`: tmux stays enabled in the configuration but is not on the server's PATH
    ### (nothing is uninstalled), as on a host that never had it.
    tmux_enabled = request.param != 'plain'
    if use_tmux and not HAS_TMUX:
        pytest.skip("tmux is not installed; the plain path covers this host.")
    instance = Server(
        f'term-{request.param}',
        config={'api': {'webterm': {'tmux': {'enabled': tmux_enabled}}}},
        native=False,
        webterm=True,
    )
    instance.path = 'tmux' if use_tmux else 'plain'
    ### A private tmux server: its socket directory lives under a short temporary path.
    tmux_dir = tempfile.mkdtemp(prefix='mt-', dir='/tmp')
    instance.env['TMUX_TMPDIR'] = tmux_dir
    ### Every tmux command the tests run addresses this socket explicitly (`-S`).
    instance.tmux_socket = str(pathlib.Path(tmux_dir) / f"tmux-{os.getuid()}" / 'default')
    if request.param == 'no-tmux-binary':
        bin_dir = pathlib.Path(tmux_dir) / 'bin'
        bin_dir.mkdir()
        for tool in ('sh', 'env', 'python3', 'python'):
            found = shutil.which(tool)
            if found:
                (bin_dir / tool).symlink_to(found)
        instance.env['PATH'] = str(bin_dir)
        assert shutil.which('tmux', path=str(bin_dir)) is None
    try:
        instance.start()
    except Exception:
        instance.stop()
        shutil.rmtree(tmux_dir, ignore_errors=True)
        raise
    yield instance
    if use_tmux:
        ### Address the private server's socket explicitly, never a bare `kill-server`.
        subprocess.run(
            ['tmux', '-S', instance.tmux_socket, 'kill-server'],
            env={k: v for k, v in instance.env.items() if k not in ('TMUX', 'TMUX_PANE')},
            capture_output=True, timeout=20,
        )
    instance.stop()
    shutil.rmtree(tmux_dir, ignore_errors=True)


def _mrsm_version(server: Server) -> str:
    out = server.mrsm('show', 'version').stdout
    match = VERSION_PATTERN.search(re.sub(r'\x1b\[[0-9;]*m', '', out))
    assert match, out
    return match.group(0)


def _login(context, server: Server) -> tuple:
    page = context.new_page()
    tab = Tab(page, server.base_url)
    tab.login()
    cookie = [c for c in context.cookies() if c['name'] == 'mrsm-session-id'][0]['value']
    return page, tab, cookie


def _open_terminal(page, base: str, path: str = '/dash/webterm') -> None:
    page.goto(f"{base}{path}")
    page.wait_for_selector('.xterm .xterm-screen', timeout=30000)
    page.wait_for_selector('.xterm-rows > div', timeout=30000)
    ### The shell's banner arrives through the websocket and xterm paints it.
    page.wait_for_function(
        "document.querySelector('.xterm-rows').innerText.trim().length > 0",
        timeout=90000,
    )


def _rows(page) -> str:
    return page.evaluate("document.querySelector('.xterm-rows').innerText")


def _wait_for_text(page, text: str, occurrences: int = 1, timeout: int = 60000) -> None:
    """Wait until `text` is painted at least `occurrences` times; on failure show the rows."""
    try:
        page.wait_for_function(
            "([t, n]) => document.querySelector('.xterm-rows').innerText.split(t).length - 1 >= n",
            arg=[text, occurrences],
            timeout=timeout,
        )
    except Exception as e:
        raise AssertionError(f"{text!r} x{occurrences} never painted: {e}\nrows:\n{_rows(page)}") from None


def _focus_terminal(page) -> None:
    page.click('.xterm-screen')
    page.wait_for_function(
        "document.activeElement && document.activeElement.classList.contains('xterm-helper-textarea')"
    )


def test_vendored_xterm_is_served_locally(server, context):
    page = context.new_page()
    for asset, package in (
        ('xterm.js', '@xterm/xterm'),
        ('addon-fit.js', '@xterm/addon-fit'),
        ('addon-unicode11.js', '@xterm/addon-unicode11'),
    ):
        version = _vendored_version(asset, package)
        response = page.request.get(f"{server.base_url}/dash/assets/{asset}")
        assert response.status == 200, (asset, response.status)
        assert response.body() == (ASSETS / asset).read_bytes(), (
            f"served {asset} differs from the vendored {version} asset"
        )
    assert _vendored_version('xterm.js', '@xterm/xterm') == '6.0.0'
    for name in ('xterm.css', 'webterm.js', 'webterm.css'):
        assert page.request.get(f"{server.base_url}/dash/assets/{name}").status == 200, name
    for name in ('xterm.LICENSE', 'addon-fit.LICENSE', 'addon-unicode11.LICENSE'):
        assert (ASSETS / name).read_text().strip(), name


def test_without_the_flag_there_is_no_terminal(server, context):
    """The shared `server` runs `--no-webterm`: neither route is served, nor proxied to the host."""
    page = context.new_page()
    tab = Tab(page, server.base_url)
    tab.login()
    cookie = [c for c in context.cookies() if c['name'] == 'mrsm-session-id'][0]['value']
    assert page.request.get(f"{server.base_url}/webterm/{cookie}").status == 404
    assert page.request.get(f"{server.base_url}/websocket/{cookie}").status == 404
    page.goto(f"{server.base_url}/dash/webterm")
    assert 'not configured' in page.inner_text('main')
    assert page.locator('[data-mrsm-webterm]').count() == 0
    assert 'Terminal' not in page.inner_text('nav')


def test_terminal_requires_a_logged_in_administrator(term_server, context):
    base = term_server.base_url
    page = context.new_page()
    response = page.request.get(f"{base}/webterm/not-a-session")
    assert response.status == 401
    assert 'Unauthorized' in response.text() and '/dash/login' in response.text()
    ### The websocket is accepted and closed, as in v4.
    page.goto(f"{base}/dash/login")
    closed = page.evaluate(
        """() => new Promise((resolve) => {
            const socket = new WebSocket(`ws://${location.host}/websocket/not-a-session`);
            socket.onclose = (e) => resolve({code: e.code, clean: e.wasClean});
            socket.onmessage = (e) => resolve({message: e.data});
        })"""
    )
    assert closed == {'code': 1000, 'clean': True}, closed
    ### The console page without a session redirects to the login page.
    anonymous = context.new_page()
    anonymous.goto(f"{base}/dash/webterm")
    assert anonymous.url == f"{base}/dash/login"


def test_show_version_in_the_console_terminal(term_server, context):
    """Log in, open `/dash/webterm`, type `show version` and read the version out of the xterm rows."""
    base = term_server.base_url
    version = _mrsm_version(term_server)
    page, tab, cookie = _login(context, term_server)
    _open_terminal(page, base)
    try:
        assert page.evaluate("typeof Terminal") == 'function'
        assert page.evaluate("window.terminal.term.options.allowProposedApi") is True
        assert page.evaluate("window.terminal.term.unicode.activeVersion") == '11'
        assert page.locator('.mrsm-webterm-screen').get_attribute('data-ws') == f"/websocket/{cookie}"
        ### The new terminal is inside the console chrome, with its nav.
        assert page.locator('nav[aria-label=Console]').count() == 1
        ### Typed keys leave through the socket as terminado `["stdin", ...]` frames.
        page.evaluate(
            "window.__sent = []; const s = window.terminal.socket; const o = s.send.bind(s);"
            " s.send = (d) => { window.__sent.push(d); o(d); }; true;"
        )
        before = _rows(page).count(version)
        _focus_terminal(page)
        page.keyboard.type('show version')
        page.keyboard.press('Enter')
        frames = [json.loads(f) for f in page.evaluate("window.__sent")]
        typed = ''.join(f[1] for f in frames if f[0] == 'stdin')
        assert 'show version' in typed, typed
        ### The shell's answer (not the echo of the command) carries the version.
        page.wait_for_function(
            "([version, before]) => document.querySelector('.xterm-rows').innerText.split(version).length - 1 > before",
            arg=[version, before],
            timeout=60000,
        )
        ### The fit addon sized the PTY: a `set_size` frame went out with the terminal's dimensions.
        dims = page.evaluate("[window.terminal.term.rows, window.terminal.term.cols]")
        assert dims[0] > 2 and dims[1] > 20, dims
    except Exception as e:
        raise AssertionError(
            f"{term_server.path}: {e}\nrows:\n{_rows(page)}\nerrors: {tab.errors}\n{term_server.log()}"
        ) from None
    assert [e for e in tab.errors if not e.startswith('console.error: Failed to load')] == [], tab.errors


def test_standalone_page_matches_the_v4_route(term_server, context):
    """`/webterm/<session>` (the v4 page) renders the same terminal without the console chrome."""
    base = term_server.base_url
    page, tab, cookie = _login(context, term_server)
    response = page.request.get(f"{base}/webterm/{cookie}")
    assert response.status == 200
    assert 'Meerschaum Shell' in response.text()
    assert response.headers['referrer-policy'] == 'no-referrer'
    _open_terminal(page, base, f"/webterm/{cookie}")
    assert page.locator('nav').count() == 0
    assert page.evaluate("window.terminal.term.unicode.activeVersion") == '11'
    assert page.evaluate("window.terminal.socket.readyState") == 1
    ### Resizing the viewport refits the terminal and tells the PTY.
    page.evaluate(
        "window.__sizes = []; const s = window.terminal.socket; const o = s.send.bind(s);"
        " s.send = (d) => { window.__sizes.push(d); o(d); }; true;"
    )
    cols = page.evaluate("window.terminal.term.cols")
    page.set_viewport_size({'width': 640, 'height': 480})
    page.wait_for_function(f"window.terminal.term.cols !== {cols}", timeout=15000)
    page.wait_for_function(
        "window.__sizes.some((d) => JSON.parse(d)[0] === 'set_size')", timeout=15000,
    )


def test_extra_key_row_on_a_narrow_screen(term_server, browser):
    """v4's Termux-style keys: arrows send escape sequences, CTRL is sticky, focus stays in the terminal."""
    base = term_server.base_url
    context = browser.new_context(viewport={'width': 390, 'height': 800}, has_touch=True)
    try:
        page, tab, _ = _login(context, term_server)
        _open_terminal(page, base)
        keys = page.locator('.mrsm-webterm-keys')
        assert keys.is_visible()
        page.evaluate(
            "window.__sent = []; const s = window.terminal.socket; const o = s.send.bind(s);"
            " s.send = (d) => { window.__sent.push(JSON.parse(d)); o(d); }; true;"
        )
        _focus_terminal(page)
        for key, sequence in (('up', '\x1b[A'), ('left', '\x1b[D'), ('esc', '\x1b'), ('tab', '\t')):
            page.click(f'[data-key={key}]')
            assert ['stdin', sequence] in page.evaluate("window.__sent"), key
            ### Tapping a key button keeps the terminal's textarea focused (the soft keyboard stays up).
            assert page.evaluate("document.activeElement.classList.contains('xterm-helper-textarea')")
        page.click('[data-key=ctrl]')
        assert page.locator('[data-key=ctrl]').get_attribute('aria-pressed') == 'true'
        page.keyboard.press('c')
        assert ['stdin', '\x03'] in page.evaluate("window.__sent")
        assert page.locator('[data-key=ctrl]').get_attribute('aria-pressed') == 'false'
        page.click('[data-key=shift]')
        page.keyboard.press('a')
        assert ['stdin', 'A'] in page.evaluate("window.__sent")
        ### The tmux path shares one shell across the module's tests: do not leave `A` on its line.
        page.keyboard.press('Backspace')
        assert ['stdin', '\x7f'] in page.evaluate("window.__sent")
        assert [e for e in tab.errors if not e.startswith('console.error: Failed to load')] == [], tab.errors
    finally:
        context.close()


def test_the_key_row_is_hidden_with_room_for_a_keyboard(term_server, context):
    page, tab, _ = _login(context, term_server)
    page.set_viewport_size({'width': 1280, 'height': 800})
    _open_terminal(page, term_server.base_url)
    assert not page.locator('.mrsm-webterm-keys').is_visible()
    ### Theme follows the console: toggling it repaints the terminal's background.
    background = page.evaluate("window.terminal.term.options.theme.background")
    page.click('#theme-toggle')
    page.wait_for_function(
        "(was) => window.terminal.term.options.theme.background !== was", arg=background,
    )
    ### Fullscreen toggles the console column and refits.
    page.click('[data-webterm-action=fullscreen]')
    assert page.evaluate("document.querySelector('.mrsm-webterm').classList.contains('mrsm-webterm-fullscreen')")
    page.click('[data-webterm-action=fullscreen]')
    assert not page.evaluate("document.querySelector('.mrsm-webterm').classList.contains('mrsm-webterm-fullscreen')")
    ### The new-window control is the tmux path's.
    assert (page.locator('[data-webterm-action=new-window]').count() == 1) == (term_server.path == 'tmux')


def test_two_sessions_in_two_contexts(term_server, browser):
    """Two logins of one user, in two browser contexts (v4's second login returned a 500)."""
    base = term_server.base_url
    first_context, second_context = browser.new_context(), browser.new_context()
    try:
        first, first_tab, first_cookie = _login(first_context, term_server)
        second, second_tab, second_cookie = _login(second_context, term_server)
        assert first_cookie != second_cookie
        _open_terminal(first, base)
        _open_terminal(second, base)
        ### The shell repaints the screen after each command, so only the latest answer shows.
        for page, marker in ((first, 'echo-marker-one'), (second, 'echo-marker-two')):
            _focus_terminal(page)
            page.keyboard.type(f"echo {marker}")
            page.keyboard.press('Enter')
            _wait_for_text(page, marker)
            if term_server.path == 'tmux':
                ### The same user's tabs attach to one tmux session: the other tab mirrors it.
                _wait_for_text(second if page is first else first, marker)
        if term_server.path != 'tmux':
            ### One shell per tab: neither sees the other's command.
            assert 'echo-marker-two' not in _rows(first)
            assert 'echo-marker-one' not in _rows(second)
        for tab in (first_tab, second_tab):
            assert [e for e in tab.errors if not e.startswith('console.error: Failed to load')] == []
    finally:
        first_context.close()
        second_context.close()


@pytest.mark.skipif(not HAS_TMUX, reason="tmux is not installed")
def test_reload_reattaches_to_the_tmux_session(term_server, context):
    if term_server.path != 'tmux':
        pytest.skip("the plain path starts a new shell on every connection")
    version = _mrsm_version(term_server)
    page, tab, _ = _login(context, term_server)
    _open_terminal(page, term_server.base_url)
    _focus_terminal(page)
    page.keyboard.type('show version')
    page.keyboard.press('Enter')
    ### (The version is only in the shell's answer, never in the typed command.)
    _wait_for_text(page, version)
    ### Reloading closes the PTY (and the tmux client) but not the session: the screen comes back
    ### with the command and its answer, without typing anything.
    page.reload()
    page.wait_for_selector('.xterm-rows > div', timeout=30000)
    _wait_for_text(page, version)
    ### The session is named after the user, with the configured suffix.
    listing = subprocess.run(
        ['tmux', '-S', term_server.tmux_socket, 'list-sessions', '-F', '#{session_name}'],
        env={k: v for k, v in term_server.env.items() if k not in ('TMUX', 'TMUX_PANE')}, capture_output=True, text=True, timeout=20,
    ).stdout.split()
    assert f"{USERNAME}_mrsm" in listing, listing
