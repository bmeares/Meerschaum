#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
End-to-end checks of the web terminal (opt-in, see `conftest.py`).

The Rust server serves the terminal itself (`meerschaum_server::webterm`) at `/webterm/<session>`
(and its websocket at `/websocket/<session>`, which bridges the tab to a PTY running
`mrsm -i <instance>`); the Dash console embeds that page in an iframe on the dashboard
(also reached through the old `/dash/webterm` URL), with v4's controls (refresh, fullscreen,
new tmux window) and the mobile key row,
which talk to the terminal over `postMessage`. The terminal tests run twice when `tmux` is
installed: once on the tmux path (`api.webterm.tmux.enabled`, the default; reloads and second tabs
reattach to the user's tmux session) and once on the plain path (one shell per tab). `tmux` runs
on a private socket directory, never the user's own server.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import shlex
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
    tmux_dir = tempfile.mkdtemp(prefix='mt-')
    instance.env['TMUX_TMPDIR'] = tmux_dir
    ### Every tmux command the tests run addresses this socket explicitly (`-S`).
    instance.tmux_socket = str(pathlib.Path(tmux_dir) / 'socket')
    if use_tmux:
        bin_dir = pathlib.Path(tmux_dir) / 'bin'
        bin_dir.mkdir()
        wrapper = bin_dir / 'tmux'
        wrapper.write_text(
            '#!/bin/sh\nexec ' + shlex.quote(shutil.which('tmux'))
            + ' -S ' + shlex.quote(instance.tmux_socket) + ' "$@"\n'
        )
        wrapper.chmod(0o700)
        instance.env['PATH'] = str(bin_dir) + os.pathsep + instance.env['PATH']
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


def _open_terminal(page, base: str, path: str = '/dash/webterm'):
    """
    Open a terminal and return the frame holding it: the page itself for the standalone
    `/webterm/<session>`, the console's iframe for a console page.
    """
    page.goto(f"{base}{path}")
    if path.startswith('/webterm/'):
        frame = page.main_frame
    else:
        page.wait_for_selector('#webterm-iframe', timeout=60000)
        page.locator('#webterm-iframe').scroll_into_view_if_needed()
        frame = page.query_selector('#webterm-iframe').content_frame()
    frame.wait_for_selector('.xterm .xterm-screen', timeout=30000)
    frame.wait_for_selector('.xterm-rows > div', timeout=30000)
    ### The shell's banner arrives through the websocket and xterm paints it.
    frame.wait_for_function(
        "document.querySelector('.xterm-rows').innerText.trim().length > 0",
        timeout=90000,
    )
    return frame


def _rows(frame) -> str:
    return frame.evaluate("document.querySelector('.xterm-rows').innerText")


def _wait_for_text(frame, text: str, occurrences: int = 1, timeout: int = 60000) -> None:
    """Wait until `text` is painted at least `occurrences` times; on failure show the rows."""
    try:
        frame.wait_for_function(
            "([t, n]) => document.querySelector('.xterm-rows').innerText.split(t).length - 1 >= n",
            arg=[text, occurrences],
            timeout=timeout,
        )
    except Exception as e:
        raise AssertionError(f"{text!r} x{occurrences} never painted: {e}\nrows:\n{_rows(frame)}") from None


def _focus_terminal(frame) -> None:
    frame.click('.xterm-screen')
    frame.wait_for_function(
        "document.activeElement && document.activeElement.classList.contains('xterm-helper-textarea')"
    )


def test_vendored_xterm_is_served_locally(webterm_server, context):
    server = webterm_server
    page = context.new_page()
    for asset, package in (
        ('xterm.js', '@xterm/xterm'),
        ('addon-fit.js', '@xterm/addon-fit'),
        ('addon-unicode11.js', '@xterm/addon-unicode11'),
    ):
        version = _vendored_version(asset, package)
        response = page.request.get(f"{server.base_url}/webterm/assets/{asset}")
        assert response.status == 200, (asset, response.status)
        assert response.body() == (ASSETS / asset).read_bytes(), (
            f"served {asset} differs from the vendored {version} asset"
        )
    assert _vendored_version('xterm.js', '@xterm/xterm') == '6.0.0'
    for name in ('xterm.css', 'webterm.js', 'webterm.css'):
        assert page.request.get(f"{server.base_url}/webterm/assets/{name}").status == 200, name
    for name in ('xterm.LICENSE', 'addon-fit.LICENSE', 'addon-unicode11.LICENSE'):
        assert (ASSETS / name).read_text().strip(), name


def test_without_the_flag_there_is_no_terminal(server, context):
    """The shared `server` runs `--no-webterm`: neither route is served, nor proxied to the host."""
    tab = Tab(context.new_page(), server.base_url)
    tab.login()
    page = tab.page
    cookie = [c for c in context.cookies() if c['name'] == 'mrsm-session-id'][0]['value']
    assert page.request.get(f"{server.base_url}/webterm/{cookie}").status == 404
    assert page.request.get(f"{server.base_url}/websocket/{cookie}").status == 404
    tab.goto('/dash/webterm', '#go-button')
    assert page.locator('#webterm-iframe').count() == 0
    ### v4's banner takes the terminal's place on the dashboard.
    tab.goto('/dash', '#go-button')
    assert page.locator('#webterm-iframe').count() == 0


def test_terminal_requires_a_logged_in_administrator(term_server, context):
    base = term_server.base_url
    page = context.new_page()
    response = page.request.get(f"{base}/webterm/not-a-session")
    assert response.status == 401
    assert 'Unauthorized' in response.text() and '/dash/login' in response.text()
    ### The websocket is accepted and closed, as in v4.
    page.goto(f"{base}/version")
    closed = page.evaluate(
        """() => new Promise((resolve) => {
            const socket = new WebSocket(`ws://${location.host}/websocket/not-a-session`);
            socket.onclose = (e) => resolve({code: e.code, clean: e.wasClean});
            socket.onmessage = (e) => resolve({message: e.data});
        })"""
    )
    assert closed == {'code': 1000, 'clean': True}, closed
    ### The console page without a session is the login page.
    anonymous = Tab(context.new_page(), base)
    anonymous.goto('/dash/webterm', '#username-input')


def test_show_version_in_the_console_terminal(term_server, context):
    """Log in, open `/dash/webterm`, type `show version` and read the version out of the xterm rows."""
    base = term_server.base_url
    version = _mrsm_version(term_server)
    page, tab, cookie = _login(context, term_server)
    frame = _open_terminal(page, base)
    try:
        assert page.get_attribute('#webterm-iframe', 'src') == f"/webterm/{cookie}"
        assert frame.evaluate("typeof Terminal") == 'function'
        assert frame.evaluate("window.terminal.term.options.allowProposedApi") is True
        assert frame.evaluate("window.terminal.term.unicode.activeVersion") == '11'
        assert frame.locator('.mrsm-webterm-screen').get_attribute('data-ws') == f"/websocket/{cookie}"
        ### The terminal sits in the console's chrome, with its navbar.
        assert page.locator('#logo-img').count() == 1
        ### Typed keys leave through the socket as terminado `["stdin", ...]` frames.
        frame.evaluate(
            "window.__sent = []; const s = window.terminal.socket; const o = s.send.bind(s);"
            " s.send = (d) => { window.__sent.push(d); o(d); }; true;"
        )
        before = _rows(frame).count(version)
        _focus_terminal(frame)
        page.keyboard.type('show version')
        page.keyboard.press('Enter')
        frames = [json.loads(f) for f in frame.evaluate("window.__sent")]
        typed = ''.join(f[1] for f in frames if f[0] == 'stdin')
        assert 'show version' in typed, typed
        ### The shell's answer (not the echo of the command) carries the version.
        frame.wait_for_function(
            "([version, before]) => document.querySelector('.xterm-rows').innerText.split(version).length - 1 > before",
            arg=[version, before],
            timeout=60000,
        )
        ### The fit addon sized the PTY: a `set_size` frame went out with the terminal's dimensions.
        dims = frame.evaluate("[window.terminal.term.rows, window.terminal.term.cols]")
        assert dims[0] > 2 and dims[1] > 20, dims
    except Exception as e:
        raise AssertionError(
            f"{term_server.path}: {e}\nrows:\n{_rows(frame)}\nerrors: {tab.errors}\n{term_server.log()}"
        ) from None
    assert tab.errors == [], tab.errors


def test_the_dashboard_embeds_the_same_terminal(term_server, context):
    page, tab, cookie = _login(context, term_server)
    page.wait_for_selector('#webterm-iframe', timeout=60000)
    ### Fullscreen gives the terminal the whole row and takes it back.
    page.click('#webterm-fullscreen-button')
    page.wait_for_function("document.getElementById('content-col-right').dataset.fullscreen === '1'")
    assert not page.locator('#content-col-left').is_visible()
    page.click('#webterm-fullscreen-button')
    page.wait_for_function("!document.getElementById('content-col-right').dataset.fullscreen")
    assert page.locator('#content-col-left').is_visible()
    assert page.get_attribute('#webterm-iframe', 'src') == f"/webterm/{cookie}"
    frame = page.query_selector('#webterm-iframe').content_frame()
    frame.wait_for_selector('.xterm .xterm-screen', timeout=30000)
    ### `Exec` submits into this existing terminal.
    page.click('#go-button')
    page.wait_for_selector('#webterm-div', state='visible')
    assert not page.locator('#exec-div').is_visible()
    assert tab.errors == [], tab.errors


def test_exec_accepts_an_idle_terminal_after_reattaching(term_server, context):
    """A visible idle prompt stays executable through tmux and iframe refreshes."""
    page, tab, _ = _login(context, term_server)
    frame = _open_terminal(page, term_server.base_url, '/dash')
    try:
        frame.wait_for_function(
            'window.terminal.session.idle && !window.terminal.session.lineDirty',
            timeout=10000,
        )
    except Exception:
        state = frame.evaluate('({idle: terminal.session.idle, dirty: terminal.session.lineDirty})')
        pytest.fail(f'Idle terminal rejected: {state}; rows: {_rows(frame)}')
    page.click('#go-button')
    page.wait_for_function("() => { const id = window._mrsmExecSubmitted; const s = document.getElementById('webterm-iframe')?.contentWindow?.terminal?.session; return s?.requests.get(id)?.accepted; }")
    frame.wait_for_function('window.terminal.session.idle && !window.terminal.session.lineDirty')
    page.click('#webterm-refresh-button')
    frame = page.query_selector('#webterm-iframe').content_frame()
    frame.wait_for_function('window.terminal?.session.idle && !window.terminal.session.lineDirty')
    page.click('#go-button')
    page.wait_for_function("() => { const id = window._mrsmExecSubmitted; const s = document.getElementById('webterm-iframe')?.contentWindow?.terminal?.session; return s?.requests.get(id)?.accepted; }")
    frame.wait_for_function('window.terminal.session.idle && !window.terminal.session.lineDirty')
    # Real unfinished input remains untouched by Exec, including after reattachment.
    frame.evaluate("window.terminal.term.input('unfinished')")
    page.click('#go-button')
    page.wait_for_function("document.getElementById('terminal-exec-status').innerText.includes('unfinished input')")
    assert 'unfinished' in _rows(frame)
    # Erasing the entire buffer restores readiness without submitting a blank command.
    frame.evaluate("window.terminal.term.input('\\x15')")
    frame.wait_for_function('window.terminal.session.idle && !window.terminal.session.lineDirty')
    page.click('#go-button')
    page.wait_for_function("() => { const id = window._mrsmExecSubmitted; const s = document.getElementById('webterm-iframe')?.contentWindow?.terminal?.session; return s?.requests.get(id)?.accepted; }")
    frame.wait_for_function('window.terminal.session.idle && !window.terminal.session.lineDirty')
    frame.evaluate("window.terminal.term.input('cancelled\\x03')")
    frame.wait_for_function('window.terminal.session.idle && !window.terminal.session.lineDirty')
    assert tab.errors == [], tab.errors
    page.close()


def test_standalone_page_matches_the_v4_route(term_server, context):
    """`/webterm/<session>` (the v4 page) renders the same terminal without the console chrome."""
    base = term_server.base_url
    page, tab, cookie = _login(context, term_server)
    response = page.request.get(f"{base}/webterm/{cookie}")
    assert response.status == 200
    assert 'Meerschaum Shell' in response.text()
    assert response.headers['referrer-policy'] == 'no-referrer'
    frame = _open_terminal(page, base, f"/webterm/{cookie}")
    assert page.locator('nav').count() == 0
    assert frame.evaluate("window.terminal.term.unicode.activeVersion") == '11'
    assert frame.evaluate("window.terminal.socket.readyState") == 1
    ### Resizing the viewport refits the terminal and tells the PTY.
    frame.evaluate(
        "window.__sizes = []; const s = window.terminal.socket; const o = s.send.bind(s);"
        " s.send = (d) => { window.__sizes.push(d); o(d); }; true;"
    )
    cols = frame.evaluate("window.terminal.term.cols")
    page.set_viewport_size({'width': 640, 'height': 480})
    frame.wait_for_function(f"window.terminal.term.cols !== {cols}", timeout=15000)
    frame.wait_for_function(
        "window.__sizes.some((d) => JSON.parse(d)[0] === 'set_size')", timeout=15000,
    )


def test_extra_key_row_on_a_narrow_screen(term_server, browser):
    """v4's Termux-style keys: arrows send escape sequences, CTRL is sticky, focus stays in the terminal."""
    base = term_server.base_url
    context = browser.new_context(viewport={'width': 390, 'height': 800}, has_touch=True)
    try:
        page, tab, _ = _login(context, term_server)
        frame = _open_terminal(page, base)
        keys = page.locator('.webterm-key-btn')
        assert keys.first.is_visible()
        frame.evaluate(
            "window.__sent = []; const s = window.terminal.socket; const o = s.send.bind(s);"
            " s.send = (d) => { window.__sent.push(JSON.parse(d)); o(d); }; true;"
        )
        _focus_terminal(frame)
        for label, sequence in (('↑', '\x1b[A'), ('←', '\x1b[D'), ('ESC', '\x1b'), ('TAB', '\t')):
            page.click(f'.webterm-key-btn:text-is("{label}")')
            frame.wait_for_function(
                "(seq) => window.__sent.some((f) => f[0] === 'stdin' && f[1] === seq)", arg=sequence,
            )
        page.click('.webterm-key-btn:text-is("CTRL")')
        page.wait_for_selector('.webterm-key-btn:text-is("CTRL").active')
        _focus_terminal(frame)
        page.keyboard.press('c')
        frame.wait_for_function(
            "() => window.__sent.some((f) => f[0] === 'stdin' && f[1] === '\\x03')"
        )
        page.click('.webterm-key-btn:text-is("SHIFT")')
        _focus_terminal(frame)
        page.keyboard.press('a')
        frame.wait_for_function(
            "() => window.__sent.some((f) => f[0] === 'stdin' && f[1] === 'A')"
        )
        ### The tmux path shares one shell across the module's tests: do not leave `A` on its line.
        page.keyboard.press('Backspace')
        frame.wait_for_function(
            "() => window.__sent.some((f) => f[0] === 'stdin' && f[1] === '\\x7f')"
        )
        assert tab.errors == [], tab.errors
    finally:
        context.close()


def test_the_key_row_is_hidden_with_room_for_a_keyboard(term_server, context):
    page, tab, _ = _login(context, term_server)
    page.set_viewport_size({'width': 1280, 'height': 800})
    _open_terminal(page, term_server.base_url)
    assert not page.locator('.webterm-key-btn').first.is_visible()
    ### The legacy terminal URL now opens the console, including its fullscreen control.
    assert page.locator('#webterm-fullscreen-button').count() == 1
    ### The new-window control is the tmux path's.
    assert (page.locator('#webterm-new-tab-button').count() == 1) == (term_server.path == 'tmux')
    ### Refresh reloads the terminal's page: it comes back with its shell.
    page.click('#webterm-refresh-button')
    frame = page.query_selector('#webterm-iframe').content_frame()
    frame.wait_for_selector('.xterm-rows > div', timeout=30000)
    assert tab.errors == [], tab.errors


def test_two_sessions_in_two_contexts(term_server, browser):
    """Two logins of one user, in two browser contexts (v4's second login returned a 500)."""
    base = term_server.base_url
    first_context, second_context = browser.new_context(), browser.new_context()
    try:
        first_page, first_tab, first_cookie = _login(first_context, term_server)
        second_page, second_tab, second_cookie = _login(second_context, term_server)
        assert first_cookie != second_cookie
        first = _open_terminal(first_page, base)
        second = _open_terminal(second_page, base)
        ### The shell repaints the screen after each command, so only the latest answer shows.
        for page, frame, other, marker in (
            (first_page, first, second, 'echo-marker-one'),
            (second_page, second, first, 'echo-marker-two'),
        ):
            _focus_terminal(frame)
            page.keyboard.type(f"echo {marker}")
            page.keyboard.press('Enter')
            _wait_for_text(frame, marker)
            if term_server.path == 'tmux':
                ### The same user's tabs attach to one tmux session: the other tab mirrors it.
                _wait_for_text(other, marker)
        if term_server.path != 'tmux':
            ### One shell per tab: neither sees the other's command.
            assert 'echo-marker-two' not in _rows(first)
            assert 'echo-marker-one' not in _rows(second)
        for tab in (first_tab, second_tab):
            assert tab.errors == [], tab.errors
    finally:
        first_context.close()
        second_context.close()


@pytest.mark.skipif(not HAS_TMUX, reason="tmux is not installed")
def test_reload_reattaches_to_the_tmux_session(term_server, context):
    if term_server.path != 'tmux':
        pytest.skip("the plain path starts a new shell on every connection")
    page, tab, _ = _login(context, term_server)
    frame = _open_terminal(page, term_server.base_url)
    _focus_terminal(frame)
    ### Text typed at the prompt (not submitted) lives in the shell: a new shell would not have it.
    page.keyboard.type('echo reattach-marker')
    _wait_for_text(frame, 'reattach-marker')
    ### Reloading closes the PTY (and the tmux client) but not the session: the shell, with the
    ### text on its line, comes back without typing anything. (The shell repaints its prompt when
    ### the new client sizes the window, so only the line being edited is certain to survive.)
    page.reload()
    page.wait_for_selector('#webterm-iframe', timeout=60000)
    frame = page.query_selector('#webterm-iframe').content_frame()
    frame.wait_for_selector('.xterm-rows > div', timeout=30000)
    _wait_for_text(frame, 'reattach-marker')
    ### Do not leave the text on the shared shell's line.
    _focus_terminal(frame)
    page.keyboard.press('Control+u')
    ### The session is named after the user, with the configured suffix.
    listing = subprocess.run(
        ['tmux', '-S', term_server.tmux_socket, 'list-sessions', '-F', '#{session_name}'],
        env={k: v for k, v in term_server.env.items() if k not in ('TMUX', 'TMUX_PANE')}, capture_output=True, text=True, timeout=20,
    ).stdout.split()
    assert f"{USERNAME}_mrsm" in listing, listing
