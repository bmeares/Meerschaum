#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
End-to-end check of the web terminal's xterm.js front end (opt-in, see `conftest.py`).

The terminal page is served by the API host at `/webterm/<session>`; the Rust console vendors the
same xterm.js release (`crates/meerschaum-web/assets/versions.json`) at `/dash/assets/xterm.js`.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from tests.console.conftest import REPO, Tab, Server, free_port

VERSIONS = REPO / 'crates' / 'meerschaum-web' / 'assets' / 'versions.json'


def _vendored_xterm_version() -> str:
    url = json.loads(VERSIONS.read_text())['xterm.js']
    return url.split('@xterm/xterm@', 1)[1].split('/', 1)[0]


@pytest.fixture(scope='module')
def xterm_server():
    """A private web-terminal server (the shared one is already used by another test's session)."""
    port = free_port()
    instance = Server(
        'xterm',
        config={'api': {'webterm': {'port': port, 'tmux': {'enabled': False}}}},
        native=False,
        webterm=True,
    )
    instance.watch_ports = [port]
    try:
        instance.start()
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()


def test_vendored_xterm_is_served_locally(server, context):
    page = context.new_page()
    version = _vendored_xterm_version()
    for name in ('xterm.js', 'xterm.css'):
        response = page.request.get(f"{server.base_url}/dash/assets/{name}")
        assert response.status == 200, (name, response.status)
    asset = (pathlib.Path(VERSIONS).parent / 'xterm.js').read_bytes()
    served = page.request.get(f"{server.base_url}/dash/assets/xterm.js").body()
    assert served == asset, f"served xterm.js differs from the vendored {version} asset"


def test_webterm_renders_and_forwards_input(xterm_server, context):
    """xterm.js 6 builds its DOM renderer, paints written data, and forwards keystrokes to the PTY socket.

    This checks the front end only: the shell behind the socket is the Python shell of the API host,
    which is not part of the xterm upgrade.
    """
    base = xterm_server.base_url
    page = context.new_page()
    tab = Tab(page, base)
    tab.login()
    cookie = [c for c in context.cookies() if c['name'] == 'mrsm-session-id'][0]['value']
    for _ in range(40):
        response = page.request.get(f"{base}/webterm/{cookie}")
        if response.status == 200:
            break
        page.wait_for_timeout(500)
    assert response.status == 200, (response.status, response.text()[:500], xterm_server.log())
    page.goto(f"{base}/webterm/{cookie}")
    try:
        page.wait_for_selector('#xterm-div .xterm .xterm-screen', timeout=30000)
        page.wait_for_selector('#xterm-div .xterm-rows > div', timeout=30000)
    except Exception as e:
        raise AssertionError(f"xterm did not render: {e}; errors: {tab.errors}") from None
    assert page.evaluate("typeof Terminal") == 'function'
    assert page.evaluate("window.terminal.term.options.allowProposedApi") is True
    assert page.evaluate("window.terminal.term.unicode.activeVersion") == '11'
    ### The shell prints its banner through the websocket and xterm paints it.
    page.wait_for_function(
        "document.querySelector('#xterm-div .xterm-rows').innerText.trim().length > 0",
        timeout=60000,
    )
    ### Data written to the terminal is painted into the DOM rows (SGR colors become spans).
    page.evaluate("window.terminal.term.write('\\r\\nxterm6-probe \\x1b[31mred\\x1b[0m \\u2192 ok\\r\\n')")
    page.wait_for_function(
        "document.querySelector('#xterm-div .xterm-rows').innerText.includes('xterm6-probe red')",
        timeout=15000,
    )
    ### Typed keys leave through the socket as `["stdin", ...]` frames.
    page.evaluate(
        "window.__sent = []; const s = window.terminal.socket; const o = s.send.bind(s);"
        " s.send = (d) => { window.__sent.push(d); o(d); };"
    )
    page.click('#xterm-div .xterm-screen')
    page.wait_for_function(
        "document.activeElement && document.activeElement.classList.contains('xterm-helper-textarea')"
    )
    page.keyboard.type('show version')
    page.keyboard.press('Enter')
    page.wait_for_function("window.__sent.length >= 13", timeout=15000)
    frames = [json.loads(frame) for frame in page.evaluate("window.__sent") if frame]
    typed = ''.join(frame[1] for frame in frames if frame[0] == 'stdin')
    assert 'show version\r' in typed, typed
    assert tab.errors == [], tab.errors
