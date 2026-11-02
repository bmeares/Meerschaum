#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The web terminal of an API that runs as a job (opt-in, see `conftest.py`).

A job's process carries the job's environment (`meerschaum_jobs::Job::env`: `MRSM_NONINTERACTIVE`,
`LINES`, `COLUMNS`, `PYTHONUNBUFFERED`), so an API started as a job (`start api -d`, a Compose
project, a `systemd` unit) used to hand `MRSM_NONINTERACTIVE` to its terminal's shell: the shell
read plain lines without its editor, so it drew no bottom toolbar and never reported an idle
prompt, and the console's Exec button was always refused. These tests start the API with that
environment and check, in a real browser, that Exec types into an idle shell (and still refuses a
busy one or a partly typed line), that the shell's toolbar is drawn, and, on the tmux path, that
the session's windows are listed on the terminal's own status line and can be switched.
"""

from __future__ import annotations

import os
import pathlib
import shlex
import shutil
import subprocess
import tempfile
import time

import pytest

from tests.console.conftest import Tab, Server, USERNAME

pytestmark = pytest.mark.console

HAS_TMUX = shutil.which('tmux') is not None
SCREENSHOTS = pathlib.Path(
    os.environ.get('MRSM_TEST_SCREENSHOTS', '')
    or (pathlib.Path.home() / '.cache' / 'mrsm-console-tests' / 'screenshots')
)

#: What `meerschaum_jobs::Job::env` gives every job's process (the server runs in one).
JOB_ENV = {
    'MRSM_NONINTERACTIVE': 'true',
    'PYTHONUNBUFFERED': '1',
    'LINES': '120',
    'COLUMNS': '100',
}

#: Exec's default command on the Terminal page (the form starts on `show pipes`; the page adds `-i`).
EXEC_COMMAND = 'show pipes'


def _tmux(socket: str, server: Server, *args: str) -> subprocess.CompletedProcess:
    """The real tmux on one socket (`-S`), never the caller's server."""
    return subprocess.run(
        [shutil.which('tmux') or 'tmux', '-S', socket, *args],
        env={k: v for k, v in server.env.items() if k not in ('TMUX', 'TMUX_PANE')},
        capture_output=True, text=True, timeout=20,
    )


def _webterm_tmux_socket(server: Server) -> str:
    from tests.console.test_webterm import _webterm_tmux_socket as socket_of
    return socket_of(server)


@pytest.fixture(scope='module', params=['tmux', 'plain'])
def job_server(request):
    """An API with a job's environment and the terminal on, on the tmux or the plain path."""
    use_tmux = request.param == 'tmux'
    if use_tmux and not HAS_TMUX:
        pytest.skip("tmux is not installed; the plain path covers this host.")
    instance = Server(
        f'rc28-term-{request.param}',
        config={'api': {'webterm': {'tmux': {'enabled': use_tmux}}}},
        native=False,
        webterm=True,
        env=JOB_ENV,
    )
    instance.path = request.param
    tmux_dir = tempfile.mkdtemp(prefix='mt-')
    instance.env['TMUX_TMPDIR'] = tmux_dir
    instance.tmux_socket = str(pathlib.Path(tmux_dir) / 'socket')
    if use_tmux:
        ### Any tmux command that forgot its own `-S` lands on this private socket instead of a
        ### developer's server.
        bin_dir = pathlib.Path(tmux_dir) / 'bin'
        bin_dir.mkdir()
        wrapper = bin_dir / 'tmux'
        wrapper.write_text(
            '#!/bin/sh\nexec ' + shlex.quote(shutil.which('tmux'))
            + ' -S ' + shlex.quote(instance.tmux_socket) + ' "$@"\n'
        )
        wrapper.chmod(0o700)
        instance.env['PATH'] = str(bin_dir) + os.pathsep + instance.env['PATH']
    try:
        instance.start()
    except Exception:
        instance.stop()
        shutil.rmtree(tmux_dir, ignore_errors=True)
        raise
    yield instance
    if use_tmux:
        for socket in (_webterm_tmux_socket(instance), instance.tmux_socket):
            _tmux(socket, instance, 'kill-server')
    instance.stop()
    shutil.rmtree(tmux_dir, ignore_errors=True)


def _login(context, server: Server):
    page = context.new_page()
    tab = Tab(page, server.base_url)
    tab.login()
    return page, tab


def _terminal(page):
    """The console's terminal frame, once the shell has painted something."""
    page.wait_for_selector('#webterm-iframe', timeout=60000)
    page.locator('#webterm-iframe').scroll_into_view_if_needed()
    frame = page.query_selector('#webterm-iframe').content_frame()
    frame.wait_for_selector('.xterm-rows > div', timeout=30000)
    frame.wait_for_function(
        "document.querySelector('.xterm-rows').innerText.trim().length > 0", timeout=90000,
    )
    return frame


def _rows(frame) -> list[str]:
    """The terminal's screen, one string per row (from xterm's buffer, not the DOM)."""
    return frame.evaluate(
        """() => {
            const b = window.terminal.term.buffer.active, out = [];
            for (let y = 0; y < window.terminal.term.rows; y++) {
                const line = b.getLine(b.viewportY + y);
                out.push(line ? line.translateToString(true) : '');
            }
            return out;
        }"""
    )


def _screen(frame) -> str:
    return '\n'.join(_rows(frame))


def _shot(page, name: str) -> str:
    SCREENSHOTS.mkdir(parents=True, exist_ok=True)
    path = SCREENSHOTS / f'{name}.png'
    page.screenshot(path=str(path))
    return str(path)


def _shell_state(frame):
    """The server's answer to the terminal's readiness query (`{idle, empty}` or `None`)."""
    return frame.evaluate(
        "() => window.terminal ? window.terminal.session.queryState() : null"
    )


def _wait_state(frame, predicate, what: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    state = None
    while time.time() < deadline:
        state = _shell_state(frame)
        if state is not None and predicate(state):
            return
        time.sleep(0.25)
    raise AssertionError(f"the shell never reported {what} (last: {state}):\n{_screen(frame)}")


def _wait_idle(frame, timeout: float = 30.0) -> None:
    _wait_state(frame, lambda s: s['idle'] and s['empty'], 'an idle prompt', timeout)


def _exec(page) -> dict:
    """Click Exec and return the terminal's acknowledgement."""
    page.click('#go-button')
    handle = page.wait_for_function(
        """() => {
            const id = window._mrsmExecSubmitted;
            const s = document.getElementById('webterm-iframe')?.contentWindow?.terminal?.session;
            return id && s && s.requests.get(id);
        }""",
        timeout=20000,
    )
    return handle.json_value()


def _focus(frame) -> None:
    frame.click('.xterm-screen')
    frame.wait_for_function(
        "document.activeElement && document.activeElement.classList.contains('xterm-helper-textarea')"
    )


def _history(server: Server) -> list[str]:
    """The lines the shell ran, oldest first (`.mrsm_history` in the root: `+<line>` entries)."""
    path = pathlib.Path(server.root) / '.mrsm_history'
    try:
        text = path.read_text()
    except OSError:
        return []
    return [line[1:] for line in text.splitlines() if line.startswith('+')]


def test_exec_types_into_the_idle_shell_of_a_job_hosted_api(job_server, context):
    page, tab = _login(context, job_server)
    frame = _terminal(page)
    _wait_idle(frame)
    ### A line of its own first, so the shell records the Exec'd command even if it ran before.
    _focus(frame)
    page.keyboard.type('pass')
    page.keyboard.press('Enter')
    _wait_idle(frame, timeout=60.0)
    ran = len(_history(job_server))
    ack = _exec(page)
    assert ack['accepted'], (ack, _screen(frame))
    ### The command ran (the shell recorded it and printed its answer) and the prompt is back.
    expected = [f'{EXEC_COMMAND} -i sql:w7console']
    deadline = time.time() + 60
    while time.time() < deadline and _history(job_server)[ran:] != expected:
        time.sleep(0.25)
    assert _history(job_server)[ran:] == expected, (_history(job_server), _screen(frame))
    frame.wait_for_function(
        "() => document.querySelector('.xterm-rows').innerText.includes('No pipes to show')",
        timeout=60000,
    )
    _wait_idle(frame, timeout=60.0)
    _shot(page, f'rc28-exec-accepted-{job_server.path}')
    assert page.inner_text('#terminal-exec-status').strip() == ''
    assert tab.errors == [], tab.errors
    page.close()


def test_exec_never_types_over_a_partly_typed_line_or_a_running_command(job_server, context):
    page, tab = _login(context, job_server)
    frame = _terminal(page)
    _wait_idle(frame)
    _focus(frame)
    page.keyboard.type('echo half')
    frame.wait_for_function("document.querySelector('.xterm-rows').innerText.includes('echo half')")
    ack = _exec(page)
    assert not ack['accepted'], ack
    assert 'unfinished input' in ack['message'], ack
    _shot(page, f'rc28-exec-refused-partial-{job_server.path}')
    ### The typed text is still there, untouched.
    assert 'echo half' in _screen(frame)
    assert EXEC_COMMAND not in _screen(frame).split('echo half', 1)[-1]
    ### Exec was a click on the page: the keyboard goes back to the terminal.
    _focus(frame)
    page.keyboard.press('Control+u')
    _wait_idle(frame)
    ### A command that is still running.
    page.keyboard.type('python -c "import time; time.sleep(30)"')
    page.keyboard.press('Enter')
    _wait_state(frame, lambda s: not s['idle'], 'a running command', 20.0)
    ack = _exec(page)
    assert not ack['accepted'], ack
    assert 'busy' in ack['message'], ack
    _shot(page, f'rc28-exec-refused-busy-{job_server.path}')
    _focus(frame)
    page.keyboard.press('Control+c')
    _wait_idle(frame, timeout=60.0)
    page.close()


def test_the_shell_draws_its_bottom_toolbar(job_server, context):
    page, tab = _login(context, job_server)
    frame = _terminal(page)
    _wait_idle(frame)
    ### The toolbar is the shell's last row: the instance, then the executor and the repository.
    try:
        frame.wait_for_function(
            """() => {
                const t = window.terminal.term, b = t.buffer.active;
                for (let y = t.rows - 1; y >= t.rows - 3; y--) {
                    const line = b.getLine(b.viewportY + y);
                    if (line && line.translateToString(true).includes('sql:w7console | ')) return true;
                }
                return false;
            }""",
            timeout=20000,
        )
    except Exception:
        _shot(page, f'rc28-toolbar-missing-{job_server.path}')
        raise AssertionError("no toolbar on the terminal's last rows:\n" + _screen(frame)) from None
    _shot(page, f'rc28-toolbar-{job_server.path}')
    page.close()


def _select_instance(page, keys: str) -> None:
    """What the page's instance selector posts to the terminal (`callbacks/dashboard.py`)."""
    page.evaluate(
        """(keys) => document.getElementById('webterm-iframe').contentWindow.postMessage(
            {action: 'instance', subaction_text: keys}, location.origin)""",
        keys,
    )


def test_the_instance_selector_types_only_at_an_idle_prompt(job_server, context):
    page, tab = _login(context, job_server)
    frame = _terminal(page)
    _wait_idle(frame)
    _focus(frame)
    page.keyboard.type('echo pending')
    frame.wait_for_function("document.querySelector('.xterm-rows').innerText.includes('echo pending')")
    _select_instance(page, 'sql:w7console')
    time.sleep(2)
    assert 'instance sql:w7console' not in _screen(frame), _screen(frame)
    assert 'echo pending' in _screen(frame)
    _focus(frame)
    page.keyboard.press('Control+u')
    _wait_idle(frame)
    _select_instance(page, 'sql:w7console')
    frame.wait_for_function(
        "() => document.querySelector('.xterm-rows').innerText.includes('instance sql:w7console')",
        timeout=20000,
    )
    _wait_idle(frame, timeout=60.0)
    assert tab.errors == [], tab.errors
    page.close()


def _windows(server: Server) -> list[tuple[str, str, bool]]:
    """The user's tmux windows on the terminal's own server: (index, name, active)."""
    listing = _tmux(
        _webterm_tmux_socket(server), server,
        'list-windows', '-t', f'={USERNAME}_mrsm', '-F', '#{window_index} #{window_name} #{window_active}',
    ).stdout.split('\n')
    out = []
    for line in listing:
        if line.strip():
            index, name, active = line.split()
            out.append((index, name, active == '1'))
    return out


def _click_cell(page, frame, row: int, col: int) -> None:
    """Click the middle of one terminal cell (the browser's mouse, which xterm.js hands to tmux)."""
    box = frame.evaluate(
        """() => {
            const r = document.querySelector('.xterm-screen').getBoundingClientRect();
            return {x: r.left, y: r.top, w: r.width, h: r.height,
                    cols: window.terminal.term.cols, rows: window.terminal.term.rows};
        }"""
    )
    frame_box = page.locator('#webterm-iframe').bounding_box()
    x = frame_box['x'] + box['x'] + (col + 0.5) * box['w'] / box['cols']
    y = frame_box['y'] + box['y'] + (row + 0.5) * box['h'] / box['rows']
    page.mouse.click(x, y)


def test_tmux_windows_are_tabs_on_the_terminal(job_server, context):
    """The tmux session is visible: its windows are tabs on the first row; `+` adds one, a click switches."""
    if job_server.path != 'tmux':
        pytest.skip("windows are the tmux path's")
    page, tab = _login(context, job_server)
    frame = _terminal(page)
    _wait_idle(frame)
    ### One window: its tab is the first row, the shell's toolbar the last.
    frame.wait_for_function(
        "() => window.terminal.term.buffer.active.getLine(window.terminal.term.buffer.active.viewportY)"
        ".translateToString(true).startsWith(' 1 mrsm')",
        timeout=20000,
    )
    assert _windows(job_server) == [('1', 'mrsm', True)], _windows(job_server)
    page.click('#webterm-new-tab-button')
    deadline = time.time() + 30
    while time.time() < deadline and len(_windows(job_server)) < 2:
        time.sleep(0.25)
    assert _windows(job_server) == [('1', 'mrsm', False), ('2', 'mrsm', True)], _windows(job_server)
    frame.wait_for_function(
        "() => window.terminal.term.buffer.active.getLine(window.terminal.term.buffer.active.viewportY)"
        ".translateToString(true).startsWith(' 1 mrsm  2 mrsm')",
        timeout=20000,
    )
    ### The new window's shell is interactive too: idle, with its toolbar.
    _wait_idle(frame, timeout=60.0)
    frame.wait_for_function(
        "() => document.querySelector('.xterm-rows').innerText.includes('sql:w7console | ')",
        timeout=20000,
    )
    shot = _shot(page, 'rc28-tmux-two-windows')
    ### A click on the first tab switches back to it.
    _click_cell(page, frame, 0, 2)
    deadline = time.time() + 15
    while time.time() < deadline and not _windows(job_server)[0][2]:
        time.sleep(0.25)
    assert _windows(job_server)[0][2], (_windows(job_server), shot)
    _shot(page, 'rc28-tmux-first-window')
    ### Exec types into the window on screen.
    _wait_idle(frame, timeout=60.0)
    ack = _exec(page)
    assert ack['accepted'], ack
    _wait_idle(frame, timeout=60.0)
    ### The browser's own menu does not cover tmux's window menu (Shift keeps it).
    prevented = frame.evaluate(
        """() => {
            const e = new MouseEvent('contextmenu', {bubbles: true, cancelable: true});
            document.querySelector('.xterm-screen').dispatchEvent(e);
            const s = new MouseEvent('contextmenu', {bubbles: true, cancelable: true, shiftKey: true});
            document.querySelector('.xterm-screen').dispatchEvent(s);
            return [e.defaultPrevented, s.defaultPrevented];
        }"""
    )
    assert prevented == [True, False], prevented
    ### `exit` in a window's shell closes that window; the session and the terminal stay.
    second = len(' 1 mrsm ') + 2
    _click_cell(page, frame, 0, second)
    deadline = time.time() + 15
    while time.time() < deadline and not _windows(job_server)[1][2]:
        time.sleep(0.25)
    assert _windows(job_server)[1][2], _windows(job_server)
    _wait_idle(frame, timeout=60.0)
    _focus(frame)
    page.keyboard.type('exit')
    page.keyboard.press('Enter')
    deadline = time.time() + 30
    while time.time() < deadline and len(_windows(job_server)) != 1:
        time.sleep(0.25)
    assert _windows(job_server) == [('1', 'mrsm', True)], _windows(job_server)
    _wait_idle(frame, timeout=60.0)
    assert '[CLOSED]' not in _screen(frame)
    _shot(page, 'rc28-tmux-window-closed')
    assert tab.errors == [], tab.errors
    page.close()
