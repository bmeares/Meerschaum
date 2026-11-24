"""Check installed Windows plugin and Dash routes and sidecar exit when the API dies."""

from contextlib import contextmanager
import ctypes
from ctypes import wintypes
from importlib.metadata import version
import json
import os
import re
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import URLError
from urllib.request import ProxyHandler, build_opener


@contextmanager
def api_job(port, root, env, log):
    """Put the launcher in a private Windows job before it can start the API or a venv redirector."""
    import _winapi

    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateJobObjectW.restype = wintypes.HANDLE
    job = wintypes.HANDLE(kernel.CreateJobObjectW(None, None))
    if not job:
        raise ctypes.WinError(ctypes.get_last_error())
    command = (
        "if ([Console]::ReadLine() -ne 'start') { exit 1 }; "
        "& $env:MRSM_PYTHON -I -m meerschaum start api -i sql:smoke "
        f"--host 127.0.0.1 --port {port} --no-webterm --no-auth; exit $LASTEXITCODE"
    )
    try:
        with subprocess.Popen(
            ['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', command],
            cwd=root, env=env, stdin=subprocess.PIPE,
            stdout=log, stderr=subprocess.STDOUT,
        ) as launcher:
            # Assign the waiting launcher before it can create any descendants.
            handle = wintypes.HANDLE(int(launcher._handle))
            if not kernel.AssignProcessToJobObject(job, handle):
                error = ctypes.WinError(ctypes.get_last_error())
                launcher.kill()
                raise error
            try:
                launcher.stdin.write(b'start\r\n')
                launcher.stdin.close()
                yield launcher, kernel, job
            finally:
                # The job includes surviving workers and redirectors even if the listener died.
                if not kernel.TerminateJobObject(job, 1):
                    raise ctypes.WinError(ctypes.get_last_error())
                launcher.wait(timeout=15)
    finally:
        _winapi.CloseHandle(job.value)


@contextmanager
def owned_process(kernel, job, pid, terminate=False):
    """Pin the process identity and verify it belongs to this smoke test."""
    import _winapi

    access = _winapi.SYNCHRONIZE | 0x1000  # PROCESS_QUERY_LIMITED_INFORMATION
    if terminate:
        access |= 0x0001  # PROCESS_TERMINATE
    handle = _winapi.OpenProcess(access, False, pid)
    try:
        member = wintypes.BOOL()
        if not kernel.IsProcessInJob(wintypes.HANDLE(handle), job, ctypes.byref(member)):
            raise ctypes.WinError(ctypes.get_last_error())
        if not member.value or _winapi.WaitForSingleObject(handle, 0) != _winapi.WAIT_TIMEOUT:
            raise AssertionError(f'Expected a live smoke-test process, got PID {pid}.')
        yield handle
    finally:
        _winapi.CloseHandle(handle)


def verify_parent_death(kernel, job, port, host_pid):
    import _winapi

    # The venv launcher PID may differ from the process that holds the public TCP listener.
    query = (
        f'(Get-NetTCPConnection -State Listen -LocalAddress 127.0.0.1 -LocalPort {port} '
        '-ErrorAction Stop).OwningProcess'
    )
    listener_pid = int(subprocess.check_output(
        ['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', query],
        text=True, timeout=20,
    ).strip())
    assert listener_pid != host_pid
    with owned_process(kernel, job, listener_pid, terminate=True) as listener:
        with owned_process(kernel, job, host_pid) as sidecar:
            start = time.monotonic()
            _winapi.TerminateProcess(listener, 1)
            # Allow the host's ten-second graceful shutdown and time for Windows to schedule it.
            if _winapi.WaitForSingleObject(sidecar, 15_000) != _winapi.WAIT_OBJECT_0:
                raise AssertionError('The Python API host survived the loss of the public listener.')
            print(f'Python API host exited after parent death in {time.monotonic()-start:.2f}s',
                  flush=True)


def log_summary(path):
    """Keep diagnostic details from the last 8 KiB without printing headers or configuration."""
    with path.open('rb') as stream:
        stream.seek(max(0, path.stat().st_size - 8192))
        tail = stream.read(8192).decode(errors='replace')
    hints = re.findall(
        r'\b(?:[A-Za-z_]{1,50}(?:Error|Exception)|WinError [0-9]+|permission denied|'
        r'access is denied|connection refused|address already in use|not found|'
        r'failed to start|timed out|symlink|lock|worker|uvicorn|traceback)\b',
        tail, flags=re.I,
    )
    frames = re.findall(r'File "[^"\r\n]*[\\/]([\w.]+\.py)", line (\d+) in ([\w.<>]+)', tail)
    return f'{len(tail)} log characters, hints {list(dict.fromkeys(hints))[-12:]}, frames {frames[-4:]}'


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        plugins = root / 'plugins'
        plugins.mkdir()
        (plugins / 'smoke_host.py').write_text("""
import os
from meerschaum.plugins import api_plugin

@api_plugin
def health(app):
    @app.get('/wheel-host-smoke')
    def smoke():
        return {'from_plugin': True, 'pid': os.getpid()}
""")
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        sql = json.dumps({'flavor': 'sqlite', 'database': str(root / 'smoke.db')})
        env = {key: value for key, value in os.environ.items() if not key.startswith('MRSM_')}
        env.update({
            'MRSM_ROOT_DIR': str(root), 'MRSM_PLUGINS_DIR': str(plugins),
            'MRSM_PYTHON': sys.executable, 'MRSM_NO_AUTO_INSTALL': '1',
            'MRSM_SQL_MAIN': sql, 'MRSM_SQL_SMOKE': sql,
            'MRSM_CONFIG': '{"meerschaum":{"instance":"sql:smoke"}}',
            'MRSM_NO_UPDATE_CHECK': '1', 'PYTHONUNBUFFERED': '1',
        })
        http = build_opener(ProxyHandler({}))
        log_path = root / 'server.log'
        with log_path.open('wb') as log, api_job(port, root, env, log) as (launcher, kernel, job):
            base = f'http://127.0.0.1:{port}'
            deadline = time.monotonic() + 120
            for path in ('/version', '/wheel-host-smoke', '/dash/_dash-layout'):
                last_error = None
                while launcher.poll() is None and time.monotonic() < deadline:
                    try:
                        with http.open(base + path, timeout=10) as response:
                            payload = json.load(response)
                        break
                    except (URLError, TimeoutError) as exc:
                        last_error = exc
                        time.sleep(0.5)
                else:
                    raise RuntimeError(f'{path} was unavailable, launcher status {launcher.poll()}, '
                                       f'last request error {last_error!r}. {log_summary(log_path)}')
                if path == '/version':
                    assert payload == version('meerschaum'), path
                elif path == '/wheel-host-smoke':
                    assert payload['from_plugin'] is True and type(payload['pid']) is int, path
                    host_pid = payload['pid']
                else:
                    assert payload['props'], path
                print(f'Installed API served {path}', flush=True)
            verify_parent_death(kernel, job, port, host_pid)


if __name__ == '__main__':
    main()
