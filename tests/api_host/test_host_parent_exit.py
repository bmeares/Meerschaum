#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The API host exits with the server that started it. The Rust server holds the write end of the
host's stdin (`MRSM_API_HOST_PARENT_PIPE=1`); when the server dies without stopping the host (it
was SIGKILLed or OOM-killed), the pipe closes and the host shuts down instead of running on,
orphaned, until someone kills it.
"""

import os
import sys
import time
import socket
import shutil
import pathlib
import tempfile
import subprocess

import pytest

from tests.api_host._helpers import make_env, sandbox  # noqa: F401

if sys.platform == 'win32':
    pytest.skip("The host listens on a Unix socket here.", allow_module_level=True)


def _start_host(root, plugins, parent_pipe: bool):
    sock_dir = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-hp-'))
    sock = sock_dir / 'host.sock'
    env = make_env(root, plugins)
    if parent_pipe:
        env['MRSM_API_HOST_PARENT_PIPE'] = '1'
    else:
        env.pop('MRSM_API_HOST_PARENT_PIPE', None)
    log = open(root / f'host-parent-{parent_pipe}.log', 'wb')
    proc = subprocess.Popen(
        [sys.executable, '-m', 'meerschaum.api.host', '--socket', str(sock)],
        env=env,
        cwd=str(root),
        stdin=subprocess.PIPE,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    deadline = time.time() + 120
    while time.time() < deadline:
        if proc.poll() is not None:
            log.close()
            pytest.fail(f"The host exited early:\n{(root / log.name).read_text()[-3000:]}")
        if sock.exists():
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                client.connect(str(sock))
                break
            except OSError:
                pass
            finally:
                client.close()
        time.sleep(0.2)
    else:
        proc.kill()
        pytest.fail("The host did not start in time.")
    return proc, log, sock_dir


def _stop(proc, log, sock_dir):
    if proc.poll() is None:
        proc.kill()
        proc.wait(timeout=15)
    log.close()
    shutil.rmtree(sock_dir, ignore_errors=True)


def test_host_exits_when_its_parent_pipe_closes(sandbox):
    root, plugins = sandbox
    proc, log, sock_dir = _start_host(root, plugins, parent_pipe=True)
    try:
        ### What the kernel does when the server dies: the write end closes.
        proc.stdin.close()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            pytest.fail("The host kept running after its server's pipe closed.")
    finally:
        _stop(proc, log, sock_dir)


def test_host_without_the_parent_pipe_ignores_stdin(sandbox):
    """`python -m meerschaum.api.host` run by hand keeps serving whatever its stdin does."""
    root, plugins = sandbox
    proc, log, sock_dir = _start_host(root, plugins, parent_pipe=False)
    try:
        proc.stdin.close()
        with pytest.raises(subprocess.TimeoutExpired):
            proc.wait(timeout=3)
    finally:
        _stop(proc, log, sock_dir)
