#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Test that a job is only matched with processes of its own root directory.

A job's process is found (when its PID file is lost) by the `daemon_id` in its command line,
but a job with the same name under another `MRSM_ROOT_DIR` is a different job.
"""

import os
import pathlib
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from meerschaum.utils.daemon import Daemon, _detached
from meerschaum.utils.packages import attempt_import


def _now() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _write_job(name: str, process: dict) -> Daemon:
    from meerschaum._internal.entry import entry
    daemon = Daemon(
        target=entry,
        target_args=[['show', 'version']],
        target_kw={},
        daemon_id=name,
        label='show version',
        properties={'process': process},
    )
    daemon.path.mkdir(parents=True, exist_ok=True)
    assert daemon.write_properties()[0]
    assert daemon.write_pickle()[0]
    return daemon


@pytest.fixture
def name():
    value = f"roottest-{uuid.uuid4().hex[:8]}"
    yield value
    try:
        Daemon(daemon_id=value).cleanup()
    except Exception:
        pass


@pytest.fixture
def roots(tmp_path, monkeypatch):
    """
    Two root directories: this process's own (A) and another (B).
    The test process switches between them by patching the root it filters on.
    """
    import meerschaum.config.paths as paths
    root_a = pathlib.Path(paths.ROOT_DIR_PATH).resolve()
    root_b = (tmp_path / 'other-root').resolve()
    root_b.mkdir()
    _detached.invalidate()
    spawned = []

    def spawn(name: str, root=None) -> subprocess.Popen:
        """
        Start a stand-in daemon: its command line holds the `daemon_id`, and its environment
        holds the given root (or no root at all for the default root).
        """
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith('MRSM_')
        }
        if root is not None:
            env['MRSM_ROOT_DIR'] = str(root)
        code = f"import time; marker = \"daemon_id='{name}'\"; time.sleep(600)"
        proc = subprocess.Popen([sys.executable, '-c', code], env=env)
        spawned.append(proc)
        ### Wait until the process has replaced itself with the interpreter
        ### so that its environment is the one given.
        deadline = time.monotonic() + 15
        psutil = attempt_import('psutil')
        while time.monotonic() < deadline:
            try:
                if name in ' '.join(psutil.Process(proc.pid).cmdline()):
                    break
            except Exception:
                pass
            time.sleep(0.05)
        return proc

    def be(root: pathlib.Path):
        monkeypatch.setattr(_detached, '_get_root', lambda: root.as_posix())
        _detached.invalidate()

    yield SimpleNamespace(a=root_a, b=root_b, spawn=spawn, be=be)

    for proc in spawned:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)
    _detached.invalidate()


def _alive(proc: subprocess.Popen) -> bool:
    return proc.poll() is None


def _wait_dead(proc: subprocess.Popen, timeout: float = 15.0) -> bool:
    try:
        proc.wait(timeout=timeout)
        return True
    except subprocess.TimeoutExpired:
        return False


def test_status_only_sees_its_own_root(roots, name):
    """
    A process of another root with the same `daemon_id` is not this job's process.
    """
    _write_job(name, {'began': _now()})
    proc_b = roots.spawn(name, roots.b)

    roots.be(roots.a)
    daemon = Daemon(daemon_id=name)
    assert daemon.pid is None
    assert daemon.status == 'stopped'
    assert not daemon.pid_path.exists()

    roots.be(roots.b)
    daemon = Daemon(daemon_id=name)
    assert daemon.pid == proc_b.pid
    assert daemon.status == 'running'


def test_unset_root_is_the_default_root(roots, name):
    """
    A process without `MRSM_ROOT_DIR` belongs to the default root, not to this one.
    """
    import meerschaum.config.paths as paths
    default_root = pathlib.Path(paths.DEFAULT_ROOT_DIR_PATH).resolve()
    assert default_root != roots.a
    _write_job(name, {'began': _now()})
    proc = roots.spawn(name, root=None)

    roots.be(roots.a)
    assert Daemon(daemon_id=name).pid is None

    roots.be(default_root)
    assert Daemon(daemon_id=name).pid == proc.pid


def test_kill_only_reaps_its_own_root(roots, name):
    _write_job(name, {'began': _now()})
    proc_a = roots.spawn(name, roots.a)
    proc_b = roots.spawn(name, roots.b)

    roots.be(roots.a)
    success, msg = Daemon(daemon_id=name).kill(timeout=15)
    assert success, msg
    assert 'Reaped 1 detached process' in msg, msg
    assert _wait_dead(proc_a)
    assert _alive(proc_b)

    roots.be(roots.b)
    success, msg = Daemon(daemon_id=name).kill(timeout=15)
    assert success, msg
    assert 'Reaped 1 detached process' in msg, msg
    assert _wait_dead(proc_b)


def test_quit_only_reaps_its_own_root(roots, name):
    _write_job(name, {'began': _now()})
    proc_a = roots.spawn(name, roots.a)
    proc_b = roots.spawn(name, roots.b)

    roots.be(roots.b)
    success, msg = Daemon(daemon_id=name).quit(timeout=15)
    assert success, msg
    assert _wait_dead(proc_b)
    assert _alive(proc_a)

    ### Nothing of this root is left to reap under the other root.
    roots.be(roots.b)
    success, msg = Daemon(daemon_id=name).quit(timeout=15)
    assert success, msg
    assert _alive(proc_a)

    roots.be(roots.a)
    success, msg = Daemon(daemon_id=name).quit(timeout=15)
    assert success, msg
    assert _wait_dead(proc_a)


def test_unreadable_or_relative_roots_are_kept(name, monkeypatch):
    """
    If the root of a process cannot be determined, keep the match (as before).
    """
    psutil = attempt_import('psutil')
    other_pid = os.getppid()

    def process(environ):
        def _environ():
            if isinstance(environ, Exception):
                raise environ
            return environ

        return SimpleNamespace(
            info={'pid': other_pid, 'cmdline': ['python', '-c', f"Daemon(daemon_id='{name}')"]},
            environ=_environ,
        )

    scenarios = {
        'denied': process(psutil.AccessDenied(other_pid)),
        'relative': process({'MRSM_ROOT_DIR': 'relative/root'}),
        'vanished': process(psutil.NoSuchProcess(other_pid)),
        'no environ method': SimpleNamespace(
            info={'pid': other_pid, 'cmdline': ['python', '-c', f"Daemon(daemon_id='{name}')"]},
        ),
    }
    for label, proc in scenarios.items():
        monkeypatch.setattr(psutil, 'process_iter', lambda *a, _proc=proc, **kw: iter([_proc]))
        _detached.invalidate()
        assert _detached.find_pids(name) == [other_pid], label

    foreign = process({'MRSM_ROOT_DIR': '/definitely/not/this/root'})
    monkeypatch.setattr(psutil, 'process_iter', lambda *a, **kw: iter([foreign]))
    _detached.invalidate()
    assert _detached.find_pids(name) == []

    own = process({'MRSM_ROOT_DIR': _detached._get_root()})
    monkeypatch.setattr(psutil, 'process_iter', lambda *a, **kw: iter([own]))
    _detached.invalidate()
    assert _detached.find_pids(name) == [other_pid]
