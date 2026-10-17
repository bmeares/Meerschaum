#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Test that reading a job's status does not walk the process table
unless a job might be running without a PID file.
"""

import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import meerschaum as mrsm
from meerschaum.jobs import Job, check_restart_jobs, get_jobs
from meerschaum.utils.daemon import Daemon, _detached
from meerschaum.utils.packages import attempt_import


def _ts(seconds_ago: float) -> str:
    return (
        datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=seconds_ago)
    ).isoformat()


@pytest.fixture
def scans(monkeypatch):
    """
    Count the walks of the process table.
    """
    psutil = attempt_import('psutil')
    counter = SimpleNamespace(count=0)
    real_process_iter = psutil.process_iter

    def counting_process_iter(*args, **kwargs):
        counter.count += 1
        return real_process_iter(*args, **kwargs)

    monkeypatch.setattr(psutil, 'process_iter', counting_process_iter)
    _detached.invalidate()
    yield counter
    _detached.invalidate()


@pytest.fixture
def prefix():
    """
    A unique prefix for the job names of a test, and clean up the jobs afterwards.
    """
    value = f"scantest-{uuid.uuid4().hex[:8]}-"
    yield value
    from meerschaum.utils.daemon import get_daemon_ids
    for daemon_id in get_daemon_ids():
        if daemon_id.startswith(value):
            Daemon(daemon_id=daemon_id).cleanup()


def _write_job(
    name: str,
    process: dict,
    restart: bool = False,
    result=None,
) -> Daemon:
    """
    Write the files of a job without running it.
    """
    from meerschaum._internal.entry import entry
    properties = {'process': process, 'restart': restart}
    if result is not None:
        properties['result'] = result
    daemon = Daemon(
        target=entry,
        target_args=[['show', 'version']],
        target_kw={},
        daemon_id=name,
        label='show version',
        properties=properties,
    )
    daemon.path.mkdir(parents=True, exist_ok=True)
    assert daemon.write_properties()[0]
    assert daemon.write_pickle()[0]
    return daemon


def _write_completed_job(name: str, restart: bool = False) -> Daemon:
    return _write_job(
        name,
        {'began': _ts(100), 'ended': _ts(90)},
        restart=restart,
        result=[True, 'Done.'],
    )


def test_completed_jobs_do_not_scan(scans, prefix):
    names = [f"{prefix}{i}" for i in range(20)]
    for name in names:
        _write_completed_job(name)

    for _ in range(3):
        success, msg = check_restart_jobs('local', silent=True)
        assert success, msg

    jobs = get_jobs('local', include_hidden=True, combine_local_and_systemd=False)
    for name in names:
        job = jobs[name]
        assert job.status == 'stopped'
        assert job.pid is None
        assert name in str(job) and name in repr(job)
        assert job.began is not None
        assert job.ended is not None
        assert job.result == (True, 'Done.')
        assert Job(name, executor_keys='local').status == 'stopped'
        assert Daemon(daemon_id=name).status == 'stopped'

    assert scans.count == 0


def test_job_without_files_does_not_scan(scans, prefix):
    job = Job(f"{prefix}missing", ['show', 'version'], executor_keys='local')
    assert job.status == 'stopped'
    assert job.pid is None
    assert str(job)
    assert scans.count == 0


def test_started_jobs_scan_once_for_all_jobs(scans, prefix):
    """
    Jobs which began but did not record an end (e.g. killed with `SIGKILL`) may be running
    without a PID file, but all of them are found with one scan, and not looked for again.
    """
    names = [f"{prefix}{i}" for i in range(10)]
    for name in names:
        _write_job(name, {'began': _ts(100)})

    for name in names:
        assert Job(name, executor_keys='local').status == 'stopped'
    assert scans.count == 1

    for _ in range(3):
        check_restart_jobs('local', silent=True)
        for name in names:
            assert Daemon(daemon_id=name).status == 'stopped'
    assert scans.count == 1


def test_negative_result_expires(scans, prefix, monkeypatch):
    name = f"{prefix}expiring"
    _write_job(name, {'began': _ts(100)})
    assert Daemon(daemon_id=name).pid is None
    assert Daemon(daemon_id=name).pid is None
    assert scans.count == 1

    monkeypatch.setattr(_detached, 'NEGATIVE_TTL_SECONDS', 0.0)
    time.sleep(0.01)
    monkeypatch.setattr(_detached, 'INDEX_TTL_SECONDS', 0.0)
    assert Daemon(daemon_id=name).pid is None
    assert scans.count == 2


def test_negative_result_cleared_by_a_new_launch(scans, prefix):
    name = f"{prefix}relaunched"
    daemon = _write_job(name, {'began': _ts(100)})
    assert daemon.pid is None
    assert scans.count == 1
    assert daemon.pid is None
    assert scans.count == 1

    ### Another process starting the job changes its recorded timestamps.
    daemon = _write_job(name, {'began': _ts(100), 'launched': _ts(0)})
    _ = Daemon(daemon_id=name).pid
    assert scans.count == 2


def _fake_process_table(monkeypatch, entries):
    """
    Replace the process table with `(pid, cmdline)` entries.
    """
    psutil = attempt_import('psutil')

    def process_iter(attrs=None, *args, **kwargs):
        for pid, cmdline in entries:
            yield SimpleNamespace(info={'pid': pid, 'cmdline': cmdline})

    monkeypatch.setattr(psutil, 'process_iter', process_iter)


def test_relaunched_job_is_found_before_its_pid_file_exists(prefix, monkeypatch):
    """
    A job which completed earlier and was just started has a stale `ended`
    and no PID file until the new process has started up.
    It must not be reported as stopped (the supervisor would start it again).
    """
    name = f"{prefix}relaunch"
    _write_job(name, {'began': _ts(100), 'ended': _ts(90), 'launched': _ts(1)})
    ### The parent of the test is a live process which can stand in for the launcher.
    launcher_pid = os.getppid()
    _fake_process_table(
        monkeypatch,
        [(launcher_pid, ['python', '-c', f"Daemon(daemon_id='{name}')"])],
    )
    _detached.invalidate()
    daemon = Daemon(daemon_id=name)
    assert daemon.pid == launcher_pid
    assert daemon.status == 'running'
    ### The PID file is left for the daemon to write.
    assert not daemon.pid_path.exists()


def test_orphan_is_recovered_and_pid_file_restored(scans, prefix, monkeypatch):
    name = f"{prefix}orphan"
    _write_job(name, {'began': _ts(100)})
    orphan_pid = os.getppid()
    _fake_process_table(
        monkeypatch,
        [(orphan_pid, ['python', '-c', f"Daemon(daemon_id='{name}')"])],
    )
    ### The parent started long before the job `began`, so it is a valid orphan.
    monkeypatch.setattr(
        _detached,
        'is_stale_process',
        lambda pid, moment, slack_seconds: False,
    )
    _detached.invalidate()
    daemon = Daemon(daemon_id=name)
    assert not daemon.pid_path.exists()
    assert daemon.pid == orphan_pid
    assert daemon.pid_path.exists()
    assert daemon.pid_path.read_text(encoding='utf-8') == str(orphan_pid)
    assert daemon.status == 'running'


def test_reused_pid_is_not_an_orphan(prefix, monkeypatch):
    """
    A process which was created after the job recorded that it began is not the job.
    """
    name = f"{prefix}reused"
    ### The job began before this test process was created.
    _write_job(name, {'began': _ts(10 * 365 * 24 * 3600)})
    _fake_process_table(
        monkeypatch,
        [(os.getppid(), ['python', '-c', f"Daemon(daemon_id='{name}')"])],
    )
    _detached.invalidate()
    daemon = Daemon(daemon_id=name)
    assert daemon.pid is None
    assert daemon.status == 'stopped'
    assert not daemon.pid_path.exists()


def test_scan_ignores_other_daemon_ids(prefix, monkeypatch):
    name = f"{prefix}exact"
    _write_job(name, {'began': _ts(100)})
    _fake_process_table(
        monkeypatch,
        [(os.getppid(), ['python', '-c', f"Daemon(daemon_id='{name}-other')"])],
    )
    _detached.invalidate()
    assert Daemon(daemon_id=name).pid is None


def test_running_job_with_lost_pid_file_is_recovered_and_stopped(scans, prefix):
    """
    Start a real daemon, delete its PID file, and check that it is still found and stoppable.
    """
    name = f"{prefix}live"
    daemon = Daemon(
        target=time.sleep,
        target_args=[120],
        target_kw={},
        daemon_id=name,
        properties={'logs': {'stdin': False}},
    )
    try:
        success, msg = daemon.run(wait=True, timeout=30)
        assert success, msg
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if (daemon.read_properties() or {}).get('process', {}).get('began'):
                break
            time.sleep(0.1)
        assert daemon.pid_path.exists()
        real_pid = daemon.pid
        assert real_pid is not None

        daemon.pid_path.unlink()
        _detached.invalidate()
        scans.count = 0

        recovered = Daemon(daemon_id=name)
        assert recovered.pid == real_pid
        assert recovered.status == 'running'
        assert scans.count <= 1
        assert recovered.pid_path.exists()

        scans_after_recovery = scans.count
        for _ in range(5):
            assert recovered.status == 'running'
            assert Daemon(daemon_id=name).pid == real_pid
        assert scans.count == scans_after_recovery

        ### Lose the PID file again and stop the job without it.
        recovered.pid_path.unlink()
        success, msg = Daemon(daemon_id=name).quit(timeout=15)
        assert success, msg

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and Daemon(daemon_id=name).status != 'stopped':
            time.sleep(0.2)
        assert Daemon(daemon_id=name).status == 'stopped'
    finally:
        _detached.invalidate()
        if Daemon(daemon_id=name).status != 'stopped':
            Daemon(daemon_id=name).kill(timeout=5)
        daemon.cleanup()


def test_check_restart_policy(scans, prefix, monkeypatch):
    """
    Completed jobs with `--restart` are started again, unless they were stopped by hand.
    """
    restarts = []

    def fake_start(self, debug=False):
        restarts.append(self.name)
        return True, f"Started {self}."

    monkeypatch.setattr(Job, 'start', fake_start)

    restart_name = f"{prefix}restart"
    stopped_name = f"{prefix}stopped"
    once_name = f"{prefix}once"
    _write_completed_job(restart_name, restart=True)
    stopped = _write_completed_job(stopped_name, restart=True)
    assert stopped._write_stop_file('quit')[0]
    _write_completed_job(once_name, restart=False)

    success, msg = check_restart_jobs('local', silent=True)
    assert success, msg
    mine = [name for name in restarts if name.startswith(prefix)]
    assert mine == [restart_name]
    assert scans.count == 0

    for name, expected in (
        (restart_name, (True, None)),
        (stopped_name, (True, 'manually stopped')),
        (once_name, (True, 'does not need to be restarted')),
    ):
        success, msg = Job(name, executor_keys='local').check_restart()
        assert success is expected[0]
        if expected[1]:
            assert expected[1] in msg
