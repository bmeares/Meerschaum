#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Test Meerschaum jobs through the `meerschaum.jobs.Job` façade (native: `meerschaum-jobs`).

The jobs run `<self> <sysargs>` under the native supervisor, where `<self>` is `python -m
meerschaum` (the extension's own CLI) or the `MRSM_BIN` binary. Every job gets a unique name and is deleted afterwards.
"""

import asyncio
import os
import re
import signal
import sys
import time
import uuid

import pytest

import meerschaum as mrsm
from meerschaum.jobs import Job, StopMonitoringLogs


LOOP = ['show', 'version', ':', '--loop', '--min-seconds', '0.2']


def _name(prefix: str = 'w16s') -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _wait_for(predicate, timeout: float = 30.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


@pytest.fixture
def make_job():
    """Build jobs with unique names; every job is stopped and deleted at the end."""
    jobs = []

    def _make(sysargs, name=None, **kwargs):
        kwargs.setdefault('executor_keys', 'local')
        job = Job(name or _name(), sysargs, **kwargs)
        jobs.append(job)
        return job

    yield _make
    for job in jobs:
        try:
            if job.status != 'stopped':
                job._core.stop(2)
                if job.status != 'stopped' and job.pid:
                    os.kill(job.pid, signal.SIGKILL)
            job.delete()
        except Exception:
            pass


def test_create_job(make_job):
    """
    Test creating and running a new job.
    """
    job = make_job(LOOP)
    job.delete()
    success, msg = job.start()
    assert success, msg

    assert _wait_for(lambda: (job.get_logs() or '').count("Meerschaum v") > 1)

    success, msg = job.stop()
    assert success, msg
    assert _wait_for(lambda: job.status == 'stopped')

    success, msg = job.result
    assert success, msg

    output_text = job.get_logs()
    assert output_text.count("Meerschaum v") > 1


def test_lifecycle_start_pause_resume_stop_delete(make_job):
    job = make_job(LOOP)
    assert not job.exists()
    assert job.status == 'stopped'
    assert job.restart is True
    assert job.hidden is False

    success, msg = job.start()
    assert success, msg
    assert _wait_for(lambda: job.status == 'running'), job.get_logs()
    assert job.exists()
    pid = job.pid
    assert isinstance(pid, int) and pid > 0
    assert job.is_running()
    ### The supervisor records `began` just after it writes the PID file.
    assert _wait_for(lambda: job.began is not None)
    assert job.ended is None

    ### Starting a running job is a no-op.
    success, msg = job.start()
    assert success and 'already running' in msg

    success, msg = job.pause()
    assert success, msg
    assert job.status == 'paused'
    assert job.paused is not None

    success, msg = job.resume()
    assert success, msg
    assert _wait_for(lambda: job.status == 'running')

    success, msg = job.stop()
    assert success, msg
    assert _wait_for(lambda: job.status == 'stopped')
    ### A manual stop is remembered: a restart job is not restarted behind the user's back.
    assert job.stop_time is not None
    success, msg = job.check_restart()
    assert success and 'manually stopped' in msg
    assert job.status == 'stopped'
    assert job.ended is not None

    success, msg = job.delete()
    assert success, msg
    assert not job.exists()


def test_completed_job_result_logs_and_no_process_scans(make_job):
    from meerschaum import _core
    job = make_job(['show', 'version'])
    success, msg = job.start()
    assert success, msg
    assert _wait_for(lambda: job.status == 'stopped' and job.ended is not None)

    success, msg = job.result
    assert success, msg
    assert 'Meerschaum v' in (job.get_logs() or '')
    assert job.began is not None and job.ended >= job.began
    assert job.restart is False
    assert job.pid is None

    ### A job which ended has no process: reading it never enumerates the process table.
    scans = _core.jobs_cmdline_scans()
    for _ in range(25):
        assert job.status == 'stopped'
        assert job.pid is None
        assert job.result[0]
        assert job.exists()
    assert _core.jobs_cmdline_scans() == scans


def test_restart_policy_restarts_a_job_which_died(make_job):
    job = make_job(LOOP)
    assert job.start()[0]
    assert _wait_for(lambda: job.status == 'running')
    pid = job.pid
    os.kill(pid, signal.SIGKILL)
    assert _wait_for(lambda: job.status == 'stopped')
    ### Nobody asked for the stop, so a restart job is started again.
    success, msg = job.check_restart()
    assert success, msg
    assert _wait_for(lambda: job.status == 'running')
    assert job.pid != pid


def test_delete_after_completion(make_job):
    job = make_job(['show', 'version'], delete_after_completion=True)
    assert job.delete_after_completion is True
    assert job.start()[0]
    assert _wait_for(lambda: not job.exists(), timeout=30)


def test_sysargs_label_env_and_hidden(make_job):
    job = make_job('sync pipes -d : --loop + show version', env={'W16S_VAR': 'x'})
    assert job.sysargs == ['sync', 'pipes', ':', '--loop', '+', 'show', 'version']
    assert job.label == 'sync pipes\n: --loop\n+ show version'
    assert job.restart is True
    env = job.env
    assert env['W16S_VAR'] == 'x'
    assert env['PYTHONUNBUFFERED'] == '1'
    assert 'LINES' in env and 'COLUMNS' in env
    assert str(job) == 'Job("%s", "sync pipes : --loop + show version")' % job.name
    assert Job('_hidden-w16s', 'show version', executor_keys='local').hidden is True
    assert hash(job) == hash(job.name)


@pytest.mark.parametrize('name', ['a;b', 'a,b', "a'b", 'a=b', 'a b*'])
def test_invalid_names(name):
    with pytest.raises(ValueError, match='is not allowed'):
        Job(name, 'show version', executor_keys='local')


@pytest.mark.parametrize('name', ['..', '.', 'a/b', ''])
def test_path_like_names_are_refused(name):
    with pytest.raises(ValueError):
        Job(name, 'show version', executor_keys='local')


def test_stop_with_a_zero_timeout_does_not_wait(make_job):
    job = make_job(LOOP)
    assert job.start()[0]
    assert _wait_for(lambda: job.status == 'running')
    success, msg = job.stop(timeout_seconds=0)
    assert success, msg
    assert _wait_for(lambda: job.status == 'stopped')


def test_monitor_logs_follows_a_job_until_it_stops(make_job):
    lines = []
    stopped = []
    job = make_job(['show', 'version'])
    assert job.start()[0]
    job.monitor_logs(
        callback_function=lines.append,
        stop_callback_function=stopped.append,
        stop_on_exit=True,
        accept_input=False,
    )
    assert any('Meerschaum v' in line for line in lines), lines
    assert stopped and stopped[0][0] is True


def test_monitor_logs_stops_on_the_exception(make_job):
    job = make_job(LOOP)
    assert job.start()[0]
    assert _wait_for(lambda: (job.get_logs() or '').count('Meerschaum v') >= 1)
    seen = []

    def callback(line):
        seen.append(line)
        raise StopMonitoringLogs

    started = time.monotonic()
    job.monitor_logs(callback_function=callback, accept_input=False)
    assert len(seen) == 1
    assert time.monotonic() - started < 10


def test_monitor_logs_reports_other_callback_errors_and_goes_on(make_job):
    job = make_job(['show', 'version'])
    assert job.start()[0]
    assert _wait_for(lambda: job.status == 'stopped')
    calls = []

    def callback(line):
        calls.append(line)
        if len(calls) == 1:
            raise ValueError('callback failed')

    with pytest.warns(UserWarning, match='callback failed'):
        job.monitor_logs(callback_function=callback, stop_on_exit=True, accept_input=False)
    assert len(calls) > 1


def test_monitor_logs_stop_event(make_job):
    job = make_job(LOOP)
    assert job.start()[0]
    assert _wait_for(lambda: (job.get_logs() or '').count('Meerschaum v') >= 1)
    event = asyncio.Event()
    lines = []

    async def run():
        async def stop_later():
            await asyncio.sleep(1.0)
            event.set()

        task = asyncio.create_task(stop_later())
        await job.monitor_logs_async(
            callback_function=lines.append,
            stop_event=event,
            accept_input=False,
        )
        await task

    started = time.monotonic()
    asyncio.run(run())
    assert event.is_set()
    assert lines
    assert time.monotonic() - started < 15


def test_monitor_logs_async_awaits_coroutine_callbacks(make_job):
    job = make_job(['show', 'version'])
    assert job.start()[0]
    lines = []
    results = []

    async def on_line(line):
        lines.append(line)

    async def on_stop(result):
        results.append(result)

    asyncio.run(job.monitor_logs_async(
        callback_function=on_line,
        stop_callback_function=on_stop,
        stop_on_exit=True,
        accept_input=False,
    ))
    assert any('Meerschaum v' in line for line in lines)
    assert results and results[0][0] is True


def test_monitor_logs_strips_timestamps(make_job):
    job = make_job(['show', 'version'])
    assert job.start()[0]
    assert _wait_for(lambda: job.status == 'stopped')
    plain = []
    job.monitor_logs(callback_function=plain.append, stop_on_exit=True, accept_input=False,
                     strip_timestamps=True)
    stamped = []
    job.monitor_logs(callback_function=stamped.append, stop_on_exit=True, accept_input=False)
    assert plain and stamped
    assert any(' | ' in line for line in stamped)
    assert not any(re.match(r'^\d{4}-\d\d-\d\d \d\d:\d\d \| ', line) for line in plain)


def test_stdin_and_prompt_state_of_an_idle_job(make_job):
    job = make_job(['show', 'version'])
    ### A job which is not running blocks on nothing and has no prompt.
    assert job.is_blocking_on_stdin() is False
    assert job.get_prompt_kwargs() == {}


def test_jobs_run_this_process_own_command(monkeypatch, tmp_path):
    """Jobs run `python -P -m meerschaum` (the extension's own CLI) unless `MRSM_BIN` says otherwise."""
    from meerschaum import _core
    from meerschaum.jobs import _native
    monkeypatch.delenv('MRSM_JOB_SUPERVISOR', raising=False)
    monkeypatch.delenv('MRSM_JOB_PROGRAM', raising=False)
    monkeypatch.setenv('MRSM_BIN', str(tmp_path / 'missing'))
    _native.core()
    supervisor, program = _core.jobs_launcher()
    assert program == [sys.executable, '-P', '-m', 'meerschaum']
    assert supervisor == [*program, '__mrsm_job_supervisor__']

    ### An executable program named by `MRSM_BIN` replaces it; a script named `mrsm` does not.
    shim = tmp_path / 'mrsm'
    shim.write_text('#!/bin/sh\n')
    shim.chmod(0o755)
    monkeypatch.setenv('MRSM_BIN', str(shim))
    assert _core.jobs_launcher()[1] == [sys.executable, '-P', '-m', 'meerschaum']
    program_file = tmp_path / 'prog'
    program_file.write_bytes(b'\x7fELF')
    program_file.chmod(0o755)
    monkeypatch.setenv('MRSM_BIN', str(program_file))
    assert _core.jobs_launcher()[1] == [str(program_file)]
