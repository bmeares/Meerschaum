#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The job startup gate (D23) as the Python SDK sees it: `Job.start()` of an explicit job respects
the startup slots (`jobs:startup:*`), a job waiting at the gate reads as running and can be
stopped, and generated systemd units carry `RestartSec=5`.

The gate itself is `meerschaum-jobs` (`src/startup.rs`, tests in `crates/meerschaum-jobs/tests/`);
this is the façade's end of it.
"""

import time
import uuid

import pytest

import meerschaum as mrsm
import meerschaum.config as mrsm_config


def _name(prefix: str = 'w18jb') -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _wait_for(predicate, timeout: float = 40.0, interval: float = 0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


@pytest.fixture
def one_slot():
    """One startup slot, no jitter and no host-load checks, for the duration of a test."""
    ### Resolve the live dictionary now: `set_config()` (other tests) replaces the object, so a
    ### `from meerschaum.config import config` taken at import time can be a stale one.
    jobs = mrsm_config.config.setdefault('jobs', {})
    previous = jobs.get('startup')
    jobs['startup'] = {
        'enabled': True,
        'max_concurrent': 1,
        'slot_timeout_seconds': 120,
        'settle_seconds': 0,
        'jitter_seconds': 0,
        'max_load': 0,
        'min_free_memory_percent': 0,
    }
    yield
    if previous is None:
        jobs.pop('startup', None)
    else:
        jobs['startup'] = previous


def test_a_job_waits_for_a_free_slot_and_can_be_stopped_there(one_slot):
    first = mrsm.Job(_name(), ['sh', 'sleep', '8'], executor_keys='local')
    second = mrsm.Job(_name(), ['sh', 'echo', 'hi'], executor_keys='local')
    try:
        assert first.start()[0]
        assert _wait_for(lambda: first.began is not None)

        began = time.monotonic()
        success, msg = second.start()
        assert success, msg
        time.sleep(1.5)
        ### Waiting at the gate: the job reads as running, and its worker has not begun.
        assert second.status == 'running'
        assert second.began is None

        ### The first job's worker is a plain `sleep` which never finishes a lap: the slot is
        ### freed when it exits, and only then does the second worker begin.
        assert _wait_for(lambda: second.began is not None)
        assert time.monotonic() - began >= 3.0
        assert _wait_for(lambda: second.status == 'stopped')
        assert second.result[0]
    finally:
        for job in (first, second):
            try:
                job.stop()
                job.delete()
            except Exception:
                pass


def test_a_job_stopped_at_the_gate_never_runs(one_slot):
    holder = mrsm.Job(_name(), ['sh', 'sleep', '30'], executor_keys='local')
    waiter = mrsm.Job(_name(), ['sh', 'echo', 'never'], executor_keys='local')
    try:
        assert holder.start()[0]
        assert _wait_for(lambda: holder.began is not None)
        assert waiter.start()[0]
        time.sleep(1.0)
        assert waiter.status == 'running'
        success, msg = waiter.stop()
        assert success, msg
        assert _wait_for(lambda: waiter.status == 'stopped')
        assert waiter.began is None
        assert 'never' not in (waiter.get_logs() or '')
    finally:
        for job in (holder, waiter):
            try:
                job.stop()
                job.delete()
            except Exception:
                pass


def test_systemd_units_space_out_their_restarts():
    from meerschaum.jobs.systemd import SystemdExecutor
    text = SystemdExecutor('systemd').get_service_file_text('x', ['show', 'version'])
    assert 'Restart=always\nRestartSec=5\n' in text
    ### `RandomizedDelaySec` is a timer option: systemd ignores it (with a warning) in a service.
    assert 'RandomizedDelaySec' not in text
