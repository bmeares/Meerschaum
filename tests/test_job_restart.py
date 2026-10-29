#! /usr/bin/env python3
# vim:fenc=utf-8

"""
A job can be started again right after it was stopped.
"""

import time
import uuid


def test_a_job_can_be_started_again_right_after_it_was_stopped():
    """
    `Job.stop()` returns once the job is really stopped, so the `start()` which follows starts it
    (it was once told the job "is already running" while the old supervisor finished up).
    """
    import meerschaum as mrsm

    ### `mrsm sh sleep` runs a `sleep` of its own, which keeps the supervisor's output pipes
    ### open after the worker is gone: the supervisor outlives its worker for a while.
    job = mrsm.Job(f'test-restart-{uuid.uuid4().hex[:12]}', ['sh', 'sleep', '8'])
    try:
        for _ in range(2):
            success, msg = job.start()
            assert success, msg
            assert 'already running' not in msg, msg
            deadline = time.time() + 30
            while job.status != 'running' and time.time() < deadline:
                time.sleep(0.1)
            assert job.status == 'running'

            success, msg = job.stop()
            assert success, msg
            assert job.status == 'stopped'
    finally:
        job.stop()
        job.delete()
