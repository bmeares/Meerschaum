#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Test Meerschaum jobs.
"""

import time
import pytest

import meerschaum as mrsm


def test_create_job():
    """
    Test creating and running a new job.
    """
    sysargs = ['show', 'version', ':', '--loop', '--min-seconds', '0.1']
    job = mrsm.Job(
        'test', sysargs, executor_keys='local'
    )
    job.delete()
    success, msg = job.start()
    assert success, msg

    duration = 1.0
    time.sleep(duration)

    success, msg = job.stop()
    assert success, msg

    success, msg = job.result
    assert success, msg

    output_text = job.get_logs()
    assert output_text.count("Meerschaum v") > 1

    success, msg = job.delete()
    assert success, msg
