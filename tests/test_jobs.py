#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Test Meerschaum jobs.
"""

import pytest
import time

import meerschaum as mrsm


def test_create_job():
    """
    Test creating and running a new job.
    """
    sysargs = ['show', 'version', '-s', 'every 1 second']
    job = mrsm.Job('test', sysargs)
    job.delete()
    success, msg = job.start()
    assert success, msg

    duration = 3
    loop_begin_ts = time.perf_counter()
    while True:

        if (time.perf_counter() - loop_begin_ts) >= duration:
            break

    success, msg = job.stop()
    assert success, msg

    success, msg = job.result
    assert success, msg
