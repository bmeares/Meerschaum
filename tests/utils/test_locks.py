#! /usr/bin/env python3

"""Tests for the standard-library inter-process lock."""

import subprocess
import sys
import textwrap

from meerschaum.utils.locks import InterProcessLock


def test_lock_is_held_against_another_process(tmp_path):
    """A locked file cannot be locked by another process."""
    lock_path = tmp_path / 'held.lock'
    script = textwrap.dedent(
        f"""
        from meerschaum.utils.locks import InterProcessLock
        lock = InterProcessLock({str(lock_path)!r})
        print('acquired' if lock.acquire(blocking=False) else 'blocked')
        """
    )

    with InterProcessLock(lock_path):
        blocked = subprocess.run(
            [sys.executable, '-c', script],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

    ### The lock is released, so the same check now succeeds.
    acquired = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    assert blocked == 'blocked'
    assert acquired == 'acquired'


def test_lock_is_reusable_and_reentrant(tmp_path):
    """The same lock object may be entered again and reused after release."""
    lock = InterProcessLock(tmp_path / 'nested.lock')

    with lock:
        with lock:
            assert lock.acquire(blocking=False)
            lock.release()

    ### Releasing more than acquiring must not raise.
    lock.release()

    with lock:
        pass


def test_lock_creates_missing_parent_directories(tmp_path):
    """The lock file's directory is created on demand."""
    lock_path = tmp_path / 'nested' / 'dir' / 'created.lock'

    with InterProcessLock(lock_path):
        assert lock_path.exists()
