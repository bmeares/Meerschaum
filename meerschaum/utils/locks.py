#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Cross-process file locks built on the standard library.

This module is imported while the `mrsm` virtual environment is still being
bootstrapped, so it must not depend on anything outside of the standard library.
"""

import pathlib
import platform
import threading

__all__ = ('InterProcessLock',)

_IS_WINDOWS: bool = platform.system() == 'Windows'


class InterProcessLock:
    """
    A reusable lock on a file which is honored across processes.

    Locking is performed with `fcntl.flock()` on POSIX systems and `msvcrt.locking()`
    on Windows. Acquiring a lock which this object already holds is a no-op, and the
    file is only unlocked once the outermost `release()` is called.

    Examples
    --------
    >>> from meerschaum.utils.locks import InterProcessLock
    >>> lock = InterProcessLock('/tmp/foo.lock')
    >>> with lock:
    ...     pass
    >>> lock.acquire(blocking=False)
    True
    >>> lock.release()
    """

    def __init__(self, path: 'pathlib.Path | str'):
        self.path = pathlib.Path(path)
        self._lock_file = None
        self._depth = 0
        self._thread_lock = threading.RLock()

    def acquire(self, blocking: bool = True) -> bool:
        """
        Lock the file, returning whether the lock is held.

        Parameters
        ----------
        blocking: bool, default True
            If `False`, return `False` instead of waiting for another process
            to release the lock.
        """
        with self._thread_lock:
            if self._depth:
                self._depth += 1
                return True

            self.path.parent.mkdir(parents=True, exist_ok=True)
            lock_file = open(self.path, 'a+b')
            try:
                acquired = (
                    _lock_windows(lock_file, blocking)
                    if _IS_WINDOWS
                    else _lock_posix(lock_file, blocking)
                )
            except BaseException:
                lock_file.close()
                raise

            if not acquired:
                lock_file.close()
                return False

            self._lock_file = lock_file
            self._depth = 1
            return True

    def release(self) -> None:
        """Unlock the file once the outermost lock is released."""
        with self._thread_lock:
            if not self._depth:
                return

            self._depth -= 1
            if self._depth:
                return

            lock_file, self._lock_file = self._lock_file, None
            if lock_file is None:
                return

            try:
                if _IS_WINDOWS:
                    import msvcrt
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            finally:
                lock_file.close()

    def __enter__(self) -> 'InterProcessLock':
        self.acquire()
        return self

    def __exit__(self, *args) -> None:
        self.release()

    def __repr__(self) -> str:
        return f"InterProcessLock('{self.path}')"


def _lock_posix(lock_file, blocking: bool) -> bool:
    """Lock a file with `fcntl.flock()`."""
    import fcntl
    flags = fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB)
    try:
        fcntl.flock(lock_file.fileno(), flags)
    except OSError:
        if blocking:
            raise
        return False
    return True


def _lock_windows(lock_file, blocking: bool) -> bool:
    """Lock the first byte of a file with `msvcrt.locking()`."""
    import msvcrt
    import time

    ### `msvcrt` cannot lock a byte which does not exist.
    lock_file.seek(0)
    if lock_file.read(1) == b'':
        lock_file.write(b'\0')
        lock_file.flush()

    while True:
        try:
            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            if not blocking:
                return False
            time.sleep(0.05)
