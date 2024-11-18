#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define a custom Thread class with a callback method.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional

import threading
Lock = threading.Lock
RLock = threading.RLock
Event = threading.Event
Timer = threading.Timer
get_ident = threading.get_ident

class Thread(threading.Thread):
    """Wrapper for threading.Thread with optional callback and error_callback functions."""

    def __init__(self, *args, callback=None, error_callback=None, **kw):
        target = kw.pop('target')
        super().__init__(target=self.wrap_target_with_callback, *args, **kw)
        self.callback = callback
        self.error_callback = error_callback
        self.method = target
        self._return = None

    def wrap_target_with_callback(self, *args, **kw):
        """Wrap the designated target function with a try-except.
        Captures the output and executes either the callback or error_callback.
        """
        try:
            result = self.method(*args, **kw)
            success = True
        except Exception as e:
            success = False
            result = e

        cb = self.callback if success else self.error_callback
        if cb is not None:
            cb(result)
        return result

    def join(self, timeout: Optional[float] = None):
        """
        Join the thread with an optional timeout.
        """
        threading.Thread.join(self, timeout=timeout)
        return self._return

    def run(self):
        """Set the return to the result of the target."""
        self._return = self._target(*self._args, **self._kwargs)


class Worker(threading.Thread):
    """Wrapper for `threading.Thread` for working with `queue.Queue` objects."""

    def __init__(self, queue, *args, timeout: int = 3, **kw):
        self.queue = queue
        self.timeout = timeout
        super().__init__(*args, **kw)

    def run(self):
        while True:
            try:
                _ = self.queue.get(timeout=self.timeout)
            except self.queue.Empty:
                return None

            self.queue.task_done()


class RepeatTimer(Timer):
    """
    Fire the timer's target function in a loop, every `interval` seconds.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_running = False

    def is_running(self) -> bool:
        """
        Return whether this timer has been started and is running.
        """
        return self._is_running

    def run(self) -> None:
        """
        Fire the target function in a loop.
        """
        self._is_running = True
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)
        self._is_running = False
