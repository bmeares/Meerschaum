#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define a custom Thread class with a callback method
"""
import threading
class Thread(threading.Thread):
    """
    Wrapper for threading.Thread with optional callback and error_callback functions
    """

    def __init__(self, *args, callback=None, error_callback=None, **kw):
        target = kw.pop('target')
        super().__init__(target=self.wrap_target_with_callback, *args, **kw)
        self.callback = callback
        self.error_callback = error_callback
        self.method = target
        self._return = None

    def wrap_target_with_callback(self, *args, **kw):
        """
        Wrap the designated target function with a try-except.
        Captures the output and executes either the callback or error_callback.
        """
        try:
            result = self.method(*args, **kw)
            success = True
        except Exception as e:
            success = False
            result = None

        cb = self.callback if success else self.error_callback
        if cb is not None:
            cb(result)

    def join(self):
        """
        Return the thread's return value upon joining.
        """
        threading.Thread.join(self)
        return self._return

    def run(self):
        """
        Set the return to the result of the target.
        """
        self._return = self._target(*self._args, **self._kwargs)
