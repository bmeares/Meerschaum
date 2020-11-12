#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define a custom Thread class with a callback method
"""
import threading
class Thread(threading.Thread):
    def __init__(self, callback=None, error_callback=None, *args, **kw):
        """
        Wrapper for threading.Thread with optional callback and error_callback functions
        """
        target = kw.pop('target')
        super().__init__(target=self.wrap_target_with_callback, *args, **kw)
        self.callback = callback
        self.error_callback = error_callback
        self.method = target

    def wrap_target_with_callback(self, *args, **kw):
        """
        Wrap the designated target function with a try-except.
        Captures the output and executes either the callback or error_callback.
        """
        try:
            result = self.method(*args, **kw)
            success = True
        except:
            success = False
            result = None

        cb = self.callback if success else self.error_callback
        if cb is not None: cb(result)

