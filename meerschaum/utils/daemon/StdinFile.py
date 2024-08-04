#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create a file manager to pass STDIN to the Daemon.
"""

import io
import pathlib
import time
import os
import selectors


class StdinFile(io.TextIOBase):
    """
    Redirect user input into a Daemon's context.
    """
    def __init__(self, file_path: pathlib.Path):
        self.file_path = file_path
        self._file_handler = None
        self._fd = None
        self.sel = selectors.DefaultSelector()

    @property
    def file_handler(self):
        """
        Return the read file handler to the provided file path.
        """
        if self._file_handler is not None:
            return self._file_handler

        if self.file_path.exists():
            self.file_path.unlink()

        os.mkfifo(self.file_path.as_posix(), mode=0o600)

        self._fd = os.open(self.file_path, os.O_RDONLY | os.O_NONBLOCK)
        self._file_handler = os.fdopen(self._fd, 'rb', buffering=0)
        self.sel.register(self._file_handler, selectors.EVENT_READ)
        return self._file_handler

    def fileno(self):
        fileno = self.file_handler.fileno()
        return fileno

    def read(self, size=-1):
        _ = self.file_handler
        while True:
            try:
                events = self.sel.select(timeout=0)
                for key, _ in events:
                    data = key.fileobj.read(size)
                    if data:
                        return data.decode('utf-8')

            except (OSError, EOFError):
                pass

            time.sleep(0.1)

    def readline(self, size=-1):
        line = ''
        while True:
            try:
                data = self.read(1)
            except Exception:
                return ''
            if not data or data == '\n':
                break
            line += data

        return line

    def close(self):
        if self._file_handler is not None:
            self.sel.unregister(self._file_handler)
            self._file_handler.close()
            os.close(self._fd)
            self._file_handler = None
            self._fd = None

        super().close()

    def is_open(self):
        return self._file_handler is not None
