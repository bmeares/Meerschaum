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
import traceback

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Union
from meerschaum.utils.warnings import warn


class StdinFile(io.TextIOBase):
    """
    Redirect user input into a Daemon's context.
    """
    def __init__(
        self,
        file_path: Union[pathlib.Path, str],
        lock_file_path: Optional[pathlib.Path] = None,
        decode: bool = True,
        refresh_seconds: Union[int, float, None] = None,
    ):
        if isinstance(file_path, str):
            file_path = pathlib.Path(file_path)

        self.file_path = file_path
        self.blocking_file_path = (
            lock_file_path
            if lock_file_path is not None
            else (file_path.parent / (file_path.name + '.block'))
        )
        self._file_handler = None
        self._fd = None
        self.sel = selectors.DefaultSelector()
        self.decode = decode
        self._write_fp = None
        self.refresh_seconds = (
            refresh_seconds
            if refresh_seconds is not None
            else mrsm.get_config('system', 'cli', 'refresh_seconds')
        )

    @property
    def encoding(self):
        return 'utf-8'

    @property
    def file_handler(self):
        """
        Return the read file handler to the provided file path.
        """
        if self._file_handler is not None:
            return self._file_handler

        if not self.file_path.exists():
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            os.mkfifo(self.file_path.as_posix(), mode=0o600)

        self._fd = os.open(self.file_path, os.O_RDONLY | os.O_NONBLOCK)
        self._file_handler = os.fdopen(self._fd, 'rb', buffering=0)
        self.sel.register(self._file_handler, selectors.EVENT_READ)
        return self._file_handler

    def write(self, data):
        if self._write_fp is None:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            if not self.file_path.exists():
                os.mkfifo(self.file_path.as_posix(), mode=0o600)
            self._write_fp = open(self.file_path, 'wb')

        if isinstance(data, str):
            data = data.encode('utf-8')
        try:
            self._write_fp.write(data)
            self._write_fp.flush()
        except BrokenPipeError:
            pass

    def fileno(self):
        fileno = self.file_handler.fileno()
        return fileno

    def read(self, size=-1):
        """
        Read from the FIFO pipe, blocking on EOFError.
        """
        _ = self.file_handler
        while True:
            try:
                data = self._file_handler.read(size)
                if data:
                    try:
                        if self.blocking_file_path.exists():
                            self.blocking_file_path.unlink()
                    except Exception:
                        warn(traceback.format_exc())
                    return data.decode('utf-8') if self.decode else data
            except (OSError, EOFError):
                pass

            if not self.blocking_file_path.exists():
                self.blocking_file_path.touch()
            time.sleep(self.refresh_seconds)

    def readline(self, size=-1):
        line = '' if self.decode else b''
        while True:
            data = self.read(1)
            if not data or ((data == '\n') if self.decode else (data == b'\n')):
                break
            line += data

        return line

    def close(self):
        if self._file_handler is not None:
            self.sel.unregister(self._file_handler)
            self._file_handler.close()
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._file_handler = None
            self._fd = None

        if self._write_fp is not None:
            try:
                self._write_fp.close()
            except BrokenPipeError:
                pass
            self._write_fp = None

        try:
            if self.blocking_file_path.exists():
                self.blocking_file_path.unlink()
        except Exception:
            pass
        super().close()

    def is_open(self):
        return self._file_handler is not None

    def isatty(self) -> bool:
        return True

    def __str__(self) -> str:
        return f"StdinFile('{self.file_path}')"

    def __repr__(self) -> str:
        return str(self)
