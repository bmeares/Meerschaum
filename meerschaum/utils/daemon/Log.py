#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A logging class to keep track of the offset when printing unread lines.
"""

from __future__ import annotations
import os
import pathlib
import sys
from meerschaum.utils.typing import Optional, Union, List


class Log:
    """
    Manage the offset of a rolling logfile.
    """

    def __init__(
        self,
        file_path: pathlib.Path,
        offset_file_path: Optional[pathlib.Path] = None,
    ):
        self.file_path = file_path
        self._offset_file_path = offset_file_path or self.file_path / '.offset'
        self._offset_inode = 0
        self._offset = 0
        self._since_update = 0
        self._handle = None

        if self._offset_file_path.exists() and os.path.getsize(self._offset_file_path):
            with open(self._offset_file_path, 'r', encoding='utf-8') as f:
                self._offset = int(f.readline().strip())


    def next(self):
        """Update the offset and return the next line."""
        try:
            line = self._get_next_line()
        except StopIteration:
            if self._is_new_file():
                self._handle.close()
                self._offset = 0
                try:
                    line = self._get_next_line()
                except StopIteration:
                    self._update_offset_file_path()
                    raise
            else:
                self._update_offset_file_path()
                raise

        return line


    def __next__(self):
        return self.next()


    def read(self) -> Union[str, None]:
        """Read and return the unread lines as a string."""
        lines = self.readlines()
        return ''.join(lines)


    def readlines(self) -> List[str]:
        """Read and return the unread lines as a list of strings."""
        return [line for line in self]


    def _file_handle(self):
        """Set the cursor the the offset and return a handle to the file."""
        import gzip
        if not self._handle or self._handle.closed:
            self._handle = (
                open(self.file_path, 'r', 1, encoding='utf-8')
                if not str(self.file_path).endswith('.gz')
                else gzip.open(self.file_path, 'r')
            )
            self._handle.seek(self._offset)

        return self._handle


    def _update_offset_file_path(self):
        offset = self._file_handle().tell()
        with open(self._offset_file_path, 'w', encoding='utf-8') as f:
            f.write(f"{offset}\n")
        self._since_update = 0


    def _is_new_file(self):
        return (
            self._file_handle().tell() == os.fstat(self._file_handle().fileno()).st_size
            and os.fstat(self._file_handle().fileno()).st_ino != os.stat(self.file_path).st_ino
        )


    def _get_next_line(self):
        curr_offset = self._file_handle().tell()
        line = self._file_handle().readline()
        if not line:
            raise StopIteration
        self._since_update += 1
        return line


    def __del__(self):
        if self._file_handle():
            self._file_handle().close()


    def __iter__(self):
        return self
