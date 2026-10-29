#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The FIFO a job reads its stdin from (native: `meerschaum-jobs`, `stdin.rs`).
"""

from __future__ import annotations

import io
import pathlib

from meerschaum.utils.typing import Optional, Union


class StdinFile(io.IOBase):
    """
    The client end of a job's stdin FIFO: `write()` sends input to the job.
    """

    def __init__(
        self,
        file_path: Union[pathlib.Path, str],
        lock_file_path: Optional[pathlib.Path] = None,
        decode: bool = True,
        refresh_seconds: Union[int, float, None] = None,
    ):
        from meerschaum.jobs import _native
        self.file_path = pathlib.Path(file_path)
        self.blocking_file_path = (
            pathlib.Path(lock_file_path)
            if lock_file_path is not None
            else (self.file_path.parent / (self.file_path.name + '.block'))
        )
        self.decode = decode
        self._refresh_seconds = refresh_seconds
        self._core = _native.core().StdinFile(
            str(self.file_path),
            str(self.blocking_file_path),
        )

    @property
    def encoding(self) -> str:
        return 'utf-8'

    @property
    def is_open(self) -> bool:
        return self._core.is_open()

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def isatty(self) -> bool:
        return False

    def write(self, data: Union[str, bytes]) -> int:
        """Send input to the job."""
        text = data.decode('utf-8') if isinstance(data, bytes) else data
        return self._core.write(text)

    def writelines(self, lines) -> None:
        for line in lines:
            self.write(line)

    def readline(self, *args, **kw) -> str:
        """Read a line from the FIFO (job side)."""
        return self._core.readline()

    def close(self) -> None:
        self._core.close()
