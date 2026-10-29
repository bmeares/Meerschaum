#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A file-like object which writes to rotating subfiles (native: `meerschaum-jobs`, `rotating.rs`).
"""

from __future__ import annotations

import io
import pathlib

from meerschaum.utils.typing import Optional, List


class RotatingFile(io.IOBase):
    """
    A `RotatingFile` may be treated like a normal file-like object.
    Under the hood, however, it will create new sub-files and delete old ones.

    The stream redirection of v4 (`redirect_streams`, `start_log_fd_interception`) is not
    available: the job supervisor copies a job's output into its log.
    """

    SEEK_BACK_ATTEMPTS: int = 5

    def __init__(
        self,
        file_path: pathlib.Path,
        num_files_to_keep: Optional[int] = None,
        max_file_size: Optional[int] = None,
        redirect_streams: bool = False,
        write_timestamps: bool = False,
        timestamp_format: Optional[str] = None,
        write_callback=None,
    ):
        """
        Create a file-like object which manages other files.

        Parameters
        ----------
        file_path: pathlib.Path
            The base path (`logs/<name>.log`); subfiles are `<name>.log.<n>`.

        num_files_to_keep: int, default None
            How many sub-files to keep at any given time.
            Defaults to the configured value (5).

        max_file_size: int, default None
            How large in bytes each sub-file can grow before another file is created.
            Defaults to the configured value (100_000).

        write_timestamps: bool, default False
            If `True`, prepend the current UTC timestamp to each line of the file.

        timestamp_format: str, default None
            If `write_timestamps` is `True`, use this format for the timestamps.
            Defaults to `'%Y-%m-%d %H:%M'`.
        """
        from meerschaum.config import get_config
        from meerschaum.jobs import _native
        if redirect_streams or write_callback is not None:
            raise NotImplementedError(
                "`redirect_streams` and `write_callback` are not supported in Meerschaum v5 "
                "(see the migration guide, 'Removed in 5.0')."
            )
        self.file_path = pathlib.Path(file_path)
        if num_files_to_keep is None:
            num_files_to_keep = get_config('jobs', 'logs', 'num_files_to_keep')
        if max_file_size is None:
            max_file_size = get_config('jobs', 'logs', 'max_file_size')
        if timestamp_format is None:
            timestamp_format = get_config('jobs', 'logs', 'timestamps', 'format')
        if num_files_to_keep < 1:
            raise ValueError("At least 1 file must be kept.")
        if max_file_size < 100:
            raise ValueError("Subfiles must contain at least 100 bytes.")
        self.num_files_to_keep = num_files_to_keep
        self.max_file_size = max_file_size
        self.write_timestamps = write_timestamps
        self.timestamp_format = timestamp_format
        self._core = _native.core().RotatingFile(
            str(self.file_path),
            num_files_to_keep,
            max_file_size,
            write_timestamps,
            timestamp_format,
        )
        self._closed = False
        self._lines_buffer: List[str] = []

    @property
    def closed(self) -> bool:
        return self._closed

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return False

    def fileno(self) -> int:
        raise io.UnsupportedOperation("A RotatingFile has no file descriptor.")

    def read(self, *args, **kw) -> str:
        """Return the text of every subfile."""
        return self._core.read()

    def readlines(self, *args, **kw) -> List[str]:
        """Return the lines since the cursor."""
        return self._core.readlines()

    def readline(self, *args, **kw) -> str:
        """Return the next line since the cursor ('' at the end)."""
        if not self._lines_buffer:
            self._lines_buffer = self._core.readlines()
        return self._lines_buffer.pop(0) if self._lines_buffer else ''

    def write(self, data: str) -> int:
        """Write text, rotating subfiles as they fill."""
        return self._core.write(data)

    def writelines(self, lines) -> None:
        for line in lines:
            self.write(line)

    def seek(self, position: int, *args, **kw) -> int:
        """Move the read cursor in the latest subfile."""
        self._lines_buffer = []
        self._core.seek(position)
        return position

    def touch(self) -> None:
        """Create the latest subfile."""
        self._core.touch()

    def refresh_cursor(self) -> None:
        self._core.refresh_cursor()

    def flush(self) -> None:
        pass

    def close(self) -> None:
        """Close the open subfile handles."""
        self._core.close()
        self._closed = True

    def delete(self, unused_only: bool = False) -> None:
        """Delete the subfiles (`unused_only`: only those beyond the files to keep)."""
        self._core.delete(unused_only)

    def get_latest_subfile_index(self) -> int:
        return self._core.get_latest_subfile_index()

    def get_latest_subfile_path(self) -> pathlib.Path:
        return pathlib.Path(self._core.get_latest_subfile_path())

    def get_existing_subfile_indices(self) -> List[int]:
        return self._core.get_existing_subfile_indices()

    def get_existing_subfile_paths(self) -> List[pathlib.Path]:
        return [pathlib.Path(p) for p in self._core.get_existing_subfile_paths()]

    def get_subfile_path_from_index(self, index: int) -> pathlib.Path:
        return pathlib.Path(self._core.get_subfile_path_from_index(index))

    def get_subfile_name_from_index(self, index: int) -> str:
        return self._core.get_subfile_name_from_index(index)

    def get_index_from_subfile_name(self, subfile_name: str) -> int:
        return self._core.get_index_from_subfile_name(subfile_name)

    def get_timestamp_prefix_str(self) -> str:
        return self._core.get_timestamp_prefix_str()
