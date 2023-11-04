#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create a file-like object that manages sub-files under the hood.
"""

import os
import io
import re
import pathlib
import traceback
import sys
import atexit
from typing import List, Union, Optional, Tuple
from meerschaum.config import get_config
from meerschaum.utils.warnings import warn
import meerschaum as mrsm
daemon = mrsm.attempt_import('daemon')

class RotatingFile(io.IOBase):
    """
    A `RotatingFile` may be treated like a normal file-like object.
    Under the hood, however, it will create new sub-files and delete old ones.
    """

    SEEK_BACK_ATTEMPTS: int = 5

    def __init__(
            self,
            file_path: pathlib.Path,
            num_files_to_keep: Optional[int] = None,
            max_file_size: Optional[int] = None,
            redirect_streams: bool = False,
        ):
        """
        Create a file-like object which manages other files.

        Parameters
        ----------
        num_files_to_keep: int, default None
            How many sub-files to keep at any given time.
            Defaults to the configured value (5).

        max_file_size: int, default None
            How large in bytes each sub-file can grow before another file is created.
            Note that this is not a hard limit but rather a threshold
            which may be slightly exceeded.
            Defaults to the configured value (100_000).

        redirect_streams: bool, default False
            If `True`, redirect previous file streams when opening a new file descriptor.
            
            NOTE: Only set this to `True` if you are entering into a daemon context.
            Doing so will redirect `sys.stdout` and `sys.stderr` into the log files.
        """
        self.file_path = pathlib.Path(file_path)
        if num_files_to_keep is None:
            num_files_to_keep = get_config('jobs', 'logs', 'num_files_to_keep')
        if max_file_size is None:
            max_file_size = get_config('jobs', 'logs', 'max_file_size')
        if num_files_to_keep < 2:
            raise ValueError("At least 2 files must be kept.")
        if max_file_size < 1:
            raise ValueError("Subfiles must contain at least one byte.")

        self.num_files_to_keep = num_files_to_keep
        self.max_file_size = max_file_size
        self.redirect_streams = redirect_streams
        self.subfile_regex_pattern = re.compile(
            r'^'
            + self.file_path.name
            + r'(?:\.\d+)?$'
        )

        ### When subfiles are opened, map from their index to the file objects.
        self.subfile_objects = {}
        self._redirected_subfile_objects = {}
        self._current_file_obj = None
        self._previous_file_obj = None

        ### When reading, keep track of the file index and position.
        self._cursor: Tuple[int, int] = (0, 0)

        ### Don't forget to close any stray files.
        atexit.register(self.close)


    def fileno(self):
        """
        Return the file descriptor for the latest subfile.
        """
        self.refresh_files()
        return self._current_file_obj.fileno()


    def get_latest_subfile_path(self) -> pathlib.Path:
        """
        Return the path for the latest subfile to which to write into.
        """
        return self.get_subfile_path_from_index(
            self.get_latest_subfile_index()
        )


    def get_remaining_subfile_size(self, subfile_index: int) -> int:
        """
        Return the remaining buffer size for a subfile.

        Parameters
        ---------
        subfile_index: int
            The index of the subfile to be checked.

        Returns
        -------
        The remaining size in bytes.
        """
        subfile_path = self.get_subfile_path_from_index(subfile_index)
        if not subfile_path.exists():
            return self.max_file_size

        return self.max_file_size - os.path.getsize(subfile_path)


    def is_subfile_too_large(self, subfile_index: int, potential_new_len: int = 0) -> bool:
        """
        Return whether a given subfile is too large.

        Parameters
        ----------
        subfile_index: int
            The index of the subfile to be checked.

        potential_new_len: int, default 0
            The length of a potential write of new data.

        Returns
        -------
        A bool indicating the subfile is or will be too large.
        """
        subfile_path = self.get_subfile_path_from_index(subfile_index)
        if not subfile_path.exists():
            return False

        self.flush()

        return (
            (os.path.getsize(subfile_path) + potential_new_len)
            >=
            self.max_file_size
        )


    def get_latest_subfile_index(self) -> int:
        """
        Return the latest existing subfile index.
        If no index may be found, return -1.
        """
        existing_subfile_paths = self.get_existing_subfile_paths()
        latest_index = (
            self.get_index_from_subfile_name(existing_subfile_paths[-1].name)
            if existing_subfile_paths
            else -1
        )
        return latest_index


    def get_index_from_subfile_name(self, subfile_name: str) -> int:
        """
        Return the index from a given subfile name.
        If the file name cannot be parsed, return -1.
        """
        try:
            return int(subfile_name.replace(self.file_path.name + '.', ''))
        except Exception as e:
            return -1


    def get_subfile_name_from_index(self, subfile_index: int) -> str:
        """
        Return the subfile name from the given index.
        """
        return f'{self.file_path.name}.{subfile_index}'


    def get_subfile_path_from_index(self, subfile_index: int) -> pathlib.Path:
        """
        Return the subfile's path from its index.
        """
        return self.file_path.parent / self.get_subfile_name_from_index(subfile_index)


    def get_existing_subfile_indices(self) -> List[int]:
        """
        Return of list of subfile indices which exist on disk.
        """
        existing_subfile_paths = self.get_existing_subfile_paths()
        return [self.get_index_from_subfile_name(path.name) for path in existing_subfile_paths]


    def get_existing_subfile_paths(self) -> List[pathlib.Path]:
        """
        Return a list of file paths that match the input filename pattern.
        """
        if not self.file_path.parent.exists():
            return []

        subfile_names_indices = sorted(
            [
                (file_name, self.get_index_from_subfile_name(file_name))
                for file_name in os.listdir(self.file_path.parent)
                if re.match(self.subfile_regex_pattern, file_name)
            ],
            key = lambda x: x[1],
        )
        return [
            (self.file_path.parent / file_name)
            for file_name, _ in subfile_names_indices
        ]


    def refresh_files(self, potential_new_len: int = 0) -> '_io.TextUIWrapper':
        """
        Check the state of the subfiles.
        If the latest subfile is too large, create a new file and delete old ones.

        Parameters
        ----------
        potential_new_len: int, default 0
        """
        self.flush()

        latest_subfile_index = self.get_latest_subfile_index()
        latest_subfile_path = self.get_subfile_path_from_index(latest_subfile_index)

        ### First run with existing log files: open the most recent log file.
        is_first_run_with_logs = ((latest_subfile_index > -1) and self._current_file_obj is None)

        ### Sometimes a new file is created but output doesn't switch over.
        lost_latest_handle = (
            self._current_file_obj is not None
            and
            self.get_index_from_subfile_name(self._current_file_obj.name) == -1
        )
        if is_first_run_with_logs or lost_latest_handle:
            self._current_file_obj = open(latest_subfile_path, 'a+', encoding='utf-8')
            if self.redirect_streams:
                daemon.daemon.redirect_stream(sys.stdout, self._current_file_obj)
                daemon.daemon.redirect_stream(sys.stderr, self._current_file_obj)

        create_new_file = (
            (latest_subfile_index == -1)
            or
            self._current_file_obj is None
            or
            self.is_subfile_too_large(latest_subfile_index, potential_new_len)
        )
        if create_new_file:
            old_subfile_index = latest_subfile_index
            new_subfile_index = old_subfile_index + 1
            new_file_path = self.get_subfile_path_from_index(new_subfile_index)
            self._previous_file_obj = self._current_file_obj
            self._current_file_obj = open(new_file_path, 'a+', encoding='utf-8')
            self.subfile_objects[new_subfile_index] = self._current_file_obj
            self.flush()

            if self._previous_file_obj is not None:
                if self.redirect_streams:
                    self._redirected_subfile_objects[old_subfile_index] = self._previous_file_obj
                    daemon.daemon.redirect_stream(self._previous_file_obj, self._current_file_obj)
                    daemon.daemon.redirect_stream(sys.stdout, self._current_file_obj)
                    daemon.daemon.redirect_stream(sys.stderr, self._current_file_obj)
                self.close(unused_only=True)

            ### Sanity check in case writing somehow fails.
            if self._previous_file_obj is self._current_file_obj:
                self._previous_file_obj is None

            self.delete(unused_only=True)
        return self._current_file_obj


    def close(self, unused_only: bool = False) -> None:
        """
        Close any open file descriptors.

        Parameters
        ----------
        unused_only: bool, default False
            If `True`, only close file descriptors not currently in use.
        """
        subfile_indices = sorted(self.subfile_objects.keys())
        for subfile_index in subfile_indices:
            subfile_object = self.subfile_objects[subfile_index]
            if unused_only and subfile_object in (self._previous_file_obj, self._current_file_obj):
                continue
            try:
                if not subfile_object.closed:
                    #  subfile_object.flush()
                    subfile_object.close()
                _ = self.subfile_objects.pop(subfile_index, None)
                if self.redirect_streams:
                    _ = self._redirected_subfile_objects.pop(subfile_index, None)
            except Exception as e:
                warn(f"Failed to close an open subfile:\n{traceback.format_exc()}")

        if not unused_only:
            self._previous_file_obj = None
            self._current_file_obj = None


    def write(self, data: str) -> None:
        """
        Write the given text into the latest subfile.
        If the subfile will be too large, create a new subfile.
        If too many subfiles exist at once, the oldest one will be deleted.

        NOTE: This will not split data across multiple files.
        As such, if data is larger than max_file_size, then the corresponding subfile
        may exceed this limit.
        """
        self.file_path.parent.mkdir(exist_ok=True, parents=True)
        if isinstance(data, bytes):
            data = data.decode('utf-8')

        self.refresh_files(potential_new_len=len(data))
        try:
            self._current_file_obj.write(data)
        except Exception as e:
            warn(f"Failed to write to subfile:\n{traceback.format_exc()}")
        self.flush()
        self.delete(unused_only=True)


    def delete(self, unused_only: bool = False) -> None:
        """
        Delete old subfiles.

        Parameters
        ----------
        unused_only: bool, default False
            If `True`, only delete subfiles which are no longer needed.
        """
        existing_subfile_paths = self.get_existing_subfile_paths()
        if unused_only and len(existing_subfile_paths) <= self.num_files_to_keep:
            return

        self.flush()
        self.close(unused_only=unused_only)

        end_ix = (
            (-1 * self.num_files_to_keep)
            if unused_only
            else len(existing_subfile_paths)
        )
        for subfile_path_to_delete in existing_subfile_paths[0:end_ix]:
            subfile_index = self.get_index_from_subfile_name(subfile_path_to_delete.name)
            subfile_object = self.subfile_objects.get(subfile_index, None)

            try:
                subfile_path_to_delete.unlink()
            except Exception as e:
                warn(
                    f"Unable to delete subfile '{subfile_path_to_delete}':\n"
                    + f"{traceback.format_exc()}"
                )


    def read(self, *args, **kwargs) -> str:
        """
        Read the contents of the existing subfiles.
        """
        existing_subfile_indices = [
            self.get_index_from_subfile_name(subfile_path.name)
            for subfile_path in self.get_existing_subfile_paths()
        ]
        paths_to_read = [
            self.get_subfile_path_from_index(subfile_index)
            for subfile_index in existing_subfile_indices
            if subfile_index >= self._cursor[0]
        ]
        buffer = ''
        refresh_cursor = True
        for subfile_path in paths_to_read:
            subfile_index = self.get_index_from_subfile_name(subfile_path.name)
            seek_ix = (
                self._cursor[1]
                if subfile_index == self._cursor[0]
                else 0
            )

            if (
                subfile_index in self.subfile_objects
                and
                subfile_index not in self._redirected_subfile_objects
            ):
                subfile_object = self.subfile_objects[subfile_index]
                for i in range(self.SEEK_BACK_ATTEMPTS):
                    try:
                        subfile_object.seek(max(seek_ix - i, 0))
                        buffer += subfile_object.read()
                    except UnicodeDecodeError:
                        continue
                    break
            else:
                with open(subfile_path, 'r', encoding='utf-8') as f:
                    for i in range(self.SEEK_BACK_ATTEMPTS):
                        try:
                            f.seek(max(seek_ix - i, 0))
                            buffer += f.read()
                        except UnicodeDecodeError:
                            continue
                        break

                    ### Handle the case when no files have yet been opened.
                    if not self.subfile_objects and subfile_path == paths_to_read[-1]:
                        self._cursor = (subfile_index, f.tell())
                        refresh_cursor = False

        if refresh_cursor:
            self.refresh_cursor()
        return buffer


    def refresh_cursor(self) -> None:
        """
        Update the cursor to the latest subfile index and file.tell() value.
        """
        self.flush()
        existing_subfile_paths = self.get_existing_subfile_paths()
        current_ix = (
            self.get_index_from_subfile_name(existing_subfile_paths[-1].name)
            if existing_subfile_paths
            else 0
        )
        position = self._current_file_obj.tell() if self._current_file_obj is not None else 0
        self._cursor = (current_ix, position)


    def readlines(self) -> List[str]:
        """
        Return a list of lines of text.
        """
        existing_subfile_indices = [
            self.get_index_from_subfile_name(subfile_path.name)
            for subfile_path in self.get_existing_subfile_paths()
        ]
        paths_to_read = [
            self.get_subfile_path_from_index(subfile_index)
            for subfile_index in existing_subfile_indices
            if subfile_index >= self._cursor[0]
        ]

        lines = []
        refresh_cursor = True
        for subfile_path in paths_to_read:
            subfile_index = self.get_index_from_subfile_name(subfile_path.name)
            seek_ix = (
                self._cursor[1]
                if subfile_index == self._cursor[0]
                else 0
            )

            if (
                subfile_index in self.subfile_objects
                and
                subfile_index not in self._redirected_subfile_objects
            ):
                subfile_object = self.subfile_objects[subfile_index]
                for i in range(self.SEEK_BACK_ATTEMPTS):
                    try:
                        subfile_object.seek(max(seek_ix - i), 0)
                        subfile_lines = subfile_object.readlines()
                    except UnicodeDecodeError:
                        continue
                    break
            else:
                with open(subfile_path, 'r', encoding='utf-8') as f:
                    for i in range(self.SEEK_BACK_ATTEMPTS):
                        try:
                            f.seek(max(seek_ix - i, 0))
                            subfile_lines = f.readlines()
                        except UnicodeDecodeError:
                            continue
                        break

                    ### Handle the case when no files have yet been opened.
                    if not self.subfile_objects and subfile_path == paths_to_read[-1]:
                        self._cursor = (subfile_index, f.tell())
                        refresh_cursor = False

            ### Sometimes a line may span multiple files.
            if lines and subfile_lines and not lines[-1].endswith('\n'):
                lines[-1] += subfile_lines[0]
                new_lines = subfile_lines[1:]
            else:
                new_lines = subfile_lines
            lines.extend(new_lines)

        if refresh_cursor:
            self.refresh_cursor()
        return lines


    def seekable(self) -> bool:
        return True


    def seek(self, position: int) -> None:
        """
        Seek to the beginning of the logs stream.
        """
        existing_subfile_indices = self.get_existing_subfile_indices()
        min_ix = existing_subfile_indices[0] if existing_subfile_indices else 0
        max_ix = existing_subfile_indices[-1] if existing_subfile_indices else 0
        if position == 0:
            self._cursor = (min_ix, 0)
            return

        self._cursor = (max_ix, position)
        self._current_file_obj.seek(position)

    
    def flush(self) -> None:
        """
        Flush any open subfiles.
        """
        for subfile_index, subfile_object in self.subfile_objects.items():
            if not subfile_object.closed:
                try:
                    subfile_object.flush()
                except Exception as e:
                    warn(f"Failed to flush subfile:\n{traceback.format_exc()}")
        if self.redirect_streams:
            sys.stdout.flush()
            sys.stderr.flush()


    def __repr__(self) -> str:
        """
        Return basic info for this `RotatingFile`.
        """
        return (
            "RotatingFile("
            + f"'{self.file_path.as_posix()}', "
            + f"num_files_to_keep={self.num_files_to_keep}, "
            + f"max_file_size={self.max_file_size})"
        )
