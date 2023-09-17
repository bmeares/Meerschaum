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
            Defaults to the configured value (100_000).

        redirect_streams: bool, default False
            If `True`, redirect previous file streams when opening a new file descriptor.
        """
        self.file_path = pathlib.Path(file_path)
        if num_files_to_keep is None:
            num_files_to_keep = get_config('system', 'daemons', 'num_files_to_keep')
        if max_file_size is None:
            max_file_size = get_config('system', 'daemons', 'max_file_size')
        if num_files_to_keep < 1:
            raise ValueError("At least 1 file must be kept.")
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
        """
        return int(subfile_name.replace(self.file_path.name + '.', ''))


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


    def get_existing_subfile_paths(self) -> List[pathlib.Path]:
        """
        Return a list of file paths that match the input filename pattern.
        """
        if not self.file_path.parent.exists():
            return []

        subfile_names = sorted([
            file_name
            for file_name in os.listdir(self.file_path.parent)
            if re.match(self.subfile_regex_pattern, file_name)
        ])
        return [
            (self.file_path.parent / file_name)
            for file_name in subfile_names
        ]


    def refresh_files(self, potential_new_len: int = 0) -> '_io.TextUIWrapper':
        """
        Check the state of the subfiles.
        If the latest subfile is too large, create a new file and delete old ones.

        Parameters
        ----------
        potential_new_len: int, default 0
        """
        latest_subfile_index = self.get_latest_subfile_index()
        create_new_file = (
            self._current_file_obj is None
            or
            self.is_subfile_too_large(latest_subfile_index, potential_new_len)
            or
            self._current_file_obj.closed
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
        """
        self.file_path.parent.mkdir(exist_ok=True, parents=True)
        if isinstance(data, bytes):
            data = data.decode('utf-8')

        self.refresh_files(potential_new_len=len(data))
        try:
            self._current_file_obj.write(data)
        except Exception as e:
            warn(f"Failed to write to subfile:\n{traceback.format_exc()}")
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
        existing_subfile_paths = self.get_existing_subfile_paths()
        buffer = ''
        for subfile_path in existing_subfile_paths:
            subfile_index = self.get_index_from_subfile_name(subfile_path.name)
            if (
                subfile_index in self.subfile_objects
                and
                subfile_index not in self._redirected_subfile_objects
            ):
                subfile_object = self.subfile_objects[subfile_index]
                subfile_object.seek(0)
                buffer += subfile_object.read()
            else:
                with open(subfile_path, 'r', encoding='utf-8') as f:
                    buffer += f.read()

        if self._current_file_obj is not None:
            current_ix = self.get_index_from_subfile_name(self._current_file_obj.name)
            self._file_cursors[current_ix] = self._current_file_obj.tell()
        return buffer


    def readlines(self) -> List[str]:
        """
        Return a list of lines of text.
        """
        existing_subfile_paths = self.get_existing_subfile_paths()
        lines = []
        for subfile_path in existing_subfile_paths:
            subfile_index = self.get_index_from_subfile_name(subfile_path.name)
            if (
                subfile_index in self.subfile_objects
                and
                subfile_index not in self._redirected_subfile_objects
            ):
                subfile_object = self.subfile_objects[subfile_index]
                subfile_object.seek(0)
                lines.extend(subfile_object.readlines())
            else:
                with open(subfile_path, 'r', encoding='utf-8') as f:
                    lines.extend(f.readlines())
        return lines


    def seekable(self) -> bool:
        """
        This file-like class is not randomly accessible. 
        """
        return False

    
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
