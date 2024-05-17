#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Intercept OS-level file descriptors.
"""

import os
import traceback
from datetime import datetime
from meerschaum.utils.typing import Callable
from meerschaum.utils.warnings import warn
from meerschaum.utils.threading import Thread

class FileDescriptorInterceptor:
    """
    A management class to intercept data written to a file descriptor.
    """
    def __init__(
        self,
        file_descriptor: int,
        injection_hook: Callable[[], str],
    ):
        """
        Parameters
        ----------
        file_descriptor: int
            The OS file descriptor from which to read.

        injection_hook: Callable[[], str]
            A callable which returns a string to be injected into the written data.
        """
        self.injection_hook = injection_hook
        self.original_file_descriptor = file_descriptor
        self.new_file_descriptor = os.dup(file_descriptor)
        self.read_pipe, self.write_pipe = os.pipe()
        os.dup2(self.write_pipe, file_descriptor)

    def start_interception(self):
        """
        Read from the file descriptor and write the modified data after injection.

        NOTE: This is blocking and is meant to be run in a thread.
        """
        while True:
            data = os.read(self.read_pipe, 1024)
            if not data:
                break
            injected_str = self.injection_hook()
            injected_bytes = injected_str.encode('utf-8')
            modified_data = data.replace(b'\n', b'\n' + injected_bytes)
            os.write(self.new_file_descriptor, modified_data)

    def stop_interception(self):
        """
        Restore the file descriptors and close the new pipes.
        """
        try:
            os.dup2(self.new_file_descriptor, self.original_file_descriptor)
        except OSError as e:
            warn(
                f"Error while trying to restore the intercepted file descriptor:\n"
                + f"{traceback.format_exc()}"
            )
        try:
            os.close(self.write_pipe)
        except OSError as e:
            warn(
                f"Error while trying to close the write-pipe to the intercepted file descriptor:\n"
                + f"{traceback.format_exc()}"
            )
        try:
            os.close(self.read_pipe)
        except OSError as e:
            warn(
                f"Error while trying to close the read-pipe to the intercepted file descriptor:\n"
                + f"{traceback.format_exc()}"
            )
