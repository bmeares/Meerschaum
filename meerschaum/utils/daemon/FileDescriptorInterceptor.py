#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Intercept OS-level file descriptors.
"""

import os
import select
import traceback
import errno
from threading import Event
from datetime import datetime
from meerschaum.utils.typing import Callable
from meerschaum.utils.warnings import warn
from meerschaum.config.paths import DAEMON_ERROR_LOG_PATH

FD_CLOSED: int = 9
STOP_READING_FD_EVENT: Event = Event()

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
        self.stop_event = Event()
        self.injection_hook = injection_hook
        self.original_file_descriptor = file_descriptor
        self.new_file_descriptor = os.dup(file_descriptor)
        self.read_pipe, self.write_pipe = os.pipe()
        self.signal_read_pipe, self.signal_write_pipe = os.pipe()
        os.dup2(self.write_pipe, file_descriptor)

    def start_interception(self):
        """
        Read from the file descriptor and write the modified data after injection.

        NOTE: This is blocking and is meant to be run in a thread.
        """
        os.set_blocking(self.read_pipe, False)
        os.set_blocking(self.signal_read_pipe, False)
        is_first_read = True
        while not self.stop_event.is_set():
            try:
                rlist, _, _ = select.select([self.read_pipe, self.signal_read_pipe], [], [], 0.1)
                if self.signal_read_pipe in rlist:
                    break
                if not rlist:
                    continue
                data = os.read(self.read_pipe, 1024)
                if not data:
                    break
            except BlockingIOError:
                continue
            except OSError as e:
                if e.errno == errno.EBADF:
                    ### File descriptor is closed.
                    pass
                elif e.errno == errno.EINTR:
                    continue  # Interrupted system call, just try again
                else:
                    warn(f"OSError in FileDescriptorInterceptor: {e}")
                break

            try:
                first_char_is_newline = data[0] == b'\n'
                last_char_is_newline = data[-1] == b'\n'

                injected_str = self.injection_hook()
                injected_bytes = injected_str.encode('utf-8')

                if is_first_read:
                    data = b'\n' + data
                    is_first_read = False

                modified_data = (
                    (data[:-1].replace(b'\n', b'\n' + injected_bytes) + b'\n')
                    if last_char_is_newline
                    else data.replace(b'\n', b'\n' + injected_bytes)
                )
                os.write(self.new_file_descriptor, modified_data)
            except (BrokenPipeError, OSError):
                break
            except Exception:
                with open(DAEMON_ERROR_LOG_PATH, 'a+', encoding='utf-8') as f:
                    f.write(traceback.format_exc())
                break


    def stop_interception(self):
        """
        Close the new file descriptors.
        """
        self.stop_event.set()
        os.write(self.signal_write_pipe, b'\0')
        try:
            os.close(self.new_file_descriptor)
        except OSError as e:
            if e.errno != FD_CLOSED:
                warn(
                    "Error while trying to close the duplicated file descriptor:\n"
                    + f"{traceback.format_exc()}"
                )

        try:
            os.close(self.write_pipe)
        except OSError as e:
            if e.errno != FD_CLOSED:
                warn(
                    "Error while trying to close the write-pipe "
                    + "to the intercepted file descriptor:\n"
                    + f"{traceback.format_exc()}"
                )
        try:
            os.close(self.read_pipe)
        except OSError as e:
            if e.errno != FD_CLOSED:
                warn(
                    "Error while trying to close the read-pipe "
                    + "to the intercepted file descriptor:\n"
                    + f"{traceback.format_exc()}"
                )

        try:
            os.close(self.signal_read_pipe)
        except OSError as e:
            if e.errno != FD_CLOSED:
                warn(
                    "Error while trying to close the signal-read-pipe "
                    + "to the intercepted file descriptor:\n"
                    + f"{traceback.format_exc()}"
                )

        try:
            os.close(self.signal_write_pipe)
        except OSError as e:
            if e.errno != FD_CLOSED:
                warn(
                    "Error while trying to close the signal-write-pipe "
                    + "to the intercepted file descriptor:\n"
                    + f"{traceback.format_exc()}"
                )
