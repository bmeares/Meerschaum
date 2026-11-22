#!/usr/bin/env python3
"""Run a Linux CI test command and reap any descendants it leaves behind."""

import ctypes
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def run(command: list[str]) -> int:
    # A detached descendant becomes our child when its parent exits, even if it changed its
    # session or cleared its environment. No process outside this tree can be adopted here.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))
    children = Path('/proc/thread-self/children')
    cancelled = None

    def cancel(signum, _frame):
        nonlocal cancelled
        if cancelled is None:
            cancelled = signum

    signal.signal(signal.SIGINT, cancel)
    signal.signal(signal.SIGTERM, cancel)
    child = subprocess.Popen(command, start_new_session=True)
    cleanup_started = None
    leaked = False
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            if cancelled is not None:
                return 128 + cancelled
            return child.returncode or int(leaked)
        if pid == child.pid:
            child.returncode = os.waitstatus_to_exitcode(status)
        if pid and cancelled is None and cleanup_started is None:
            continue  # Drain already-exited children before deciding a successful command leaked.

        if cancelled is not None or child.returncode is not None:
            if cleanup_started is None:
                cleanup_started = time.monotonic()
                if cancelled is None:
                    leaked = True
                    print('The test command left running child processes. Cleaning them up.', file=sys.stderr)
                elif child.returncode is None:
                    # Give the command and its foreground children the runner's signal.
                    try:
                        os.killpg(child.pid, cancelled)
                    except ProcessLookupError:
                        pass
            elapsed = time.monotonic() - cleanup_started
            if elapsed > 5:
                print('Test process cleanup timed out.', file=sys.stderr)
                return 1
            stop = signal.SIGKILL if elapsed >= 1 else signal.SIGTERM
            for pid_text in children.read_text().split():
                owned_pid = int(pid_text)
                if child.returncode is None and owned_pid == child.pid and stop == signal.SIGTERM:
                    continue  # The command's own group already received the cancellation signal.
                # A direct child's PID cannot be reused until this process reaps it.
                try:
                    os.kill(owned_pid, stop)
                except ProcessLookupError:
                    pass
        if not pid:
            time.sleep(0.05)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit(f'Usage: {sys.argv[0]} COMMAND [ARGS...]')
    code = run(sys.argv[1:])
    sys.exit(code if code >= 0 else 128 - code)
