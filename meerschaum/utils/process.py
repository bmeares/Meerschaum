#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Custom process-handling functions.
See `meerschaum.utils.pool` for multiprocessing and
`meerschaum.utils.threading` for threads.
"""

from __future__ import annotations
import os, signal, subprocess, sys, platform
from meerschaum.utils.typing import Union, Optional, Any, Callable, Dict, Tuple

def run_process(
        *args,
        foreground: bool = False,
        as_proc: bool = False,
        line_callback: Optional[Callable[[bytes], Any]] = None,
        store_proc_dict: Optional[Dict[str, Any]] = None,
        store_proc_key: str = 'child_process',
        capture_output: bool = False,
        **kw: Any
    ) -> Union[int, subprocess.Popen]:
    """Original foreground solution found here:
    https://stackoverflow.com/questions/23826695/handling-keyboard-interrupt-when-using-subproccess

    Parameters
    ----------
    *args:
        The sysargs to execute.

    foreground: bool, default False
        If `True`, execute the process as a foreground process that passes Ctrl-C to children.
        From the original post:
        The "correct" way of spawning a new subprocess:
        signals like C-c must only go
        to the child process, and not to this python.
        
        Some side-info about "how ctrl-c works":
        https://unix.stackexchange.com/a/149756/1321

    as_proc: bool, default False
        If `True`, return the `subprocess.Popen` object.

    line_callback: Optional[Callable[[str], Any]], default None
        If provided, poll the process and execute the callback when `readline()` gets new text.

    store_proc_dict: Optional[Dict[str, Any]], default None
        If provided, store the `subprocess.Popen` object under the key `store_proc_key`.
        Useful for accessing the process while it is polling in another thread.

    store_proc_key: str, default 'child_process'
        If `store_proc_dict` is provided, store the process in the dictionary under this key.

    kw: Any
        Additional keyword arguments to pass to `subprocess.Popen`.

    Returns
    -------
    Either an int for the return code or a `subprocess.Popen` object.
    """
    try:
        import termios
    except ImportError:
        termios = None

    if platform.system() == 'Windows':
        foreground = False

    if line_callback is not None:
        kw['stdout'] = subprocess.PIPE
        kw['stderr'] = subprocess.STDOUT

    if 'env' not in kw:
        kw['env'] = os.environ

    user_preexec_fn = kw.get("preexec_fn", None)

    if foreground:
        try:
            old_pgrp = os.tcgetpgrp(sys.stdin.fileno())
        except Exception as e:
            termios = None
        if termios:
            try:
                old_attr = termios.tcgetattr(sys.stdin.fileno())
            except Exception as e:
                termios = None

    def new_pgid():
        if user_preexec_fn:
            user_preexec_fn()

        # set a new process group id
        os.setpgid(os.getpid(), os.getpid())

        # generally, the child process should stop itself
        # before exec so the parent can set its new pgid.
        # (setting pgid has to be done before the child execs).
        # however, Python 'guarantee' that `preexec_fn`
        # is run before `Popen` returns.
        # this is because `Popen` waits for the closure of
        # the error relay pipe '`errpipe_write`',
        # which happens at child's exec.
        # this is also the reason the child can't stop itself
        # in Python's `Popen`, since the `Popen` call would never
        # terminate then.
        # `os.kill(os.getpid(), signal.SIGSTOP)`

    if foreground:
        kw['preexec_fn'] = new_pgid

    try:
        # fork the child
        #  stdout, stderr = (
            #  (sys.stdout, sys.stderr) if not capture_output
            #  else (subprocess.PIPE, subprocess.PIPE)
        #  )
        if capture_output:
            kw['stdout'] = subprocess.PIPE
            kw['stderr'] = subprocess.PIPE

        child = subprocess.Popen(*args, **kw)

        # we can't set the process group id from the parent since the child
        # will already have exec'd. and we can't SIGSTOP it before exec,
        # see above.
        # `os.setpgid(child.pid, child.pid)`

        if foreground:
            # set the child's process group as new foreground
            try:
                os.tcsetpgrp(sys.stdin.fileno(), child.pid)
            except Exception as e:
                pass
            # revive the child,
            # because it may have been stopped due to SIGTTOU or
            # SIGTTIN when it tried using stdout/stdin
            # after setpgid was called, and before we made it
            # forward process by tcsetpgrp.
            os.kill(child.pid, signal.SIGCONT)

        # wait for the child to terminate
        if store_proc_dict is not None:
            store_proc_dict[store_proc_key] = child
        _ret = poll_process(child, line_callback) if line_callback is not None else child.wait()
        ret = _ret if not as_proc else child

    finally:
        if foreground:
            # we have to mask SIGTTOU because tcsetpgrp
            # raises SIGTTOU to all current background
            # process group members (i.e. us) when switching tty's pgrp
            # it we didn't do that, we'd get SIGSTOP'd
            hdlr = signal.signal(signal.SIGTTOU, signal.SIG_IGN)
            # make us tty's foreground again
            try:
                os.tcsetpgrp(sys.stdin.fileno(), old_pgrp)
            except Exception as e:
                pass
            # now restore the handler
            signal.signal(signal.SIGTTOU, hdlr)
            # restore terminal attributes
            if termios:
                termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_attr)

    return ret

def poll_process(
        proc: subprocess.Popen,
        line_callback: Callable[[bytes], Any],
        timeout_seconds: Union[int, float, None] = None,
        timeout_callback: Optional[Callable[[Any], Any]] = None,
        timeout_callback_args: Optional[Tuple[Any]] = None,
        timeout_callback_kwargs: Optional[Dict[str, Any]] = None,
    ) -> int:
    """
    Poll a process and execute a callback function for each line printed to the process's `stdout`.
    """
    from meerschaum.utils.threading import Timer

    def timeout_handler():
        nonlocal timeout_callback_args, timeout_callback_kwargs
        proc.terminate()
        if timeout_callback_args is None:
            timeout_callback_args = []
        if timeout_callback_kwargs is None:
            timeout_callback_kwargs = {}
        timeout_callback(*timeout_callback_args, **timeout_callback_kwargs)

    if timeout_seconds is not None:
        watchdog_thread = Timer(timeout_seconds, timeout_handler)
        watchdog_thread.daemon = True
        watchdog_thread.start()

    while proc.poll() is None:
        line = proc.stdout.readline()
        line_callback(line)
    if timeout_seconds is not None:
        watchdog_thread.cancel()
    return proc.poll()
