#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Run the `mrsm` command line from Python.

`pip install meerschaum` installs one wheel. The `mrsm` console script, `python -m meerschaum`,
`Shell.cmdloop()` and the processes the CLI starts again (jobs, compose, `systemd` units) run the
CLI in-process from the `_native` extension (`_native.main(argv)`): the same program as the
standalone `mrsm` binary of `cargo install meerschaum`, with the same file descriptors, signals
and exit codes. Setting `MRSM_BIN` to a standalone binary runs that instead.

Only the standard library is imported here, and nothing from the façade: starting `mrsm` must not
pay for `import meerschaum.Pipe`.
"""

from __future__ import annotations

import os
import sys

BINARY_ENV = 'MRSM_BIN'

### Set to the path of `MRSM_BIN` by the launcher which hands the process over to it. Seen again with
### the same `MRSM_BIN`, this process was started *by* that "binary": a launcher for the console script
### (pip's `mrsm.exe` on Windows), which would start this process again, forever. See `explicit_binary`.
BINARY_GUARD_ENV = 'MRSM_BIN_EXEC'

### `-P` keeps the working directory off `sys.path` (`-m` and `-c` put it first). Everything the CLI
### starts runs in the user's directory, where a `meerschaum.py` or a `meerschaum/` would replace the
### package. The `mrsm` console script never had it there (`sys.path[0]` is the scripts directory).
PYTHON_FLAGS = ['-P']


def explicit_binary() -> str | None:
    """
    Return the standalone `mrsm` binary named by `$MRSM_BIN`, or `None` when it is unset, not an
    executable program (a Python script named `mrsm` would start this process again), or the launcher
    this very process was started from (`$MRSM_BIN_EXEC` names it too).
    """
    from_env = os.environ.get(BINARY_ENV, '')
    if not from_env:
        return None
    if os.environ.get(BINARY_GUARD_ENV, '') == from_env:
        import warnings
        warnings.warn(
            f"`{BINARY_ENV}={from_env}` started this process again (it is a launcher for the "
            "`mrsm` console script, not a standalone binary); ignoring it.",
            RuntimeWarning,
            stacklevel=2,
        )
        return None
    try:
        usable = os.path.isfile(from_env) and os.access(from_env, os.X_OK)
        if usable:
            with open(from_env, 'rb') as f:
                usable = f.read(2) != b'#!'
    except OSError:
        usable = False
    if usable:
        return from_env
    import warnings
    warnings.warn(
        f"`{BINARY_ENV}={from_env}` is not an executable `mrsm` binary; ignoring it.",
        RuntimeWarning,
        stacklevel=2,
    )
    return None


def is_python_executable(path: str | None) -> bool:
    """
    Whether `path` names a Python interpreter (`python3.13`, `python.exe`, `pypy3`, ...) and not the
    program embedding Python (`uwsgi`, `httpd`, a frozen application), which `sys.executable` can be.
    """
    if not path:
        return False
    name = path.replace('\\', '/').rsplit('/', 1)[-1].lower()
    if name.endswith('.exe'):
        name = name[:-4]
    return name.startswith(('python', 'pypy'))


def interpreter() -> str:
    """
    Return the Python interpreter which runs the CLI again: `sys.executable`, or `sys._base_executable`
    when `sys.executable` is a host program (mod_wsgi, uWSGI). The same choice as the native code's.
    """
    for candidate in (sys.executable, getattr(sys, '_base_executable', None)):
        if is_python_executable(candidate):
            return candidate
    raise RuntimeError(
        f"Cannot start another Meerschaum process: `sys.executable` ({sys.executable!r}) is not a "
        "Python interpreter (an embedded Python, such as mod_wsgi or uWSGI). "
        f"Set `{BINARY_ENV}` to a standalone `mrsm` binary (`cargo install meerschaum`)."
    )


def self_command() -> list[str]:
    """
    Return the command which runs the CLI again from this process: the `MRSM_BIN` binary, else
    `[python, '-P', '-m', 'meerschaum']` (the same one the native jobs and compose code use).
    """
    binary = explicit_binary()
    if binary:
        return [binary]
    return [interpreter(), *PYTHON_FLAGS, '-m', 'meerschaum']


def _environment(binary: str | None = None) -> dict[str, str]:
    """
    The environment of a standalone binary: ours, with `MRSM_PYTHON` defaulting to this interpreter
    so workers run Python plugins in the same environment, and `MRSM_BIN_EXEC` naming `binary` (see
    `BINARY_GUARD_ENV`). This process's own environment is not modified.
    """
    env = dict(os.environ)
    if is_python_executable(sys.executable):
        env.setdefault('MRSM_PYTHON', sys.executable)
    if binary:
        env[BINARY_GUARD_ENV] = os.environ.get(BINARY_ENV, binary)
    return env


def _extension():
    """Return the native extension module, or exit with the reason it is missing."""
    from meerschaum import _core
    extension = _core.load()
    if extension is None:
        print(
            f"The Meerschaum native extension is not available: {_core.LOAD_ERROR}",
            file=sys.stderr,
        )
        sys.exit(1)
    return extension


def run_cli(sysargs: list[str] | None = None) -> int:
    """
    Run `mrsm <sysargs>` and return its exit code: in this process (`_native.main`), or as a
    child process of the `MRSM_BIN` binary.
    """
    args = list(sys.argv[1:] if sysargs is None else sysargs)
    binary = explicit_binary()
    if binary:
        import subprocess
        return subprocess.call([binary, *args], env=_environment(binary))
    return _extension().main(args)


def run_binary(sysargs: list[str] | None = None) -> int:
    """Run `mrsm <sysargs>` attached to this terminal and return its exit code."""
    return run_cli(list(sysargs or []))


def main(sysargs: list[str] | None = None) -> None:
    """
    The entry point of the `mrsm` and `meerschaum` console scripts and of `python -m meerschaum`:
    run the CLI and exit with its status. With `MRSM_BIN` set, the process is replaced by that
    binary (a child process on Windows).
    """
    args = list(sys.argv[1:] if sysargs is None else sysargs)
    binary = explicit_binary()
    if binary:
        env = _environment(binary)
        sys.stdout.flush()
        sys.stderr.flush()
        if os.name == 'nt':
            import subprocess
            sys.exit(subprocess.call([binary, *args], env=env))
        os.execve(binary, [binary, *args], env)
    rc = _extension().main(args)
    if rc == 75:  # Rust's RESTART_SHELL; replace the old in-process engine.
        sys.stdout.flush()
        sys.stderr.flush()
        command = [sys.executable, *PYTHON_FLAGS, '-m', 'meerschaum', '--shell']
        if os.name == 'nt':
            import subprocess
            sys.exit(subprocess.call(command))
        os.execv(sys.executable, command)
    ### A process exit status is 0-255 (`meerschaum_cli::run::exit_code`: anything else is 1).
    rc = rc if 0 <= rc <= 255 else 1
    sys.stdout.flush()
    sys.stderr.flush()
    package = sys.modules.get('meerschaum')
    lazy = getattr(package, '_LAZY', False)
    if not lazy or 'meerschaum._internal._facade' in sys.modules:
        ### An eager `import meerschaum` (a script calling `main()`) or an imported façade: the caller's
        ### `atexit` handlers and buffered files, and the façade's pools, must run (v4's `main()` ended in
        ### `sys.exit`).
        sys.exit(rc)
    ### The CLI process (`mrsm`, `python -m meerschaum`) and nothing of Python's is left to finalize
    ### (the CLI wrote to the real file descriptors): skip interpreter teardown.
    os._exit(rc)
