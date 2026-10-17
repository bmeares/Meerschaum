#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Locate and run the native `mrsm` binary (the `meerschaum-cli` wheel).

`python -m meerschaum`, the `meerschaum` console script, `Shell.cmdloop()` and every façade that
needs a terminal program use this module. Only the standard library is imported.
"""

from __future__ import annotations

import os
import sys
import shutil
import sysconfig
from typing import Dict, List, Optional

BINARY_ENV = 'MRSM_BIN'
BINARY_NAME = 'mrsm.exe' if os.name == 'nt' else 'mrsm'

MISSING_MESSAGE = (
    "The `mrsm` binary was not found. Meerschaum runs its commands in the native `mrsm` program "
    "from the `meerschaum-cli` package: install it with `pip install meerschaum-cli` "
    "(`pip install meerschaum` depends on it), or set the environment variable "
    f"`{BINARY_ENV}` to the path of the binary."
)


class BinaryNotFoundError(FileNotFoundError):
    """The `mrsm` binary could not be found."""


def _is_binary(path: str) -> bool:
    """
    Return whether `path` is an executable program and not a Python script: a console script
    named `mrsm` that runs `python -m meerschaum` would start itself again.
    """
    if not (os.path.isfile(path) and os.access(path, os.X_OK)):
        return False
    try:
        with open(path, 'rb') as f:
            return f.read(2) != b'#!'
    except OSError:
        return False


def find_binary() -> Optional[str]:
    """
    Return the path of the `mrsm` binary, or `None`.

    Looked up, in order: `$MRSM_BIN`; the scripts directory of this interpreter (where `pip`
    installs the `meerschaum-cli` wheel); `$PATH`; a `target/{release,debug}/mrsm` build of a
    source checkout.
    """
    candidates: List[str] = []
    from_env = os.environ.get(BINARY_ENV, '')
    if from_env:
        if _is_binary(from_env):
            candidates.append(from_env)
        else:
            import warnings
            warnings.warn(
                f"`{BINARY_ENV}={from_env}` is not an executable `mrsm` binary; ignoring it.",
                RuntimeWarning,
                stacklevel=2,
            )
    scripts_dirs = []
    try:
        scripts_dirs.append(sysconfig.get_path('scripts'))
    except Exception:
        pass
    scripts_dirs.append(os.path.dirname(os.path.abspath(sys.executable)))
    candidates.extend(os.path.join(scripts_dir, BINARY_NAME) for scripts_dir in scripts_dirs if scripts_dir)
    on_path = shutil.which(BINARY_NAME)
    if on_path:
        candidates.append(on_path)
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ### A source checkout: the most recent build wins.
    builds = [
        os.path.join(repo_root, 'target', profile, BINARY_NAME)
        for profile in ('release', 'debug')
    ]
    candidates.extend(
        sorted(
            (build for build in builds if os.path.isfile(build)),
            key=os.path.getmtime,
            reverse=True,
        )
    )
    for candidate in candidates:
        if _is_binary(candidate):
            return candidate
    return None


def _environment() -> Dict[str, str]:
    """
    The environment of the child: ours, with `MRSM_PYTHON` defaulting to this interpreter so
    workers run Python plugins in the same environment. This process's own environment is
    not modified.
    """
    env = dict(os.environ)
    if sys.executable:
        env.setdefault('MRSM_PYTHON', sys.executable)
    return env


def exec_binary(sysargs: Optional[List[str]] = None) -> None:
    """
    Replace this process with `mrsm <sysargs>` (a child process on Windows).
    Exits with `127` and a message on stderr when the binary is missing.
    """
    binary = find_binary()
    if binary is None:
        print(MISSING_MESSAGE, file=sys.stderr)
        sys.exit(127)
    args = list(sys.argv[1:] if sysargs is None else sysargs)
    env = _environment()
    sys.stdout.flush()
    sys.stderr.flush()
    if os.name == 'nt':
        import subprocess
        sys.exit(subprocess.call([binary, *args], env=env))
    os.execve(binary, [binary, *args], env)


def run_binary(sysargs: Optional[List[str]] = None) -> int:
    """Run `mrsm <sysargs>` as a child process attached to this terminal and return its exit code."""
    import subprocess
    binary = find_binary()
    if binary is None:
        raise BinaryNotFoundError(MISSING_MESSAGE)
    return subprocess.call([binary, *list(sysargs or [])], env=_environment())
