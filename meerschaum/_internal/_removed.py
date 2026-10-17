#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Clear `ImportError`s for the modules removed in Meerschaum 5.0.

The Python core of v4 (the action modules, the API routes, the Dash console, the shell, ...) was
replaced by the native `mrsm` binary and the `_core` extension. Code that still imports one of those
modules gets a `ModuleNotFoundError` whose message names the replacement instead of a bare
"No module named ...". The finder sits at the end of `sys.meta_path`, so a module that exists is
never affected.

Add an entry to `REMOVED` (longest matching prefix wins) when a module is deleted.
"""

from __future__ import annotations

import sys
import importlib.abc
import importlib.machinery
from typing import Any, Dict, Optional

_ACTIONS = (
    "The standard actions run in the native dispatcher: use `meerschaum.actions.actions[<name>]`, "
    "`meerschaum.entry([...])` or the `mrsm` command."
)
_SERVER = (
    "The API server and the web console are native (`mrsm start api`); "
    "`@api_plugin` routes and plugin-owned Dash apps run in `meerschaum.api.host`."
)
_SHELL = (
    "The interactive shell is the native shell (`mrsm`): run `mrsm`, or use "
    "`meerschaum._internal.entry.get_shell().cmdloop()`, which starts it. The web terminal is part "
    "of `mrsm start api` (the console's Terminal page at `/dash/webterm`; the terminal itself is "
    "served at `/webterm/<session id>`), and there is no separate `start webterm`."
)

### module prefix -> what replaces it
REMOVED: Dict[str, str] = {
    'meerschaum.actions': _ACTIONS,
    'meerschaum.api.routes': _SERVER,
    'meerschaum.api._events': _SERVER,
    'meerschaum.api._websockets': _SERVER,
    'meerschaum.api._chunks': _SERVER,
    'meerschaum.api.dash.callbacks': _SERVER,
    'meerschaum.api.dash.pages': _SERVER,
    'meerschaum.api.dash.jobs': _SERVER,
    'meerschaum.api.dash.plugins': _SERVER,
    'meerschaum.api.dash.tokens': _SERVER,
    'meerschaum.api.dash.users': _SERVER,
    'meerschaum.api.dash.webterm': _SHELL,
    'meerschaum.api.dash.websockets': _SERVER,
    'meerschaum.api.dash.sync': _SERVER,
    'meerschaum._internal.term': _SHELL,
    'meerschaum._internal.cli': (
        "The CLI daemons are not used in Meerschaum 5: actions run natively in `mrsm`."
    ),
    'meerschaum._internal.shell.Shell': _SHELL,
    'meerschaum._internal.shell.ShellCompleter': _SHELL,
    'meerschaum._internal.shell.ValidAutoSuggest': _SHELL,
    'meerschaum._internal.shell.updates': _SHELL,
    'meerschaum.connectors.sql._instance': (
        "The temporary-table bookkeeping of the SQL instance runs in the Rust core."
    ),
}


def removal_message(fullname: str) -> Optional[str]:
    """Return the message for a removed module (or a submodule of one), else `None`."""
    best = None
    for prefix in REMOVED:
        if fullname == prefix or fullname.startswith(prefix + '.'):
            if best is None or len(prefix) > len(best):
                best = prefix
    return REMOVED[best] if best is not None else None


class _RemovedModuleLoader(importlib.abc.Loader):
    """The loader of a removed module: importing it raises the message."""

    def __init__(self, message: str):
        self.message = message

    def create_module(self, spec: Any):
        raise ModuleNotFoundError(
            f"`{spec.name}` was removed in Meerschaum 5.0. {self.message}",
            name=spec.name,
        )

    def exec_module(self, module: Any) -> None:  # pragma: no cover (create_module raises)
        raise ModuleNotFoundError(module.__name__, name=module.__name__)


class RemovedModuleFinder(importlib.abc.MetaPathFinder):
    """
    Give a removed module a spec whose loader raises the `ModuleNotFoundError` naming its
    replacement. Probes (`importlib.util.find_spec()`) do not raise: only importing does.
    """

    def find_spec(self, fullname: str, path: Any = None, target: Any = None):
        message = removal_message(fullname)
        if message is None:
            return None
        return importlib.machinery.ModuleSpec(fullname, _RemovedModuleLoader(message))


def install() -> None:
    """Install the finder (once, at the end of `sys.meta_path`)."""
    if not any(isinstance(finder, RemovedModuleFinder) for finder in sys.meta_path):
        sys.meta_path.append(RemovedModuleFinder())
