#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Copyright 2020–2026 Bennett Meares

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import sys as _sys


def _cli_launch() -> bool:
    """
    Whether this process is the `mrsm` command line, which does not need the façade.

    `import meerschaum` is EAGER, exactly as it always was (the façade is imported here, under this
    package's import lock, so threads which import `meerschaum.*` and read `meerschaum.Pipe` at the
    same time cannot deadlock). The one exception is the CLI process: `mrsm` (the console script
    `_meerschaum_mrsm`, which sets `sys._meerschaum_cli`) and `python -m meerschaum` (which jobs
    run; `sys.orig_argv` starts with `-m meerschaum`) only need the extension, and importing the
    façade costs about 0.12 s. There the façade is imported on first use instead (PEP 562).
    """
    if getattr(_sys, '_meerschaum_cli', False):
        return True
    argv = getattr(_sys, 'orig_argv', None) or []
    skip = False
    for index, arg in enumerate(argv[1:], start=1):
        if skip:
            skip = False
            continue
        if arg == '-m':
            return argv[index + 1:index + 2] == ['meerschaum']
        if arg == '-mmeerschaum':
            return True
        if arg in ('-X', '-W', '--check-hash-based-pycs'):
            skip = True
            continue
        if not arg.startswith('-'):
            return False
    return False


_LAZY = _cli_launch()
if not _LAZY:
    import atexit

    from meerschaum.utils.typing import SuccessTuple
    from meerschaum.utils.packages import attempt_import
    from meerschaum.core.Pipe import Pipe
    from meerschaum.core.Pipe._sync import SyncResult
    from meerschaum.core.Pipe._status import get_sync_statuses
    from meerschaum.core.Pipeline import Pipeline, Plan, RunResult, get_pipelines, get_runs
    from meerschaum.core.Team import Team, get_teams
    from meerschaum.plugins import Plugin
    from meerschaum.utils.venv import Venv
    from meerschaum.jobs import Job, make_executor
    from meerschaum.connectors import get_connector, Connector, InstanceConnector, make_connector
    from meerschaum.utils import get_pipes
    from meerschaum.utils.formatting import pprint
    from meerschaum._internal.docs import index as __doc__
    from meerschaum.config import __version__, get_config
    from meerschaum._internal.entry import entry
    from meerschaum.__main__ import _close_pools

    atexit.register(_close_pools)

else:
    import importlib as _importlib

    _FACADE = 'meerschaum._internal._facade'
    _FACADE_NAMES = (
        'atexit',
        'SuccessTuple',
        'attempt_import',
        'Pipe',
        'SyncResult',
        'get_sync_statuses',
        'Pipeline',
        'Plan',
        'RunResult',
        'get_pipelines',
        'get_runs',
        'Team',
        'get_teams',
        'Plugin',
        'Venv',
        'Job',
        'make_executor',
        'get_connector',
        'Connector',
        'InstanceConnector',
        'make_connector',
        'get_pipes',
        'pprint',
        '__doc__',
        '__version__',
        'get_config',
        'entry',
        '_close_pools',
    )


__pdoc__ = {'gui': False, 'api': False, 'core': False, '_internal': False}
__all__ = (
    "get_pipes",
    "get_pipelines",
    "get_runs",
    "get_sync_statuses",
    "get_teams",
    "get_connector",
    "get_config",
    "Pipe",
    "Pipeline",
    "Plan",
    "RunResult",
    "SyncResult",
    "Team",
    "Plugin",
    "SuccessTuple",
    "Venv",
    "Job",
    "make_executor",
    "pprint",
    "attempt_import",
    "actions",
    "config",
    "connectors",
    "jobs",
    "plugins",
    "utils",
    "Connector",
    "InstanceConnector",
    "make_connector",
    "entry",
)


if _LAZY:
    def _facade_module():
        """
        Import the façade (once) and return its module: complete, or partial when called while the
        façade itself is being imported (a circular import sees the names bound so far, as it did
        when they were bound in this module's body).
        """
        return _importlib.import_module(_FACADE)


    def _bind_facade(facade) -> None:
        """Bind the façade's names in this namespace once its import has finished."""
        spec = getattr(facade, '__spec__', None)
        if getattr(spec, '_initializing', False):
            return
        namespace = globals()
        for name in _FACADE_NAMES:
            if name in vars(facade):
                namespace[name] = vars(facade)[name]


    def __getattr__(name: str):
        """Resolve a façade name, importing the façade on first use (PEP 562)."""
        if name.startswith('__') and name.endswith('__') and name != '__version__':
            ### Probes (`__wrapped__`, `__file__`, ...) never import anything.
            raise AttributeError(f"module 'meerschaum' has no attribute '{name}'")
        submodule = _sys.modules.get(f'meerschaum.{name}')
        if submodule is not None:
            ### A submodule being imported right now (`import meerschaum.config._paths as _paths` reads
            ### `meerschaum.config` before the import of it has finished): never a reason to load the façade.
            return submodule
        if name == '__version__':
            ### The version needs the config package, not the façade.
            from meerschaum.config._version import __version__
            return __version__
        if name not in _FACADE_NAMES and name not in __all__:
            ### Anything else is a submodule (`from meerschaum import _core`, `mrsm.core`): import just
            ### that one; the façade stays unloaded.
            try:
                return _importlib.import_module(f'meerschaum.{name}')
            except ModuleNotFoundError as e:
                if e.name != f'meerschaum.{name}':
                    raise
            raise AttributeError(f"module 'meerschaum' has no attribute '{name}'")
        facade = _facade_module()
        _bind_facade(facade)
        namespace = globals()
        if name in namespace:
            return namespace[name]
        if name in vars(facade) and name in _FACADE_NAMES:
            return vars(facade)[name]
        if name in __all__:
            ### A submodule the façade did not import as a side effect (`mrsm.plugins`, ...).
            return _importlib.import_module(f'meerschaum.{name}')
        raise AttributeError(f"module 'meerschaum' has no attribute '{name}'")


    def __dir__():
        _bind_facade(_facade_module())
        return sorted(set(globals()) | set(__all__))
