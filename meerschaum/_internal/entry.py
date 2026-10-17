#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
# type: ignore

"""
The entry point for launching Meerschaum actions.

`entry()` runs a command line in the native dispatcher (`_core.entry`), the same one the `mrsm`
binary uses. The only Python left is `_do_action_wrapper()`, which the plugin worker calls to run
the actions of Python plugins.
"""

from __future__ import annotations

import os
import shlex

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, List, Optional, Dict, Callable, Any, Union


def _plain_value(value: Any) -> bool:
    """Whether a connector attribute is data that can cross to Rust and to a worker."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, (list, tuple)):
        return all(_plain_value(item) for item in value)
    if isinstance(value, dict):
        return all(isinstance(k, str) and _plain_value(v) for k, v in value.items())
    return False


def _is_configured(connector_type: str, label: str) -> bool:
    """Whether the config registry or the environment (`MRSM_SQL_MAIN`) defines the connector."""
    if mrsm.get_config('meerschaum', 'connectors', connector_type, label, warn=False) is not None:
        return True
    env_key = f'MRSM_{connector_type}_{label}'.upper()
    return any(key.upper() == env_key for key in os.environ)


def adhoc_context() -> Optional[Dict[str, Any]]:
    """
    Return the connectors this process defined itself (`mrsm.get_connector('sql', 'x', **attrs)`)
    that the native dispatcher cannot find in the config registry, for `_core.entry()`.

    The context holds `connectors` (the `_core.SqlConnector` of each SQL connector with a Rust
    driver) and `attributes` (what rebuilds every such connector, native or in a worker).
    """
    import meerschaum.connectors as connectors_module
    context: Dict[str, Any] = {'connectors': {}, 'attributes': {}}
    for connector_type, by_label in list(connectors_module.connectors.items()):
        if connector_type == 'plugin':
            continue
        for label, connector in list(by_label.items()):
            if connector is None or _is_configured(connector_type, label):
                continue
            keys = f'{connector_type}:{label}'
            meta = dict(getattr(connector, 'meta', None) or {})
            meta.pop('type', None)
            meta.pop('label', None)
            context['attributes'][keys] = {
                key: value for key, value in meta.items() if _plain_value(value)
            }
            if connector_type == 'sql':
                try:
                    context['connectors'][keys] = connector._core_connector()
                except Exception:
                    ### No Rust driver (in-memory SQLite, ...): the native side reports it.
                    pass
    return context if context['attributes'] else None


def entry(
    sysargs: Union[List[str], str, None] = None,
    _patch_args: Optional[Dict[str, Any]] = None,
    _use_cli_daemon: bool = True,
    _session_id: Optional[str] = None,
) -> SuccessTuple:
    """
    Parse arguments and launch a Meerschaum action.

    The action runs natively, in this process. Ctrl-C asks a looping action to stop and the call
    then raises `KeyboardInterrupt`; Python delivers signals to the main thread only, so a call
    made from another thread cannot be interrupted. Inside a plugin worker, a call made while a plugin module is being imported (at the top
    level of the module) runs without plugins, so the import cannot recurse.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    return entry_without_daemon(sysargs, _patch_args=_patch_args)


def entry_without_daemon(
    sysargs: Union[List[str], str, None] = None,
    _patch_args: Optional[Dict[str, Any]] = None,
) -> SuccessTuple:
    """
    Parse arguments and launch a Meerschaum action.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    from meerschaum import _core
    if sysargs is None:
        sysargs = []
    if not isinstance(sysargs, list):
        sysargs = shlex.split(sysargs)
    return _core.entry([str(arg) for arg in sysargs], _patch_args or None, adhoc_context())


def entry_with_args(
    _actions: Optional[Dict[str, Callable[[Any], SuccessTuple]]] = None,
    _patch_args: Optional[Dict[str, Any]] = None,
    **kw
) -> SuccessTuple:
    """Execute a Meerschaum action with keyword arguments.
    Use `entry()` for parsing sysargs before executing.
    """
    from meerschaum import _core
    if _patch_args:
        kw.update(_patch_args)
    action = list(kw.pop('action', None) or [])
    return _core.run_action(action, kw, adhoc_context())


def _do_action_wrapper(
    action_function,
    plugin_name,
    _skip_schedule: bool = False,
    **kw
):
    from meerschaum.plugins import Plugin
    from meerschaum.utils.venv import Venv
    from meerschaum.utils.misc import filter_keywords
    plugin = Plugin(plugin_name) if plugin_name else None
    with Venv(plugin, debug=kw.get('debug', False)):
        action_name = ' '.join(action_function.__name__.split('_') + kw.get('action', []))
        try:
            result = action_function(**filter_keywords(action_function, **kw))
        except Exception as e:
            from meerschaum.utils.daemon import running_in_daemon
            log_traceback = kw.get('debug', True) or running_in_daemon()
            if log_traceback:
                import traceback
                traceback.print_exception(type(e), e, e.__traceback__)
            result = False, (
                f"Failed to execute `{action_name.strip()}` "
                + f"with `{type(e).__name__}`"
                + (f':\n\n{e}' if str(e) else '.')
                + (
                    "\n\nRun again with '--debug' to see a full stacktrace."
                    if not log_traceback else ''
                )
            )
        except KeyboardInterrupt:
            result = False, f"Cancelled action `{action_name.lstrip()}`."

    if kw.get('schedule', None) and not _skip_schedule:
        mrsm.pprint(result)

    return result


_shell = None
def get_shell(
    sysargs: Optional[List[str]] = None,
    reload: bool = False,
    debug: bool = False,
    mrsm_instance: Optional[str] = None,
    **kwargs: Any
):
    """
    Return the shell object: `get_shell().cmdloop()` runs the native `mrsm` shell.
    """
    global _shell
    from meerschaum._internal.shell import Shell
    if _shell is None or reload:
        _shell = Shell(sysargs=sysargs, instance_keys=mrsm_instance)
    return _shell
