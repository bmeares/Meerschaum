#! /usr/bin/env python3

"""Manage isolated Meerschaum projects from a Compose YAML file."""

import meerschaum as mrsm
from meerschaum.utils.typing import Any, List, Optional, SuccessTuple
from meerschaum.compose.sync import sync

_warned_about_plugin = False


def compose(action: Optional[List[str]] = None, **kw: Any) -> SuccessTuple:
    """
    Manage isolated Meerschaum projects from a Compose YAML file (`mrsm compose`).
    The action runs in the native dispatcher.
    """
    from meerschaum import _core
    from meerschaum._internal.entry import adhoc_context
    return _core.run_action(['compose', *(action or [])], kw, adhoc_context())


def complete_compose(*args: Any, **kw: Any) -> List[str]:
    """Completion of `compose` subactions and files happens in the native shell."""
    return []


def has_compose_plugin() -> bool:
    """Return whether the superseded third-party ``compose`` plugin is installed."""
    try:
        return mrsm.Plugin('compose').is_installed()
    except Exception:
        return False


def warn_if_compose_plugin_installed() -> None:
    """Warn once when the superseded third-party plugin is installed."""
    global _warned_about_plugin
    if _warned_about_plugin or not has_compose_plugin():
        return

    from meerschaum.utils.warnings import warn
    warn(
        "The 'compose' plugin is superseded by Meerschaum's built-in compose action. "
        "The built-in action is being used; uninstall the plugin with "
        "`mrsm uninstall plugin compose`.",
        stack=False,
    )
    _warned_about_plugin = True


def get_env_project_name() -> Optional[str]:
    """
    Return the name of the Compose project in which this process is running.

    Returns
    -------
    The active project's name, or `None` if not running in a Compose environment.
    """
    import json
    import os
    import pathlib

    compose_config_str = os.environ.get('MRSM__COMPOSE_CONFIG', None)
    if not compose_config_str:
        return None

    try:
        compose_config = json.loads(compose_config_str)
    except Exception:
        return None

    project_name = compose_config.get('project_name', None)
    if project_name:
        return str(project_name)

    compose_file_str = compose_config.get('__file__', None)
    if not compose_file_str:
        return None

    return pathlib.Path(compose_file_str).parent.stem
