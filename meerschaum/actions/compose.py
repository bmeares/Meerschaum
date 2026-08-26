#! /usr/bin/env python3

"""Manage Meerschaum environments with Compose."""

import pathlib

from meerschaum.utils.typing import SuccessTuple, Optional, List, Any


def compose(
    action: Optional[List[str]] = None,
    file: Optional[pathlib.Path] = None,
    env_file: Optional[pathlib.Path] = None,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """Manage an isolated Meerschaum environment with Meerschaum Compose."""
    from meerschaum.compose import warn_if_compose_plugin_installed
    from meerschaum.compose.subactions import _do_subaction
    warn_if_compose_plugin_installed()
    subaction = action[0] if action else 'default'
    return _do_subaction(
        subaction,
        action=(action or []),
        file=file,
        env_file=env_file,
        debug=debug,
        **kwargs
    )


def complete_compose(action: Optional[List[str]] = None, **kwargs: Any) -> List[str]:
    """Complete Compose subaction names."""
    from meerschaum.compose.subactions import get_subactions
    subactions = sorted(name for name in get_subactions() if name != 'default')
    if not action:
        return subactions
    return [
        name
        for name in subactions
        if name.startswith(action[0]) and action[0] != name
    ]


### Compose swaps process-wide config, environment, and plugin state and therefore
### must never run inside the persistent CLI daemon.
from meerschaum.plugins import _actions_daemon_enabled
_actions_daemon_enabled['compose'] = False
