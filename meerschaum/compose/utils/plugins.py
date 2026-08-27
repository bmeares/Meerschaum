#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage plugins in the isolated environment.
"""

from collections import defaultdict

import meerschaum as mrsm
from meerschaum.utils.typing import Dict, Any, List, SuccessTuple, Optional
from meerschaum.utils.warnings import warn, dprint


def get_installed_plugins(
    compose_config: Dict[str, Any],
    debug: bool = False,
) -> List[str]:
    """
    Return a list of plugins in the configured `plugins` directories.
    """
    from meerschaum.config.environment import replace_env
    from meerschaum.plugins import get_plugins_names
    from meerschaum.compose.utils.config import get_env_dict

    with replace_env(get_env_dict(compose_config)):
        return get_plugins_names()


def install_plugins(
    plugins: List[str],
    compose_config: Dict[str, Any],
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Install plugins to the `root/plugins/` directory.
    """
    from meerschaum.compose.utils import run_mrsm_command
    return run_mrsm_command(
        ['install', 'plugins'] + plugins,
        compose_config,
        debug=debug,
    )


def check_and_install_plugins(
    compose_config: Dict[str, Any],
    debug: bool = False,
    _existing_plugins: Optional[List[str]] = None,
) -> SuccessTuple:
    """
    Verify that required plugins are available in the root directory
    and attempt to install missing plugins.
    """
    from meerschaum.config import get_config
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.compose.utils import run_mrsm_command
    configured_plugins = compose_config.get('plugins', [])
    if not configured_plugins:
        if debug:
            dprint("Compose: No plugins are configured, nothing to install.", icon=False)
        return True, "Success"

    if not isinstance(configured_plugins, list):
        return False, "Required plugins must be a list."

    default_repository = compose_config.get(
        'config', {}
    ).get(
        'meerschaum', {}
    ).get(
        'repository', get_config('meerschaum', 'repository')
    )

    plugins_dir_paths = compose_config.get('plugins_dir', [])
    for path in plugins_dir_paths:
        if debug:
            dprint(f"Checking plugin path: {path.as_posix()}")
        if not path.exists():
            try:
                if debug:
                    dprint(f"Creating path: {path.as_posix()}")
                path.mkdir(exist_ok=True, parents=True)
            except Exception as e:
                return False, f"Failed to create plugins path '{path}': {e}"

    required_plugin_parts = [
        plugin_name.split(STATIC_CONFIG['plugins']['repo_separator'])
        for plugin_name in configured_plugins
    ]
    required_plugins = defaultdict(lambda: [])
    for plugin_parts in required_plugin_parts:
        plugin_name = plugin_parts[0]
        repo_keys = (
            plugin_parts[1]
            if len(plugin_parts) > 1
            else default_repository
        )
        required_plugins[repo_keys].append(plugin_name)

    existing_plugins = _existing_plugins or get_installed_plugins(compose_config)
    if debug:
        dprint(f"Compose: Existing plugins: {existing_plugins}")
    plugins_to_install = [
        plugin_parts[0]
        for plugin_parts in required_plugin_parts
        if plugin_parts[0] not in existing_plugins
    ]
    if not plugins_to_install:
        if debug:
            dprint("Compose: No plugins need to be installed.")
        return True, "Required plugins are already installed."

    success, msg = True, ""
    for repo_keys, plugin_names in required_plugins.items():
        install_success, install_msg = run_mrsm_command(
            (
                ['install', 'plugins']
                + plugin_names
                + (['-r', repo_keys] if repo_keys else [])
            ),
            compose_config,
            debug=debug,
            _subprocess=True,
        )
        if not install_success:
            warn(install_msg, stack=False)
        success = success and install_success
        msg += install_msg

    msg = "Success" if success else msg
    return success, msg.rstrip()
