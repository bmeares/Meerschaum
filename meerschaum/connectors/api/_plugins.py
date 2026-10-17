#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage plugins via the API connector (the Rust client, `meerschaum._core.ApiConnector`).
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import Union, Any, Optional, SuccessTuple, List, Dict


def plugin_r_url(
    plugin: Union[mrsm.core.Plugin, str],
) -> str:
    """Generate a relative URL path from a Plugin."""
    from meerschaum._internal.static import STATIC_CONFIG
    return f"{STATIC_CONFIG['api']['endpoints']['plugins']}/{plugin}"


def register_plugin(
    self,
    plugin: mrsm.core.Plugin,
    make_archive: bool = True,
    debug: bool = False,
) -> SuccessTuple:
    """Register a plugin and upload its archive."""
    import os
    archive_path = plugin.make_tar(debug=debug) if make_archive else plugin.archive_path
    try:
        with open(archive_path, 'rb') as f:
            archive = f.read()
    except Exception:
        return False, f"Failed to register plugin '{plugin}'."
    return self._rust_client().register_plugin(
        plugin,
        archive,
        os.path.basename(str(archive_path)),
    )


def install_plugin(
    self,
    name: str,
    skip_deps: bool = False,
    force: bool = False,
    debug: bool = False
) -> SuccessTuple:
    """Download and attempt to install a plugin from the API."""
    import os
    import pathlib
    from meerschaum.core import Plugin
    import meerschaum.config.paths as paths

    dest = pathlib.Path(os.path.join(paths.PLUGINS_TEMP_RESOURCES_PATH, name + '.tar.gz'))
    try:
        archive = self._rust_client().download_plugin(name)
    except ValueError as e:
        return False, str(e)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(archive)
    plugin = Plugin(name, archive_path=dest, repo_connector=self)
    return plugin.install(skip_deps=skip_deps, force=force, debug=debug)


def get_plugins(
    self,
    user_id: Optional[int] = None,
    search_term: Optional[str] = None,
    debug: bool = False
) -> List[str]:
    """Return a list of registered plugin names.

    Parameters
    ----------
    user_id: Optional[int], default None
        If specified, return all plugins from a certain user.

    search_term: Optional[str], default None
        If specified, return plugins beginning with this string.

    Returns
    -------
    A list of plugin names.
    """
    return self._rust_client().get_plugins(user_id=user_id, search_term=search_term)


def get_plugin_attributes(
    self,
    plugin: mrsm.core.Plugin,
    debug: bool = False
) -> Dict[str, Any]:
    """Return a plugin's attributes."""
    return self._rust_client().get_plugin_attributes(plugin)


def get_plugin_version(
    self,
    plugin: mrsm.core.Plugin,
    debug: bool = False
) -> Union[str, None]:
    """Return a plugin's version."""
    return self._rust_client().get_plugin_version(plugin)


def delete_plugin(
    self,
    plugin: mrsm.core.Plugin,
    debug: bool = False
) -> SuccessTuple:
    """Delete a plugin from an API repository."""
    return self._rust_client().delete_plugin(plugin)
