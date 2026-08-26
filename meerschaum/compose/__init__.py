#! /usr/bin/env python3

"""Manage isolated Meerschaum projects from a Compose YAML file."""

import meerschaum as mrsm
from meerschaum.compose.sync import sync

_warned_about_plugin = False


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
