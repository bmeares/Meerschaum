#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
If configured, check `api:mrsm` for announcement messages.
"""

import json
from datetime import datetime, timezone, timedelta

import meerschaum as mrsm
import meerschaum.config.paths as paths
from meerschaum.utils.typing import Union, SuccessTuple, Optional
from meerschaum.config import get_config
from meerschaum.utils.formatting import CHARSET, ANSI, colored
from meerschaum.utils.misc import string_width, remove_ansi
from meerschaum.utils.threading import Thread


def cache_remote_version(debug: bool = False) -> SuccessTuple:
    """
    Fetch and cache the latest version if available.
    """
    allow_update_check = get_config('shell', 'updates', 'check_remote')
    if not allow_update_check:
        return True, "Update checks are disabled."

    refresh_minutes = get_config('shell', 'updates', 'refresh_minutes')
    update_delta = timedelta(minutes=refresh_minutes)

    if paths.UPDATES_CACHE_PATH.exists():
        try:
            with open(paths.UPDATES_CACHE_PATH, 'r', encoding='utf8') as f:
                cache_dict = json.load(f)
        except Exception:
            cache_dict = {}
    else:
        cache_dict = {}

    now = datetime.now(timezone.utc)
    last_check_ts_str = cache_dict.get('last_check_ts')
    last_check_ts = datetime.fromisoformat(last_check_ts_str) if last_check_ts_str else None

    need_update = (
        last_check_ts_str is None
        or ((now - last_check_ts) >= update_delta)
    )

    if not need_update:
        return True, "No updates are needed."

    try:
        conn = mrsm.get_connector('api:mrsm')
        remote_version = conn.get_mrsm_version(debug=debug, timeout=3)
    except Exception:
        remote_version = None

    if remote_version is None:
        return False, "Could not determine remote version."

    with open(paths.UPDATES_CACHE_PATH, 'w+', encoding='utf-8') as f:
        json.dump(
            {
                'last_check_ts': now.isoformat(),
                'remote_version': remote_version,
            },
            f,
        )

    return True, "Updated remote version cache."


def cache_remote_plugin_versions(debug: bool = False) -> SuccessTuple:
    """
    Fetch and cache the latest versions of installed plugins from their origin repos.

    Only plugins with a recorded origin (see `meerschaum.plugins._origins`) are
    checked; locally-authored plugins are skipped.
    """
    allow_update_check = get_config('shell', 'updates', 'check_remote')
    if not allow_update_check:
        return True, "Update checks are disabled."

    refresh_minutes = get_config('shell', 'updates', 'refresh_minutes')
    update_delta = timedelta(minutes=refresh_minutes)

    if paths.PLUGIN_UPDATES_CACHE_PATH.exists():
        try:
            with open(paths.PLUGIN_UPDATES_CACHE_PATH, 'r', encoding='utf8') as f:
                cache_dict = json.load(f)
        except Exception:
            cache_dict = {}
    else:
        cache_dict = {}

    now = datetime.now(timezone.utc)
    last_check_ts_str = cache_dict.get('last_check_ts')
    last_check_ts = datetime.fromisoformat(last_check_ts_str) if last_check_ts_str else None

    need_update = (
        last_check_ts_str is None
        or ((now - last_check_ts) >= update_delta)
    )

    if not need_update:
        return True, "No updates are needed."

    from meerschaum.plugins._origins import read_plugin_origins
    origins = read_plugin_origins(debug=debug)

    plugins_versions = {}
    for name, repo_keys in origins.items():
        local_version = _get_local_plugin_version(name)
        if local_version is None:
            continue
        remote_version = _get_remote_plugin_version(name, repo_keys, debug=debug)
        plugins_versions[name] = {
            'repo_keys': repo_keys,
            'local': local_version,
            'remote': remote_version,
        }

    try:
        with open(paths.PLUGIN_UPDATES_CACHE_PATH, 'w+', encoding='utf-8') as f:
            json.dump(
                {
                    'last_check_ts': now.isoformat(),
                    'plugins': plugins_versions,
                },
                f,
            )
    except Exception:
        return False, "Could not write plugins update cache."

    return True, "Updated remote plugin version cache."


def _get_local_plugin_version(name: str) -> Optional[str]:
    """Return a plugin's local `__version__` by reading its file (no import)."""
    import re
    import ast
    from meerschaum.core import Plugin
    try:
        fpath = Plugin(name).__file__
    except Exception:
        fpath = None
    if not fpath:
        return None
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            for line in f:
                if '__version__' not in line:
                    continue
                if not re.search(r'__version__(\s?)=', line.strip()):
                    continue
                return ast.literal_eval(line.split('=')[1].strip())
    except Exception:
        return None
    return None


def _get_remote_plugin_version(
    name: str,
    repo_keys: str,
    debug: bool = False,
) -> Union[str, None]:
    """Return the latest version of a plugin from its origin repository."""
    try:
        from meerschaum.core import Plugin
        from meerschaum.connectors.parse import parse_repo_keys
        conn = parse_repo_keys(repo_keys)
        _warn = conn.__dict__.pop('_warn', None)
        conn._warn = False
        plugin = Plugin(name, repo=repo_keys)
        version = conn.get_plugin_version(plugin, debug=debug)
        if _warn is not None:
            conn._warn = _warn
    except Exception:
        return None
    return version


def run_version_check_thread(debug: bool = False) -> Union[Thread, None]:
    """
    Run the version update check in a separate thread.
    """
    allow_update_check = get_config('shell', 'updates', 'check_remote')
    if not allow_update_check:
        return None

    thread = Thread(
        target=_cache_remote_versions,
        daemon=True,
        kwargs={'debug': debug},
    )
    thread.start()
    return thread


def _cache_remote_versions(debug: bool = False) -> None:
    """Refresh both the core and plugin version caches."""
    try:
        cache_remote_version(debug=debug)
    except Exception:
        pass
    try:
        cache_remote_plugin_versions(debug=debug)
    except Exception:
        pass


_remote_version: Optional[str] = None
def get_remote_version_from_cache() -> Optional[str]:
    """
    Return the version string from the local cache file.
    """
    global _remote_version
    try:
        with open(paths.UPDATES_CACHE_PATH, 'r', encoding='utf-8') as f:
            cache_dict = json.load(f)
    except Exception:
        return None

    _remote_version = cache_dict.get('remote_version')
    return _remote_version


_out_of_date: Optional[bool] = None
def mrsm_out_of_date() -> bool:
    """
    Determine whether to print the upgrade message.
    """
    global _out_of_date
    if _out_of_date is not None:
        return _out_of_date

    ### NOTE: Remote version is cached asynchronously.
    if not paths.UPDATES_CACHE_PATH.exists():
        return False

    remote_version_str = get_remote_version_from_cache()

    packaging_version = mrsm.attempt_import('packaging.version')
    current_version = packaging_version.parse(mrsm.__version__)
    remote_version = packaging_version.parse(remote_version_str)

    _out_of_date = remote_version > current_version
    return _out_of_date


def get_update_message() -> str:
    """
    Return the formatted message for when the current version is behind the latest release.
    """
    if not mrsm_out_of_date():
        return ''

    intro = get_config('shell', CHARSET, 'intro')
    update_message = get_config('shell', CHARSET, 'update_message')
    remote_version = get_remote_version_from_cache()
    if not remote_version:
        return ''

    intro_width = string_width(intro)
    msg_width = string_width(update_message)
    update_left_padding = ' ' * ((intro_width - msg_width) // 2)

    update_line = (
        colored(
            update_message,
            *get_config('shell', 'ansi', 'update_message', 'color')
        ) if ANSI
        else update_message
    )
    update_instruction = (
        colored("Run ", 'white')
        + colored("upgrade mrsm", 'green')
        + colored(" to install ", 'white')
        + colored(f'v{remote_version}', 'yellow')
        + '.'
    )
    update_instruction_clean = remove_ansi(update_instruction)
    instruction_width = string_width(update_instruction_clean)
    instruction_left_padding = ' ' * ((intro_width - instruction_width) // 2)

    return (
        '\n\n'
        + update_left_padding
        + update_line
        + '\n'
        + instruction_left_padding
        + update_instruction
    )


_stale_plugins: Optional[list] = None
def get_stale_plugins() -> list:
    """
    Return a list of `(name, local_version, remote_version)` tuples for plugins
    whose origin repository has a newer version than the installed one.
    """
    global _stale_plugins
    if _stale_plugins is not None:
        return _stale_plugins

    _stale_plugins = []
    if not paths.PLUGIN_UPDATES_CACHE_PATH.exists():
        return _stale_plugins

    try:
        with open(paths.PLUGIN_UPDATES_CACHE_PATH, 'r', encoding='utf-8') as f:
            cache_dict = json.load(f)
    except Exception:
        return _stale_plugins

    plugins_versions = cache_dict.get('plugins', {})
    if not isinstance(plugins_versions, dict):
        return _stale_plugins

    packaging_version = mrsm.attempt_import('packaging.version')
    for name, versions in plugins_versions.items():
        if not isinstance(versions, dict):
            continue
        local_version = versions.get('local')
        remote_version = versions.get('remote')
        if not local_version or not remote_version:
            continue
        try:
            if packaging_version.parse(remote_version) > packaging_version.parse(local_version):
                _stale_plugins.append((name, local_version, remote_version))
        except Exception:
            continue

    return _stale_plugins


def get_plugins_update_message() -> str:
    """
    Return the formatted message listing installed plugins behind their origin repos.
    """
    stale_plugins = get_stale_plugins()
    if not stale_plugins:
        return ''

    header = (
        colored("The following plugins are out of date:", 'yellow')
        if ANSI
        else "The following plugins are out of date:"
    )
    lines = [header]
    for name, local_version, remote_version in stale_plugins:
        plugin_label = colored(name, 'green') if ANSI else name
        version_label = (
            colored(f'v{local_version}', 'yellow') + ' → ' + colored(f'v{remote_version}', 'yellow')
            if ANSI
            else f'v{local_version} → v{remote_version}'
        )
        lines.append(f"  - {plugin_label} ({version_label})")

    instruction = (
        colored("Run ", 'white')
        + colored("upgrade plugins", 'green')
        + colored(" to update.", 'white')
        if ANSI
        else "Run upgrade plugins to update."
    )
    lines.append(instruction)

    return '\n\n' + '\n'.join(lines)
