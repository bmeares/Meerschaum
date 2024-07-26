#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
If configured, check `api:mrsm` for announcement messages.
"""

import json
from datetime import datetime, timezone, timedelta

import meerschaum as mrsm
from meerschaum.utils.typing import Union, SuccessTuple, Optional
from meerschaum.config import get_config
from meerschaum.utils.formatting import CHARSET, ANSI, colored
from meerschaum.utils.misc import string_width, remove_ansi
from meerschaum.config.paths import (
    UPDATES_LOCK_PATH,
    UPDATES_CACHE_PATH,
)
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

    if UPDATES_CACHE_PATH.exists():
        try:
            with open(UPDATES_CACHE_PATH, 'r', encoding='utf8') as f:
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

    with open(UPDATES_CACHE_PATH, 'w+', encoding='utf-8') as f:
        json.dump(
            {
                'last_check_ts': now.isoformat(),
                'remote_version': remote_version,
            },
            f,
        )

    return True, "Updated remote version cache."


def run_version_check_thread(debug: bool = False) -> Union[Thread, None]:
    """
    Run the version update check in a separate thread.
    """
    allow_update_check = get_config('shell', 'updates', 'check_remote')
    if not allow_update_check:
        return None

    thread = Thread(
        target=cache_remote_version,
        daemon=True,
        kwargs={'debug': debug},
    )
    thread.start()
    return thread


_remote_version: Optional[str] = None
def get_remote_version_from_cache() -> Optional[str]:
    """
    Return the version string from the local cache file.
    """
    global _remote_version
    try:
        with open(UPDATES_CACHE_PATH, 'r', encoding='utf-8') as f:
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
    if not UPDATES_CACHE_PATH.exists():
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
