#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the interface for instance connectors.
"""

from __future__ import annotations

import abc
from typing import Any, Union, Dict, List, Tuple, Optional

import meerschaum as mrsm
from meerschaum.connectors._Connector import Connector


class InstanceConnector(Connector):
    """
    Instance connectors define the interface for managing pipes and provide methods
    for management of users, plugins, tokens, and other metadata built atop pipes.
    """

    IS_INSTANCE: bool = True
    IS_THREAD_SAFE: bool = False

    from ._users import (
        get_users_pipe,
        register_user,
        get_user_id,
        get_username,
        get_users,
        edit_user,
        delete_user,
        get_user_password_hash,
        get_user_type,
        get_user_attributes,
    )

    from ._plugins import (
        get_plugins_pipe,
        register_plugin,
        get_plugin_user_id,
        delete_plugin,
        get_plugin_id,
        get_plugin_version,
        get_plugins,
        get_plugin_user_id,
        get_plugin_username,
        get_plugin_attributes,
    )

    from ._tokens import (
        get_tokens_pipe,
        register_token,
        edit_token,
        invalidate_token,
        delete_token,
        get_token,
        get_tokens,
        get_token_model,
        get_token_secret_hash,
        token_exists,
        get_token_scopes,
    )

    from ._pipes import (
        register_pipe,
        get_pipe_attributes,
        get_pipe_id,
        edit_pipe,
        delete_pipe,
        fetch_pipes_keys,
        pipe_exists,
        drop_pipe,
        drop_pipe_indices,
        sync_pipe,
        create_pipe_indices,
        clear_pipe,
        get_pipe_data,
        get_sync_time,
        get_pipe_columns_types,
        get_pipe_columns_indices,
    )
