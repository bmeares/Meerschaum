#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with Meerschaum APIs. May be chained together (see 'meerschaum:api_instance' in your config.yaml).
"""

from __future__ import annotations

from meerschaum.utils.typing import Optional, List, Union
from meerschaum.connectors import InstanceConnector

required_attributes = {
    'host',
}

#: Attributes of the Python connector which the Rust client does not use.
_PRIVATE_STATE = ('_rust_client', '_rust_client_pid', '_session')


class APIConnector(InstanceConnector):
    """
    Connect to a Meerschaum API instance.

    The requests are made by the Rust client (`meerschaum._core.ApiConnector`, one per connector
    and process): the same routes, query parameters and bodies as v4. Nothing here imports
    `requests` or `httpx`.
    """

    IS_THREAD_SAFE: bool = False
    OPTIONAL_ATTRIBUTES: List[str] = ['port', 'client_secret', 'client_id', 'api_key']

    from ._request import (
        make_request,
        get,
        post,
        put,
        patch,
        delete,
        wget,
    )
    from ._actions import (
        get_actions,
        do_action,
        do_action_async,
        do_action_legacy,
    )
    from ._misc import get_mrsm_version, get_chaining_status
    from ._pipes import (
        get_pipe_instance_keys,
        register_pipe,
        fetch_pipes_keys,
        edit_pipe,
        sync_pipe,
        delete_pipe,
        delete_pipe_cache,
        get_pipe_data,
        get_pipe_id,
        get_pipe_attributes,
        get_sync_time,
        pipe_exists,
        create_metadata,
        get_pipe_rowcount,
        drop_pipe,
        clear_pipe,
        get_pipe_columns_types,
        get_pipe_columns_indices,
        get_pipe_index_names,
        get_pipe_docs,
        get_pipe_size,
        compress_pipe,
        decompress_pipe,
        vacuum_pipe,
        analyze_pipe,
        partition_pipe,
    )
    from ._fetch import fetch
    from ._plugins import (
        register_plugin,
        install_plugin,
        delete_plugin,
        get_plugins,
        get_plugin_attributes,
        get_plugin_version,
    )
    from ._login import login, test_connection
    from ._users import (
        register_user,
        get_user_id,
        get_users,
        edit_user,
        delete_user,
        get_user_password_hash,
        get_user_type,
        get_user_attributes,
    )
    from ._tokens import (
        register_token,
        get_token_model,
        get_tokens,
        edit_token,
        invalidate_token,
        get_token_scopes,
        token_exists,
        delete_token,
    )
    from ._uri import from_uri
    from ._jobs import (
        get_jobs,
        get_job,
        get_job_metadata,
        get_job_properties,
        get_job_exists,
        delete_job,
        start_job,
        create_job,
        stop_job,
        pause_job,
        get_logs,
        get_job_stop_time,
        monitor_logs,
        monitor_logs_async,
        get_job_is_blocking_on_stdin,
        get_job_prompt_kwargs,
        get_job_began,
        get_job_ended,
        get_job_paused,
        get_job_status,
    )

    def __init__(
        self,
        label: Optional[str] = None,
        wait: bool = False,
        debug: bool = False,
        **kw
    ):
        if 'uri' in kw:
            from_uri_params = self.from_uri(kw['uri'], as_dict=True)
            label = label or from_uri_params.get('label', None)
            _ = from_uri_params.pop('label', None)
            kw.update(from_uri_params)

        super().__init__('api', label=label, **kw)
        if 'protocol' not in self.__dict__:
            self.protocol = (
                'https' if self.__dict__.get('uri', '').startswith('https')
                else 'http'
            )

        if 'uri' not in self.__dict__:
            self.verify_attributes(required_attributes)
        else:
            conn_attrs = self.from_uri(self.__dict__['uri'], as_dict=True)
            self.__dict__.update({
                key: val
                for key, val in conn_attrs.items()
                if key not in ('label', 'protocol')
            })

        self.url = (
            self.protocol + '://' +
            self.host
            + (
                (':' + str(self.port))
                if self.__dict__.get('port', None)
                else ''
            )
        )
        self._rust_client_obj = None
        self._instance_keys = self.__dict__.get('instance_keys', None)

    def _rust_client(self):
        """
        Return the `meerschaum._core.ApiConnector` of this connector (built once per process:
        a forked child builds its own, which holds its own HTTP connections and token).
        """
        import os
        attributes = {
            key: value
            for key, value in self.__dict__.items()
            if not str(key).startswith('_')
            and isinstance(value, (str, int, float, bool))
        }
        client = self.__dict__.get('_rust_client_obj', None)
        if (
            client is not None
            and self.__dict__.get('_rust_client_pid', None) == os.getpid()
            and self.__dict__.get('_rust_client_attributes', None) == attributes
        ):
            return client

        ### A new process, or credentials / address changed (`conn.password = ...`): build a client
        ### with the current attributes (it logs in again; the native flows find it by them).
        from meerschaum import _core
        client = _core.ApiConnector(self.label, attributes)
        self.__dict__['_rust_client_obj'] = client
        self.__dict__['_rust_client_pid'] = os.getpid()
        self.__dict__['_rust_client_attributes'] = attributes
        return client

        from meerschaum import _core
        attributes = {
            key: value
            for key, value in self.__dict__.items()
            if not str(key).startswith('_')
            and isinstance(value, (str, int, float, bool))
        }
        client = _core.ApiConnector(self.label, attributes)
        self.__dict__['_rust_client_obj'] = client
        self.__dict__['_rust_client_pid'] = os.getpid()
        return client

    def __getstate__(self):
        state = self.__dict__.copy()
        for key in ('_rust_client_obj', '_rust_client_pid', '_rust_client_attributes', '_session'):
            state.pop(key, None)
        state['_rust_client_obj'] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    @property
    def URI(self) -> str:
        """
        Return the fully qualified URI.
        """
        return self._rust_client().uri

    @property
    def session(self):
        """
        A `requests.Session` (not used by the connector, which makes its requests in Rust).
        It exists for plugins which still reach for it and needs `requests` installed.
        """
        if self.__dict__.get('_session', None) is None:
            from meerschaum.utils.packages import attempt_import
            requests = attempt_import('requests', lazy=False)
            if requests is None:
                raise ImportError("Failed to import requests. Is requests installed?")
            self._session = requests.Session()
        return self._session

    @property
    def token(self):
        """
        Return the API key, or the session token (logging in when it is missing or expiring).
        """
        return self._rust_client().token

    @property
    def instance_keys(self) -> Union[str, None]:
        """
        Return the instance keys to be sent alongside pipe requests.
        """
        return self._instance_keys

    @property
    def login_scheme(self) -> str:
        """
        Return the login scheme to use based on the configured credentials.
        """
        if 'username' in self.__dict__:
            return 'password'
        if 'client_id' in self.__dict__:
            return 'client_credentials'
        elif 'api_key' in self.__dict__:
            return 'api_key'

        return 'password'
