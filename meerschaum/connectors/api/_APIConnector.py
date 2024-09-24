#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with Meerschaum APIs. May be chained together (see 'meerschaum:api_instance' in your config.yaml).
"""

from datetime import datetime, timedelta, timezone
from meerschaum.utils.typing import Optional, List
from meerschaum.connectors import Connector
from meerschaum.utils.warnings import warn, error
from meerschaum.utils.packages import attempt_import

required_attributes = {
    'host',
}

class APIConnector(Connector):
    """
    Connect to a Meerschaum API instance.
    """

    IS_INSTANCE: bool = True
    IS_THREAD_SAFE: bool = False

    OPTIONAL_ATTRIBUTES: List[str] = ['port']

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
        register_pipe,
        fetch_pipes_keys,
        edit_pipe,
        sync_pipe,
        delete_pipe,
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
    )
    from ._fetch import fetch
    from ._plugins import (
        register_plugin,
        install_plugin,
        delete_plugin,
        get_plugins,
        get_plugin_attributes,
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
            from meerschaum.connectors.sql import SQLConnector
            conn_attrs = SQLConnector.parse_uri(self.__dict__['uri'])
            if 'host' not in conn_attrs:
                raise Exception(f"Invalid URI for '{self}'.")
            self.__dict__.update(conn_attrs)

        self.url = (
            self.protocol + '://' +
            self.host
            + (
                (':' + str(self.port))
                if self.__dict__.get('port', None)
                else ''
            )
        )
        self._token = None
        self._expires = None
        self._session = None


    @property
    def URI(self) -> str:
        """
        Return the fully qualified URI.
        """
        username = self.__dict__.get('username', None)
        password = self.__dict__.get('password', None)
        creds = (username + ':' + password + '@') if username and password else ''
        return (
            self.protocol
            + '://'
            + creds
            + self.host
            + (
                (':' + str(self.port))
                if self.__dict__.get('port', None)
                else ''
            )
        )


    @property
    def session(self):
        if self._session is None:
            certifi = attempt_import('certifi', lazy=False)
            requests = attempt_import('requests', lazy=False)
            if requests:
                self._session = requests.Session()
            if self._session is None:
                error(f"Failed to import requests. Is requests installed?")
        return self._session

    @property
    def token(self):
        expired = (
            True if self._expires is None else (
                (
                    self._expires
                    <
                    datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=1)
                )
            )
        )

        if self._token is None or expired:
            success, msg = self.login()
            if not success:
                warn(msg, stack=False)
        return self._token
