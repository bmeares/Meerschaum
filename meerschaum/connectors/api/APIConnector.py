#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with Meerschaum APIs. May be chained together (see 'meerschaum:api_instance' in your config.yaml).
"""

from meerschaum.connectors import Connector

required_attributes = {
    'host',
}

class APIConnector(Connector):

    from ._delete import delete
    from ._post import post
    from ._patch import patch
    from ._get import get, wget
    from ._actions import get_actions, do_action
    from ._misc import get_mrsm_version, get_chaining_status
    from ._pipes import (
        register_pipe,
        fetch_pipes_keys,
        edit_pipe,
        sync_pipe,
        delete_pipe,
        get_pipe_data,
        get_backtrack_data,
        get_pipe_id,
        get_pipe_attributes,
        get_sync_time,
        pipe_exists,
        create_metadata,
        get_pipe_rowcount,
        drop_pipe,
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
    from ._login import login, refresh
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

    def __init__(
        self,
        label : str = 'main',
        wait : bool = False,
        debug : bool = False,
        **kw
    ):
        self.wait = wait
        super().__init__('api', label=label, **kw)
        if 'protocol' not in self.__dict__:
            self.protocol = 'http'
        if 'port' not in self.__dict__:
            self.port = 8000
        self.verify_attributes(required_attributes)
        self.url = (
            self.protocol + '://' +
            self.host + ':' +
            str(self.port)
        )
        self._token = None
        self._expires = None
        self._session = None

    @property
    def session(self):
        if self._session is None:
            from meerschaum.utils.packages import attempt_import
            from meerschaum.utils.warnings import error
            requests = attempt_import('requests')
            if requests:
                self._session = requests.Session()
            if self._session is None:
                error(f"Failed to import requests. Is requests installed?")
        return self._session

    @property
    def token(self):
        import datetime

        expired = (
            True if self._expires is None else (
                (self._expires < datetime.datetime.utcnow() + datetime.timedelta(minutes=1))
            )
        )

        def _print_tup(_tup):
            from meerschaum.utils.formatting import print_tuple
            print_tuple(tup)

        if self._token is None or expired:
            tup = self.login()
            if not tup[0]:
                _print_tup(tup)
        elif expired:
            tup = self.refresh()
            if not tup[0]:
                _print_tup(tup)
        return self._token

