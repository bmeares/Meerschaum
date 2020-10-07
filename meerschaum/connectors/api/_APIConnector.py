#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with the Meerschaum WebAPI with the APIConnector
"""

from meerschaum.connectors._Connector import Connector

class APIConnector(Connector):

    from ._post import post
    from ._patch import patch
    from ._get import get
    from ._actions import get_actions, do_action
    from ._pipes import register_pipe, fetch_pipes_keys, edit_pipe

    def __init__(
        self,
        label : str = 'main',
        debug : bool = False,
        **kw
    ):
        import requests.auth
        super().__init__('api', label=label, **kw)
        self.url = (
            self.protocol + '://' +
            self.host + ':' +
            str(self.port)
        )
        self.auth = requests.auth.HTTPBasicAuth(
            self.username,
            self.password
        )

