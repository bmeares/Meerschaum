#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with the Meerschaum WebAPI with the APIConnector
"""

from meerschaum.connectors import Connector

class APIConnector(Connector):

    from ._post import post
    from ._get import get
    from ._actions import get_actions, do_action

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

