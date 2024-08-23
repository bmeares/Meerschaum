#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the `RedisConnector`.
"""

import meerschaum as mrsm
from meerschaum.connectors import Connector, make_connector
from meerschaum.utils.typing import List, Dict, Any, SuccessTuple


@make_connector
class RedisConnector(Connector):
    """
    Manage a Redis instance.
    """
    REQUIRED_ATTRIBUTES: List[str] = ['host', 'port',]
    OPTIONAL_ATTRIBUTES: List[str] = [
        'username', 'password',
        'ssl', 'ssl_certfile', 'ssl_keyfile', 'ssl_ca_certs',
    ]
    DEFAULT_ATTRIBUTES: Dict[str, Any] = {'username': 'default'}

    @property
    def client(self):
        """
        Return the Redis client.
        """
        if '_client' in self.__dict__:
            return self.__dict__['_client']

        redis = mrsm.attempt_import('redis')
        optional_kwargs = {
            key: self.__dict__.get(key)
            for key in self.OPTIONAL_ATTRIBUTES
            if key in self.__dict__
        }
        connection_kwargs = {
            'host': self.host,
            'port': self.port,
            **optional_kwargs
        }

        self._client = redis.Redis(**connection_kwargs)
        return self._client
