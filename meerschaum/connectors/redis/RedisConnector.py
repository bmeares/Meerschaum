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

    Build a `RedisConnector` from connection attributes or a URI string.
    """
    REQUIRED_ATTRIBUTES: List[str] = ['host']
    OPTIONAL_ATTRIBUTES: List[str] = [
        'port', 'username', 'password', 'database', 'socket_timeout',
    ]
    DEFAULT_ATTRIBUTES: Dict[str, Any] = {
        'username': 'default',
        'port': 6379,
        'socket_timeout': 2,
    }

    @property
    def client(self):
        """
        Return the Redis client.
        """
        if '_client' in self.__dict__:
            return self.__dict__['_client']

        redis = mrsm.attempt_import('redis')

        if 'uri' in self.__dict__:
            self._client = redis.Redis.from_url(self.__dict__.get('uri'))
            return self._client

        optional_kwargs = {
            key: self.__dict__.get(key)
            for key in self.OPTIONAL_ATTRIBUTES
            if key in self.__dict__
        }
        connection_kwargs = {
            'host': self.host,
            **optional_kwargs
        }

        self._client = redis.Redis(**connection_kwargs)
        return self._client

    @property
    def URI(self) -> str:
        """
        Return the connection URI for this connector.
        """
        import urllib.parse

        if 'uri' in self.__dict__:
            return self.__dict__.get('uri')

        uri = "redis://"
        if 'username' in self.__dict__:
            uri += urllib.parse.quote_plus(self.username) + ':'

        if 'password' in self.__dict__:
            uri += urllib.parse.quote_plus(self.password) + '@'

        if 'host' in self.__dict__:
            uri += self.host

        if 'port' in self.__dict__:
            uri += f'{self.port}'

        if 'database' in self.__dict__:
            uri += "/" + urllib.parse.quote_plus(self.database)

        if 'socket_timeout' in self.__dict__:
            uri += f"?timeout={self.socket_timeout}s"

        return uri

    def set(self, key: str, value: Any) -> None:
        """
        Set the `key` to `value`.
        """
        return self.client.set(key, value)

    def get(self, key: str) -> Any:
        """
        Get the value for `key`.
        """
        return self.client.get(key)

    def test_connection(self) -> bool:
        """
        Return whether a connection may be established.
        """
        return self.client.ping()
