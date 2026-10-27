#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the `ValkeyConnector`.
"""

from datetime import datetime

import meerschaum as mrsm
from meerschaum.connectors import InstanceConnector, make_connector
from meerschaum.utils.typing import List, Dict, Any, Optional, Union


@make_connector
class ValkeyConnector(InstanceConnector):
    """
    Manage a Valkey instance.

    Build a `ValkeyConnector` from connection attributes or a URI string.
    The commands are run by the Rust client (`meerschaum._core.ValkeyConnector`, one per
    connector and process, connected by the first command).
    """
    REQUIRED_ATTRIBUTES: List[str] = ['host']
    OPTIONAL_ATTRIBUTES: List[str] = [
        'port', 'username', 'password', 'db', 'socket_timeout',
    ]
    DEFAULT_ATTRIBUTES: Dict[str, Any] = {
        'username': 'default',
        'port': 6379,
        'db': 0,
        'socket_timeout': 300,
    }
    KEY_SEPARATOR: str = ':'

    from ._pipes import (
        register_pipe,
        get_pipe_id,
        get_pipe_attributes,
        edit_pipe,
        pipe_exists,
        drop_pipe,
        delete_pipe,
        get_pipe_data,
        sync_pipe,
        get_pipe_columns_types,
        clear_pipe,
        get_sync_time,
        get_pipe_rowcount,
        fetch_pipes_keys,
        get_document_key,
        get_table_quoted_doc_key,
    )
    from ._fetch import (
        fetch,
    )

    from ._users import (
        get_users_pipe,
        get_user_key,
        get_user_keys_vals,
        register_user,
        get_user_id,
        edit_user,
        get_user_attributes,
        delete_user,
        get_users,
        get_user_password_hash,
        get_user_type,
    )
    from ._plugins import (
        get_plugins_pipe,
        get_plugin_key,
        get_plugin_keys_vals,
        register_plugin,
        get_plugin_id,
        get_plugin_version,
        get_plugin_user_id,
        get_plugin_username,
        get_plugin_attributes,
        delete_plugin,
    )

    def _rust_client(self):
        """
        Return the `meerschaum._core.ValkeyConnector` of this connector (built once per process:
        a forked child builds its own connection). It connects on the first command.
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
        client = _core.ValkeyConnector(self.label, attributes)
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
        client = _core.ValkeyConnector(self.label, attributes)
        self.__dict__['_rust_client_obj'] = client
        self.__dict__['_rust_client_pid'] = os.getpid()
        return client

    def __getstate__(self):
        state = self.__dict__.copy()
        for key in ('_rust_client_obj', '_rust_client_pid', '_rust_client_attributes', '_client'):
            state.pop(key, None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    @property
    def client(self):
        """
        Return a client for raw commands (`get`, `set`, `keys`, `delete`, ...).

        When the `valkey` package is installed (Meerschaum does not depend on it and never
        installs it) this is a `valkey.Valkey` client; otherwise it is a small built-in client
        over the Rust connector which supports the commands of `ValkeyClient`.
        """
        if '_client' in self.__dict__:
            return self.__dict__['_client']

        valkey = mrsm.attempt_import('valkey', install=False, warn=False, lazy=False)
        if valkey is None:
            from ._client import ValkeyClient
            self._client = ValkeyClient(self)
            return self._client

        if 'uri' in self.__dict__:
            ### `?protected=true` is Meerschaum's (`Connector.protected`), not the client's.
            from meerschaum.connectors.sql._SQLConnector import _strip_uri_param
            self._client = valkey.Valkey.from_url(
                _strip_uri_param(self.__dict__.get('uri'), 'protected')
            )
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

        self._client = valkey.Valkey(**connection_kwargs)
        return self._client

    @property
    def URI(self) -> str:
        """
        Return the connection URI for this connector.
        """
        return self._rust_client().uri

    def set(self, key: str, value: Any, **kwargs: Any) -> bool:
        """
        Set the `key` to `value`.
        `ex` sets an expiration in seconds; other keyword arguments (`px`, `nx`) go to `client`.
        """
        if set(kwargs) <= {'ex'}:
            return self._rust_client().set(key, value, **kwargs)
        return self.client.set(key, value, **kwargs)

    def get(self, key: str, decode: bool = True) -> Union[str, None]:
        """
        Get the value for `key`.
        """
        return self._rust_client().get(key, decode)

    def test_connection(self) -> bool:
        """
        Return whether a connection may be established.
        """
        return self._rust_client().test_connection()

    @classmethod
    def quote_table(cls, table: str) -> str:
        """
        Return a quoted key.
        """
        from meerschaum import _core
        return _core.ValkeyConnector.quote_table(table)

    @classmethod
    def get_counter_key(cls, table: str) -> str:
        """
        Return the counter key for a given table.
        """
        from meerschaum import _core
        return _core.ValkeyConnector.get_counter_key(table)

    def push_df(
        self,
        df: 'pd.DataFrame',
        table: str,
        datetime_column: Optional[str] = None,
        debug: bool = False,
    ) -> int:
        """
        Append a pandas DataFrame to a table.

        Parameters
        ----------
        df: pd.DataFrame
            The pandas DataFrame to append to the table.

        table: str
            The "table" name (root key).

        datetime_column: Optional[str], default None
            If provided, use this key as the datetime index.

        Returns
        -------
        The current index counter value (how many docs have been pushed).
        """
        return self._rust_client().push_df(df, table, datetime_column)

    def push_docs(
        self,
        docs: List[Dict[str, Any]],
        table: str,
        datetime_column: Optional[str] = None,
        debug: bool = False,
    ) -> int:
        """
        Append a list of documents to a table.

        Parameters
        ----------
        docs: List[Dict[str, Any]]
            The docs to be pushed.
            All keys and values will be coerced into strings.

        table: str
            The "table" name (root key).

        datetime_column: Optional[str], default None
            If set, create a sorted set with this datetime column as the index.
            Otherwise push the docs to a set.

        Returns
        -------
        The current index counter value (how many docs have been pushed).
        """
        return self._rust_client().push_docs(docs, table, datetime_column)

    def get_datetime_column_key(self, table: str) -> str:
        """
        Return the key to store the datetime index for `table`.
        """
        return self._rust_client().get_datetime_column_key(table)

    def read(
        self,
        table: str,
        begin: Union[datetime, int, str, None] = None,
        end: Union[datetime, int, str, None] = None,
        params: Optional[Dict[str, Any]] = None,
        datetime_column: Optional[str] = None,
        select_columns: Optional[List[str]] = None,
        omit_columns: Optional[List[str]] = None,
        debug: bool = False
    ) -> Union['pd.DataFrame', None]:
        """
        Query the table and return the result dataframe.

        Parameters
        ----------
        table: str
            The "table" name to be queried.

        begin: Union[datetime, int, str, None], default None
            If provided, only return rows greater than or equal to this datetime.

        end: Union[datetime, int, str, None], default None
            If provided, only return rows older than this datetime.

        params: Optional[Dict[str, Any]]
            Additional Meerschaum filter parameters.

        datetime_column: Optional[str], default None
            If provided, use this column for the datetime index.
            Otherwise infer from the table metadata.

        select_columns: Optional[List[str]], default None
            If provided, only return these columns.

        omit_columns: Optional[List[str]], default None
            If provided, do not include these columns in the result.

        Returns
        -------
        A Pandas DataFrame of the result, or `None`.
        """
        return self._rust_client().read(
            table,
            begin=begin,
            end=end,
            params=params,
            datetime_column=datetime_column,
            select_columns=select_columns,
            omit_columns=omit_columns,
        )

    def read_docs(
        self,
        table: str,
        begin: Union[datetime, int, str, None] = None,
        end: Union[datetime, int, str, None] = None,
        debug: bool = False,
    ) -> List[Dict[str, str]]:
        """
        Return a list of previously pushed docs.

        Parameters
        ----------
        table: str
            The "table" name (root key) from which to read docs.

        begin: Union[datetime, int, str, None], default None
            If provided and the table was created with a datetime index, only return documents
            newer than this datetime. If the table was not created with a datetime index
            and `begin` is an `int`, return documents with a positional index
            greater than or equal to this value.

        end: Union[datetime, int, str, None], default None
            If provided and the table was created with a datetime index, only return documents
            older than this datetime (inclusive, as `ZRANGEBYSCORE` is).

        Returns
        -------
        A list of dictionaries, one for each document.
        """
        return self._rust_client().read_docs(table, begin=begin, end=end)

    def drop_table(self, table: str, debug: bool = False) -> None:
        """
        Delete a "table" (the root key and its datetime column key).
        """
        return self._rust_client().drop_table(table)

    @classmethod
    def get_entity_key(cls, *keys: Any) -> str:
        """
        Join the keys with the key separator (`:`).
        A key may not contain the separator.
        """
        from meerschaum import _core
        return _core.ValkeyConnector.get_entity_key(*keys)
