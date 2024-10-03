#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the `ValkeyConnector`.
"""

import shlex
import json
from datetime import datetime, timezone

import meerschaum as mrsm
from meerschaum.connectors import Connector, make_connector
from meerschaum.utils.typing import List, Dict, Any, Optional, Union
from meerschaum.utils.warnings import dprint


@make_connector
class ValkeyConnector(Connector):
    """
    Manage a Valkey instance.

    Build a `ValkeyConnector` from connection attributes or a URI string.
    """
    IS_INSTANCE: bool = True
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
        get_plugins,
        delete_plugin,
    )

    @property
    def client(self):
        """
        Return the Valkey client.
        """
        if '_client' in self.__dict__:
            return self.__dict__['_client']

        valkey = mrsm.attempt_import('valkey')

        if 'uri' in self.__dict__:
            self._client = valkey.Valkey.from_url(self.__dict__.get('uri'))
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
        import urllib.parse

        if 'uri' in self.__dict__:
            return self.__dict__.get('uri')

        uri = "valkey://"
        if 'username' in self.__dict__:
            uri += urllib.parse.quote_plus(self.username) + ':'

        if 'password' in self.__dict__:
            uri += urllib.parse.quote_plus(self.password) + '@'

        if 'host' in self.__dict__:
            uri += self.host

        if 'port' in self.__dict__:
            uri += f':{self.port}'

        if 'db' in self.__dict__:
            uri += f"/{self.db}"

        if 'socket_timeout' in self.__dict__:
            uri += f"?timeout={self.socket_timeout}s"

        return uri

    def set(self, key: str, value: Any, **kwargs: Any) -> None:
        """
        Set the `key` to `value`.
        """
        return self.client.set(key, value, **kwargs)

    def get(self, key: str) -> Union[str, None]:
        """
        Get the value for `key`.
        """
        val = self.client.get(key)
        if val is None:
            return None

        return val.decode('utf-8')

    def test_connection(self) -> bool:
        """
        Return whether a connection may be established.
        """
        return self.client.ping()

    @classmethod
    def quote_table(cls, table: str) -> str:
        """
        Return a quoted key.
        """
        return shlex.quote(table)

    @classmethod
    def get_counter_key(cls, table: str) -> str:
        """
        Return the counter key for a given table.
        """
        table_name = cls.quote_table(table)
        return f"{table_name}:counter"

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
        from meerschaum.utils.dataframe import to_json
        docs_str = to_json(df)
        docs = json.loads(docs_str)
        return self.push_docs(
            docs,
            table,
            datetime_column=datetime_column,
            debug=debug,
        )

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
            Otherwise push the docs to a list.

        Returns
        -------
        The current index counter value (how many docs have been pushed).
        """
        from meerschaum.utils.misc import json_serialize_datetime
        table_name = self.quote_table(table)
        datetime_column_key = self.get_datetime_column_key(table)
        remote_datetime_column = self.get(datetime_column_key)
        datetime_column = datetime_column or remote_datetime_column
        dateutil_parser = mrsm.attempt_import('dateutil.parser')

        old_len = (
            self.client.zcard(table_name)
            if datetime_column
            else self.client.scard(table_name)
        )
        for doc in docs:
            original_dt_val = (
                doc[datetime_column]
                if datetime_column and datetime_column in doc
                else 0
            )
            dt_val = (
                dateutil_parser.parse(str(original_dt_val))
                if not isinstance(original_dt_val, int)
                else int(original_dt_val)
            ) if datetime_column else None
            ts = (
                int(dt_val.replace(tzinfo=timezone.utc).timestamp())
                if isinstance(dt_val, datetime)
                else int(dt_val)
            ) if datetime_column else None
            doc_str = json.dumps(
                doc,
                default=(lambda x: json_serialize_datetime(x) if hasattr(x, 'tzinfo') else str(x)),
                separators=(',', ':'),
                sort_keys=True,
            )
            if datetime_column:
                self.client.zadd(table_name, {doc_str: ts})
            else:
                self.client.sadd(table_name, doc_str)

        if datetime_column:
            self.set(datetime_column_key, datetime_column)
        new_len = (
            self.client.zcard(table_name)
            if datetime_column
            else self.client.scard(table_name)
        )

        return new_len - old_len

    def _push_hash_docs_to_list(self, docs: List[Dict[str, Any]], table: str) -> int:
        table_name = self.quote_table(table)
        next_ix = max(self.client.llen(table_name) or 0, 1)
        for i, doc in enumerate(docs):
            doc_key = f"{table_name}:{next_ix + i}"
            self.client.hset(
                doc_key,
                mapping={
                    str(k): str(v)
                    for k, v in doc.items()
                },
            )
            self.client.rpush(table_name, doc_key)

        return next_ix + len(docs)

    def get_datetime_column_key(self, table: str) -> str:
        """
        Return the key to store the datetime index for `table`.
        """
        table_name = self.quote_table(table)
        return f'{table_name}:datetime_column'

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
        from meerschaum.utils.dataframe import parse_df_datetimes, query_df
        docs = self.read_docs(
            table,
            begin=begin,
            end=end,
            debug=debug,
        )
        df = parse_df_datetimes(docs)
        datetime_column_key = self.get_datetime_column_key(table)
        datetime_column = datetime_column or self.get(datetime_column_key)

        return query_df(
            df,
            begin=(begin if datetime_column is not None else None),
            end=(end if datetime_column is not None else None),
            params=params,
            datetime_column=datetime_column,
            select_columns=select_columns,
            omit_columns=omit_columns,
            inplace=True,
            reset_index=True,
            debug=debug,
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
            The "table" name (root key) under which the docs were pushed.

        begin: Union[datetime, int, str, None], default None
            If provided and the table was created with a datetime index, only return documents
            newer than this datetime.
            If the table was not created with a datetime index and `begin` is an `int`,
            return documents with a positional index greater than or equal to this value.

        end: Union[datetime, int, str, None], default None
            If provided and the table was created with a datetime index, only return documents
            older than this datetime.
            If the table was not created with a datetime index and `begin` is an `int`,
            return documents with a positional index less than this value.

        Returns
        -------
        A list of dictionaries, where all keys and values are strings.
        """
        table_name = self.quote_table(table)
        datetime_column_key = self.get_datetime_column_key(table)
        datetime_column = self.get(datetime_column_key)

        if debug:
            dprint(f"Reading documents from '{table}' with {begin=}, {end=}")

        if not datetime_column:
            return [
                json.loads(doc_bytes.decode('utf-8'))
                for doc_bytes in self.client.smembers(table_name)
            ]

        dateutil_parser = mrsm.attempt_import('dateutil.parser')

        if isinstance(begin, str):
            begin = dateutil_parser.parse(begin)

        if isinstance(end, str):
            end = dateutil_parser.parse(end)

        begin_ts = (
            (
                int(begin.replace(tzinfo=timezone.utc).timestamp())
                if isinstance(begin, datetime)
                else int(begin)
            )
            if begin is not None else '-inf'
        )
        end_ts = (
            (
                int(end.replace(tzinfo=timezone.utc).timestamp())
                if isinstance(end, datetime)
                else int(end)
            )
            if end is not None else '+inf'
        )

        if debug:
            dprint(f"Reading documents with {begin_ts=}, {end_ts=}")

        return [
            json.loads(doc_bytes.decode('utf-8'))
            for doc_bytes in self.client.zrangebyscore(
                table_name,
                begin_ts,
                end_ts,
                withscores=False,
            )
        ]

    def _read_docs_from_list(
        self,
        table: str,
        begin_ix: Optional[int] = 0,
        end_ix: Optional[int] = -1,
        debug: bool = False,
    ):
        """
        Read a list of documents from a "table".

        Parameters
        ----------
        table: str
            The "table" (root key) from which to read docs.

        begin_ix: Optional[int], default 0
            If provided, only read documents from this starting index.

        end_ix: Optional[int], default -1
            If provided, only read documents up to (not including) this index.

        Returns
        -------
        A list of documents.
        """
        if begin_ix is None:
            begin_ix = 0

        if end_ix == 0:
            return

        if end_ix is None:
            end_ix = -1
        else:
            end_ix -= 1

        table_name = self.quote_table(table)
        doc_keys = self.client.lrange(table_name, begin_ix, end_ix)
        for doc_key in doc_keys:
            yield {
                key.decode('utf-8'): value.decode('utf-8')
                for key, value in self.client.hgetall(doc_key).items()
            }

    def drop_table(self, table: str, debug: bool = False) -> None:
        """
        Drop a "table" of documents.

        Parameters
        ----------
        table: str
            The "table" name (root key) to be deleted.
        """
        table_name = self.quote_table(table)
        datetime_column_key = self.get_datetime_column_key(table)
        self.client.delete(table_name)
        self.client.delete(datetime_column_key)

    @classmethod
    def get_entity_key(cls, *keys: Any) -> str:
        """
        Return a joined key to set an entity.
        """
        if not keys:
            raise ValueError("No keys to be joined.")

        for key in keys:
            if cls.KEY_SEPARATOR in str(key):
                raise ValueError(f"Key cannot contain separator '{cls.KEY_SEPARATOR}'.")

        return cls.KEY_SEPARATOR.join([str(key) for key in keys])
