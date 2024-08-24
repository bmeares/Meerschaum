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
from meerschaum.utils.typing import List, Dict, Any, SuccessTuple, Iterator, Optional, Union
from meerschaum.utils.warnings import warn, dprint


@make_connector
class ValkeyConnector(Connector):
    """
    Manage a Valkey instance.

    Build a `ValkeyConnector` from connection attributes or a URI string.
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
            uri += f'{self.port}'

        if 'db' in self.__dict__:
            uri += f"/{self.db}"

        if 'socket_timeout' in self.__dict__:
            uri += f"?timeout={self.socket_timeout}s"

        return uri

    def set(self, key: str, value: Any) -> None:
        """
        Set the `key` to `value`.
        """
        return self.client.set(key, value)

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
        docs_str = df.to_json(
            date_format='iso',
            orient='records',
            date_unit='us',
        )
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
        counter_key = self.get_counter_key(table)
        datetime_column_key = self.get_datetime_column_key(table)
        remote_datetime_column = self.get(datetime_column_key)
        datetime_column = datetime_column or remote_datetime_column

        if not datetime_column:
            return self._push_hash_docs_to_list(docs, table)

        dateutil_parser = mrsm.attempt_import('dateutil.parser')

        old_len = self.client.zcard(table_name)
        for doc in docs:
            dt_str = str(doc[datetime_column]) if datetime_column in doc else '1970-01-01'
            dt_val = dateutil_parser.parse(dt_str)
            ts = int(dt_val.replace(tzinfo=timezone.utc).timestamp())
            if debug:
                dprint(f"Adding doc with {dt_val=}, {ts=}")
            doc_str = json.dumps(
                doc,
                default=json_serialize_datetime,
                separators=(',', ':'),
            )
            self.client.zadd(table_name, {doc_str: ts})

        self.set(datetime_column_key, datetime_column)
        new_len = self.client.zcard(table_name)

        return new_len - old_len

    def _push_hash_docs_to_list(self, docs: List[Dict[str, Any]], table: str) -> int:
        table_name = self.quote_table(table)
        counter_key = self.get_counter_key(table)
        next_ix = int((self.client.get(counter_key) or b'0').decode('utf-8'))
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

        self.client.incrby(counter_key, len(docs))
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
        begin: Optional[datetime] = None,
        end: Optional[datetime] = None,
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

        begin: Optional[datetime], default None
            If provided, only return rows greater than or equal to this datetime.

        end: Optional[datetime], default None
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
            begin=begin,
            end=end,
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
        begin: Optional[datetime] = None,
        end: Optional[datetime] = None,
        debug: bool = False,
    ) -> Iterator[Dict[str, str]]:
        """
        Return a list of previously pushed docs.

        Parameters
        ----------
        table: str
            The "table" name (root key) under which the docs were pushed.

        begin: Optional[datetime], default None
            If provided and the table was created with a datetime index, only return documents
            newer than this datetime.

        end: Optional[datetime], default None
            If provided and the table was created with a datetime index, only return documents
            older than this datetime.

        Returns
        -------
        A list of dictionaries, where all keys and values are strings.
        """
        table_name = self.quote_table(table)
        datetime_column_key = self.get_datetime_column_key(table)
        datetime_column = self.get(datetime_column_key)

        if not datetime_column:
            return self._read_docs_from_list(table)

        dateutil_parser = mrsm.attempt_import('dateutil.parser')

        if isinstance(begin, str):
            begin = dateutil_parser.parse(begin)

        if isinstance(end, str):
            end = dateutil_parser.parse(end)

        begin_ts = int(begin.replace(tzinfo=timezone.utc).timestamp()) if begin else '-inf'
        end_ts = (int(end.replace(tzinfo=timezone.utc).timestamp()) - 1) if end else '+inf'

        if debug:
            dprint(f"Reading documents with {begin_ts=}, {end_ts=}")

        return (
            json.loads(doc_bytes.decode('utf-8'))
            for doc_bytes in self.client.zrangebyscore(
                table_name,
                begin_ts,
                end_ts,
                withscores=False,
            )
        )

    def _read_docs_from_list(self, table):
        table_name = self.quote_table(table)
        doc_keys = self.client.lrange(table_name, 0, -1)
        for doc_key in doc_keys:
            yield {
                key.decode('utf-8'): value.decode('utf-8')
                for key, value in self.client.hgetall(doc_key).items()
            }

    def drop_table(self, table: str) -> None:
        """
        Drop a "table" of documents.

        Parameters
        ----------
        table: str
            The "table" name (root key) to be deleted.
        """
        table_name = self.quote_table(table)

        datetime_column_key = self.get_datetime_column_key(table)
        datetime_column = self.get(datetime_column_key)

        if not datetime_column:
            doc_keys = self.client.lrange(table_name, 0, -1)
            for doc_key in doc_keys:
                self.client.delete(doc_key)
                self.client.lrem(table_name, 0, doc_key)

        counter_key = self.get_counter_key(table)
        self.client.delete(counter_key)
        self.client.delete(table_name)
        self.client.delete(datetime_column_key)
