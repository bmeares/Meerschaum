#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define pipes methods for `ValkeyConnector` (the Rust client, `meerschaum._core.ValkeyConnector`).
"""

import json
from datetime import datetime

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Any, Union, Optional, Dict, List, Tuple

PIPES_TABLE: str = 'mrsm_pipes'
PIPES_COUNTER: str = 'mrsm_pipes:counter'


def get_pipe_key(pipe: mrsm.Pipe) -> str:
    """
    Return the key to store a pipe's ID.
    """
    return f"mrsm_pipe:{pipe.connector_keys}:{pipe.metric_key}:{pipe.location_key}"


def get_pipe_parameters_key(pipe: mrsm.Pipe) -> str:
    """
    Return the key to store a pipe's parameters.
    """
    return get_pipe_key(pipe) + ':parameters'


def serialize_document(doc: Dict[str, Any]) -> str:
    """
    Return a serialized string for a document.

    Parameters
    ----------
    doc: Dict[str, Any]
        The document to be serialized.

    Returns
    -------
    A serialized string for the document.
    """
    from meerschaum.utils.dtypes import json_serialize_value
    return json.dumps(
        doc,
        default=json_serialize_value,
        separators=(',', ':'),
        sort_keys=True,
    )


@staticmethod
def get_document_key(
    doc: Dict[str, Any],
    indices: List[str],
    table_name: Optional[str] = None,
) -> str:
    """
    Return a serialized string for a document's indices only.

    Parameters
    ----------
    doc: Dict[str, Any]
        The document containing index values to be serialized.

    indices: List[str]
        The name of the indices to be serialized.

    table_name: Optional[str], default None
        If provided, prepend the table to the key.

    Returns
    -------
    A serialized string of the document's indices.
    """
    from meerschaum import _core
    return _core.ValkeyConnector.get_document_key(doc, indices, table_name)


@classmethod
def get_table_quoted_doc_key(
    cls,
    table_name: str,
    doc: Dict[str, Any],
    indices: List[str],
    datetime_column: Optional[str] = None,
) -> str:
    """
    Return the document string as stored in the underling set.
    """
    from meerschaum.utils.dtypes import json_serialize_value
    return json.dumps(
        {
            cls.get_document_key(doc, indices, table_name): serialize_document(doc),
            **(
                {datetime_column: doc.get(datetime_column, 0)}
                if datetime_column
                else {}
            )
        },
        sort_keys=True,
        separators=(',', ':'),
        default=json_serialize_value,
    )


def register_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Register a pipe's parameters and assign it an ID.
    """
    return self._rust_client().register_pipe(pipe)


def get_pipe_id(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> Union[str, int, None]:
    """
    Return the pipe's ID.
    """
    return self._rust_client().get_pipe_id(pipe)


def get_pipe_attributes(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> Dict[str, Any]:
    """
    Return the pipe's document from the pipes table.
    """
    return self._rust_client().get_pipe_attributes(pipe)


def edit_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Edit the attributes of the pipe.
    """
    return self._rust_client().edit_pipe(pipe)


def pipe_exists(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> bool:
    """
    Check whether a pipe's target table exists.
    """
    return self._rust_client().pipe_exists(pipe)


def drop_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Drop a pipe's collection but maintain its registration.
    """
    return self._rust_client().drop_pipe(pipe)


def delete_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Delete a pipe's registration from the `mrsm_pipes` collection.
    """
    return self._rust_client().delete_pipe(pipe)


def get_pipe_data(
    self,
    pipe: mrsm.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    as_polars: Optional[bool] = None,
    as_pandas: Optional[bool] = None,
    **kwargs: Any
) -> Union['pd.DataFrame', None]:
    """
    Query a pipe's target table and return the DataFrame (of the `meerschaum:dataframe` library;
    `as_polars=True` / `as_pandas=True` choose one).

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe with the target table from which to read.

    select_columns: Optional[List[str]], default None
        If provided, only select these given columns.

    omit_columns: Optional[List[str]], default None
        If provided, remove these columns from the selection.

    begin: Union[datetime, int, None], default None
        The earliest `datetime` value to search from (inclusive).

    end: Union[datetime, int, None], default None
        The lastest `datetime` value to search from (inclusive: `ZRANGEBYSCORE` is).

    params: Optional[Dict[str, Any]], default None
        Additional filters to apply to the query.

    Returns
    -------
    The target table's data as a DataFrame, or `None` if the table does not exist.
    """
    from meerschaum.utils.dataframe import frame_from_arrow, frame_library_of
    df = self._rust_client().get_pipe_data(
        pipe,
        select_columns=select_columns,
        omit_columns=omit_columns,
        begin=begin,
        end=end,
        params=params,
    )
    if frame_library_of(df) is None:
        return df
    return frame_from_arrow(df, as_polars=as_polars, as_pandas=as_pandas)


def sync_pipe(
    self,
    pipe: mrsm.Pipe,
    df: 'pd.DataFrame' = None,
    check_existing: bool = True,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Add new documents to the pipe's target table.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table to receive the new documents.

    df: 'pd.DataFrame', default None
        The data to be synced.

    check_existing: bool, default True
        If `False`, do not check the documents against existing data and instead insert directly.

    kwargs: Any
        `replace=True` clears the pipe's documents of `[begin, end)` (matching `params`), then
        writes the rows of `df` (a bound that is not given is the rows' own datetime range).

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    replace_kw = (
        {
            'replace': True,
            'begin': kwargs.get('begin', None),
            'end': kwargs.get('end', None),
            'params': kwargs.get('params', None),
        }
        if kwargs.get('replace', False)
        else {}
    )
    return self._rust_client().sync_pipe(pipe, df, check_existing=check_existing, **replace_kw)


def get_pipe_columns_types(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> Dict[str, str]:
    """
    Return the data types for the columns in the target table for data type enforcement.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table contains columns and data types.

    Returns
    -------
    A dictionary mapping columns to data types.
    """
    return self._rust_client().get_pipe_columns_types(pipe)


def clear_pipe(
    self,
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Delete rows within `begin`, `end`, and `params`.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose rows to clear.

    begin: Union[datetime, int, None], default None
        If provided, remove rows >= `begin`.

    end: Union[datetime, int, None], default None
        If provided, remove rows < `end`.

    params: Optional[Dict[str, Any]], default None
        If provided, only remove rows which match the `params` filter.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    return self._rust_client().clear_pipe(pipe, begin=begin, end=end, params=params)


def get_sync_time(
    self,
    pipe: mrsm.Pipe,
    newest: bool = True,
    **kwargs: Any
) -> Union[datetime, int, None]:
    """
    Return the most recent datetime value in the pipe's target table
    (`params` is not supported by Valkey instances).

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table contains the datetime column.

    newest: bool, default True
        If `True`, get the newest value, otherwise the oldest.

    Returns
    -------
    A `datetime` or `int`, if the pipe exists, otherwise `None`.
    """
    return self._rust_client().get_sync_time(pipe, newest)


def get_pipe_rowcount(
    self,
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kwargs: Any
) -> Union[int, None]:
    """
    Return the number of documents in the pipe's set.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose target table to count.

    begin: Union[datetime, int, None], default None
        If provided, only count rows >= `begin`.

    end: Union[datetime, int, None], default None
        If provided, only count rows < `end`.

    params: Optional[Dict[str, Any]], default None
        If provided, only count rows which match the `params` filter.

    Returns
    -------
    The number of matching documents, or `None` if the pipe does not exist.
    """
    return self._rust_client().get_pipe_rowcount(pipe, begin=begin, end=end, params=params)


def fetch_pipes_keys(
    self,
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False
) -> List[Tuple[str, str, Union[str, None], Dict[str, Any]]]:
    """
    Return the keys (and parameters) of the registered pipes which match the filters.

    Parameters
    ----------
    connector_keys: Optional[List[str]], default None
        The connector keys (`_` negates).

    metric_keys: Optional[List[str]], default None
        The metric keys.

    location_keys: Optional[List[str]], default None
        The location keys (`None` matches a pipe without a location).

    tags: Optional[List[str]], default None
        The tags (comma-separated groups are ANDed, groups are ORed).

    params: Optional[Dict[str, Any]], default None
        Additional filters against the pipes' parameters.

    Returns
    -------
    A list of tuples of `(connector_keys, metric_key, location_key, parameters)`.
    """
    return self._rust_client().fetch_pipes_keys(
        connector_keys,
        metric_keys,
        location_keys,
        tags,
        params,
    )
