#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define pipes methods for `ValkeyConnector`.
"""

import json
import traceback
from datetime import datetime

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Any, Union, Optional, Dict, List, Tuple
from meerschaum.utils.misc import json_serialize_datetime

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
    return json.dumps(
        doc,
        default=json_serialize_datetime,
        separators=(',', ':'),
        sort_keys=True,
    )


def get_table_document_key(table_name: str, doc: Dict[str, Any], indices: List[str]) -> str:
    """
    Return a serialized string for a document's indices only.

    Parameters
    ----------
    doc: Dict[str, Any]
        The document containing index values to be serialized.

    indices: List[str]
        The name of the indices to be serialized.

    Returns
    -------
    A serialized string of the document's indices.
    """
    from meerschaum.utils.dtypes import coerce_timezone
    index_vals = {
        key: (
            str(val)
            if not isinstance(val, datetime)
            else str(int(coerce_timezone(val).timestamp()))
        )
        for key, val in doc.items()
        if key in indices
    }
    indices_str = ','.join(
        sorted(
            [
                f'{key}:{val}'
                for key, val in index_vals.items()
            ]
        )
    )
    return table_name + ':indices:' + indices_str


def get_table_quoted_doc_key(
    table_name: str,
    doc: Dict[str, Any],
    indices: List[str],
    datetime_column: Optional[str] = None,
) -> str:
    """
    Return the document string as stored in the underling set.
    """
    return json.dumps(
        {
            get_table_document_key(table_name, doc, indices): serialize_document(doc),
            **(
                {datetime_column: doc.get(datetime_column, 0)}
                if datetime_column
                else {}
            )
        },
        sort_keys=True,
        separators=(',', ':'),
        default=json_serialize_datetime,
    )


def register_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Insert the pipe's attributes into the internal `pipes` table.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to be registered.

    Returns
    -------
    A `SuccessTuple` of the result.
    """
    attributes = {
        'connector_keys': str(pipe.connector_keys),
        'metric_key': str(pipe.metric_key),
        'location_key': str(pipe.location_key),
    }
    parameters_str = json.dumps(
        pipe._attributes.get('parameters', {}),
        separators=(',', ':'),
    )

    pipe_key = get_pipe_key(pipe)
    parameters_key = get_pipe_parameters_key(pipe)

    try:
        existing_pipe_id = self.get(pipe_key)
        if existing_pipe_id is not None:
            return False, f"{pipe} is already registered."

        pipe_id = self.client.incr(PIPES_COUNTER)
        _ = self.push_docs(
            [{'pipe_id': pipe_id, **attributes}],
            PIPES_TABLE,
            datetime_column='pipe_id',
            debug=debug,
        )
        self.set(pipe_key, pipe_id)
        self.set(parameters_key, parameters_str)

    except Exception as e:
        return False, f"Failed to register {pipe}:\n{e}"

    return True, "Success"


def get_pipe_id(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> Union[str, int, None]:
    """
    Return the `_id` for the pipe if it exists.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose `_id` to fetch.

    Returns
    -------
    The `_id` for the pipe's document or `None`.
    """
    pipe_key = get_pipe_key(pipe)
    try:
        return int(self.get(pipe_key))
    except Exception:
        pass
    return None


def get_pipe_attributes(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> Dict[str, Any]:
    """
    Return the pipe's document from the internal `pipes` collection.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose attributes should be retrieved.

    Returns
    -------
    The document that matches the keys of the pipe.
    """
    pipe_id = pipe.get_id(debug=debug)
    if pipe_id is None:
        return {}

    parameters_key = get_pipe_parameters_key(pipe)
    parameters_str = self.get(parameters_key)

    parameters = json.loads(parameters_str) if parameters_str else {}

    attributes = {
        'connector_keys': pipe.connector_keys,
        'metric_key': pipe.metric_key,
        'location_key': pipe.location_key,
        'parameters': parameters,
        'pipe_id': pipe_id,
    }
    return attributes


def edit_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Edit the attributes of the pipe.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose in-memory parameters must be persisted.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    pipe_id = pipe.get_id(debug=debug)
    if pipe_id is None:
        return False, f"{pipe} is not registered."

    parameters_key = get_pipe_parameters_key(pipe)
    parameters_str = json.dumps(pipe.parameters, separators=(',', ':'))
    self.set(parameters_key, parameters_str)
    return True, "Success"


def pipe_exists(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> bool:
    """
    Check whether a pipe's target table exists.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to check whether its table exists.

    Returns
    -------
    A `bool` indicating the table exists.
    """
    table_name = self.quote_table(pipe.target)
    return self.client.exists(table_name) != 0


def drop_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Drop a pipe's collection if it exists.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to be dropped.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    if not pipe.exists(debug=debug):
        return True, "Success"

    self.drop_table(pipe.target, debug=debug)
    return True, "Success"


def delete_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Delete a pipe's registration from the `pipes` collection.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to be deleted.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    from meerschaum.utils.misc import json_serialize_datetime
    drop_success, drop_message = pipe.drop(debug=debug)
    if not drop_success:
        return drop_success, drop_message

    pipe_id = self.get_pipe_id(pipe, debug=debug)
    if pipe_id is None:
        return False, f"{pipe} is not registered."

    pipe_key = get_pipe_key(pipe)
    parameters_key = get_pipe_parameters_key(pipe)
    self.client.delete(pipe_key)
    self.client.delete(parameters_key)
    df = self.read(PIPES_TABLE, params={'pipe_id': pipe_id})
    docs = df.to_dict(orient='records')
    if docs:
        doc = docs[0]
        doc_str = json.dumps(
            doc,
            default=json_serialize_datetime,
            separators=(',', ':'),
            sort_keys=True,
        )
        self.client.zrem(PIPES_TABLE, doc_str)
    return True, "Success"


def get_pipe_data(
    self,
    pipe: mrsm.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kwargs: Any
) -> Union['pd.DataFrame', None]:
    """
    Query a pipe's target table and return the DataFrame.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe with the target table from which to read.

    select_columns: Optional[List[str]], default None
        If provided, only select these given columns.
        Otherwise select all available columns (i.e. `SELECT *`).

    omit_columns: Optional[List[str]], default None
        If provided, remove these columns from the selection.

    begin: Union[datetime, int, None], default None
        The earliest `datetime` value to search from (inclusive).

    end: Union[datetime, int, None], default None
        The lastest `datetime` value to search from (exclusive).

    params: Optional[Dict[str, str]], default None
        Additional filters to apply to the query.

    Returns
    -------
    The target table's data as a DataFrame.
    """
    if not pipe.exists(debug=debug):
        return None

    from meerschaum.utils.dataframe import query_df, parse_df_datetimes

    dt_col = pipe.columns.get('datetime', None)
    docs = [
        json.loads(list({k: v for k, v in doc.items() if k != dt_col}.values())[0])
        for doc in self.read_docs(
            pipe.target,
            begin=begin,
            end=end,
            debug=debug,
        )
    ]
    df = parse_df_datetimes(docs)
    if len(df) == 0:
        return df
    return query_df(
        df,
        params=params,
        begin=begin,
        end=end,
        datetime_column=dt_col,
        inplace=True,
        reset_index=True,
    )


def sync_pipe(
    self,
    pipe: mrsm.Pipe,
    df: 'pd.DataFrame' = None,
    check_existing: bool = True,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Upsert new documents into the pipe's collection.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose collection should receive the new documents.

    df: Union['pd.DataFrame', Iterator['pd.DataFrame']], default None
        The data to be synced.

    check_existing: bool, default True
        If `False`, do not check the documents against existing data and instead insert directly.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    from meerschaum.utils.dataframe import (
        get_datetime_bound_from_df,
        get_unique_index_values,
    )

    dt_col = pipe.columns.get('datetime', None)
    indices = [col for col in pipe.columns.values() if col]
    table_name = self.quote_table(pipe.target)

    def _serialize_docs(_df):
        return [
            {
                get_table_document_key(table_name, doc, indices): serialize_document(doc),
                **(
                    {dt_col: doc.get(dt_col, 0)}
                    if dt_col
                    else {}
                )
            }
            for doc in _df.to_dict(orient='records')
        ]

    existing_dtypes = pipe.dtypes
    new_dtypes = {
        str(key): 'str'
        for key, val in df.dtypes.items()
        if str(key) not in existing_dtypes
    }
    if new_dtypes:
        pipe.dtypes.update(new_dtypes)
        edit_success, edit_msg = pipe.edit(debug=debug)
        if not edit_success:
            return edit_success, edit_msg

    df = pipe.enforce_dtypes(df, debug=debug)

    unseen_df, update_df, delta_df = (
        pipe.filter_existing(df, include_unchanged_columns=True, debug=debug)
        if check_existing
        else (df, None, df)
    )
    num_insert = len(unseen_df) if unseen_df is not None else 0
    num_update = len(update_df) if update_df is not None else 0
    msg = f"Inserted {num_insert}, updated {num_update} rows."
    if len(delta_df) == 0:
        return True, msg

    unseen_docs = _serialize_docs(unseen_df)

    try:
        self.push_docs(
            unseen_docs,
            pipe.target,
            datetime_column=dt_col,
            debug=debug,
        )
    except Exception as e:
        return False, f"Failed to push docs to '{pipe.target}':\n{e}"

    update_min_dt = get_datetime_bound_from_df(update_df, dt_col, minimum=True)
    update_max_dt = get_datetime_bound_from_df(update_df, dt_col, minimum=False)
    update_params = get_unique_index_values(update_df, [col for col in indices if col != dt_col])

    if update_df is not None and len(update_df) != 0:
        #  old_df = pipe.get_data(
            #  begin=update_min_dt,
            #  end=update_max_dt,
            #  params=update_params,
            #  debug=debug,
        #  )

        clear_success, clear_msg = pipe.clear(
            begin=update_min_dt,
            end=update_max_dt,
            params=update_params,
            debug=debug,
        )
        if not clear_success:
            return clear_success, clear_msg

        #  old_docs = old_df.to_dict(orient='records')
        #  for doc in old_docs:
            #  doc.up

        serialized_update_docs = _serialize_docs(update_df)

        try:
            self.push_docs(
                serialized_update_docs,
                pipe.target,
                datetime_column=dt_col,
                debug=debug,
            )
        except Exception as e:
            return False, f"Failed to push docs to '{pipe.target}':\n{e}"

    return True, msg


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
    if not pipe.exists(debug=debug):
        return {}

    columns_types = {}

    return columns_types


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
    if begin is None and end is None and params is None:
        return self.drop_pipe(pipe, debug=debug)

    dt_col = pipe.columns.get('datetime', None)

    existing_df = pipe.get_data(
        begin=begin,
        end=end,
        params=params,
        debug=debug,
    )
    if existing_df is None or len(existing_df) == 0:
        return True, "Deleted 0 rows."

    docs = existing_df.to_dict(orient='records')
    table_name = self.quote_table(pipe.target)
    indices = [col for col in pipe.columns.values() if col]
    for doc in docs:
        quoted_doc_key = get_table_quoted_doc_key(table_name, doc, indices, dt_col)
        if dt_col:
            self.client.zrem(table_name, quoted_doc_key)
        else:
            self.client.srem(table_name, quoted_doc_key)
    msg = (
        f"Deleted {len(docs)} row"
        + ('s' if len(docs) != 1 else '')
        + '.'
    )
    return True, msg


def get_sync_time(
    self,
    pipe: mrsm.Pipe,
    newest: bool = True,
    **kwargs: Any
) -> Union[datetime, int, None]:
    """
    Return the newest (or oldest) timestamp in a pipe.
    """
    dt_col = pipe.columns.get('datetime', None)
    dt_typ = pipe.dtypes.get(dt_col, 'datetime64[ns]')
    if not dt_col:
        return None

    dateutil_parser = mrsm.attempt_import('dateutil.parser')
    table_name = self.quote_table(pipe.target)
    try:
        vals = (
            self.client.zrevrange(table_name, 0, 0)
            if newest
            else self.client.zrange(table_name, 0, 0)
        )
        if not vals:
            return None
        val = vals[0]
    except Exception:
        return None

    doc = json.loads(val)
    dt_val = doc.get(dt_col, None)
    if dt_val is None:
        return None

    return (
        dateutil_parser.parse(str(dt_val))
        if 'datetime' in dt_typ
        else int(dt_val)
    )


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
    """
    dt_col = pipe.columns.get('datetime', None)
    table_name = self.quote_table(pipe.target)

    if not pipe.exists():
        return 0

    try:
        if begin is None and end is None and params is None:
            return (
                self.client.zcard(table_name)
                if dt_col
                else self.client.llen(table_name)
            )
    except Exception:
        return None

    df = pipe.get_data(begin=begin, end=end, params=params, debug=debug)
    if df is None:
        return 0

    return len(df)


def fetch_pipes_keys(
    self,
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False
) -> Optional[List[Tuple[str, str, Optional[str]]]]:
    """
    Return the keys for the registered pipes.
    """
    from meerschaum.utils.dataframe import query_df, parse_df_datetimes
    try:
        docs = self.read(PIPES_TABLE, debug=debug)
    except Exception:
        return []

    if not docs:
        return []

    query = {}
    if connector_keys:
        query['connector_keys'] = connector_keys
    if metric_keys:
        query['metric_key'] = metric_keys
    if location_keys:
        query['location_key'] = location_keys
    if params:
        query.update(params)

    df = query_df(
        parse_df_datetimes(docs),
        query,
        inplace=True,
    )
    return [
        (
            doc['connector_keys'],
            doc['metric_key'],
            doc['location_key'],
        )
        for doc in df.to_dict(orient='records')
    ]
