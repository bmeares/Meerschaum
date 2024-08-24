#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define pipes methods for `ValkeyConnector`.
"""

import json
from datetime import datetime

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Any, Union, Optional, Dict, List

PIPES_TABLE: str = 'pipes'
PIPES_COUNTER: str = 'pipes:counter'

def get_pipe_key(pipe: mrsm.Pipe) -> str:
    """
    Return the key to store a pipe's ID.
    """
    return f"pipe:{pipe.connector_keys}:{pipe.metric_key}:{pipe.location_key}"


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
        'parameters': json.dumps(
            pipe._attributes.get('parameters', {}),
            separators=(',', ':'),
        ),
    }

    pipe_key = get_pipe_key(pipe)

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

        pipe_key = get_pipe_key(pipe)
        self.set(pipe_key, pipe_id)

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

    docs = list(self.read_docs(
        PIPES_TABLE,
        begin=pipe_id,
        end=pipe_id,
        debug=debug,
    ))
    if not docs:
        return {}

    doc = docs[0]
    doc['parameters'] = json.loads(doc['parameters'])
    return doc


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

    doc_key = f"{PIPES_TABLE}:{pipe_id}"
    doc = {
        'pipe_id': pipe_id,
        'connector_keys': str(pipe.connector_keys),
        'metric_key': str(pipe.metric_key),
        'location_key': str(pipe.location_key),
        'parameters': json.dumps(
            pipe._attributes.get('parameters', {}),
            separators=(',', ':'),
        ),
    }

    self.client.hset(
        doc_key,
        mapping={
            str(k): str(v)
            for k, v in doc.items()
        },
    )

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
    self.client.delete(pipe_key)
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

    dt_col = pipe.columns.get("datetime", None)
    return self.read(
        pipe.target,
        begin=begin,
        end=end,
        datetime_column=dt_col,
        params=params,
        debug=debug,
    )


def sync_pipe(
    self,
    pipe: mrsm.Pipe,
    df: 'pd.DataFrame' = None,
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

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    if df is None:
        return False, f"Received `None`, cannot sync {pipe}."

    dt_col = pipe.columns.get('datetime', None)

    ### NOTE: Persist dtypes on the first sync, so that enforcement will apply to future syncs.
    if not pipe.exists():
        existing_dtypes = pipe.dtypes
        new_dtypes = {
            str(key): str(val)
            for key, val in df.dtypes.items()
            if str(key) not in existing_dtypes
        }
        if new_dtypes:
            pipe.dtypes.update(new_dtypes)
            edit_success, edit_msg = pipe.edit()
            if not edit_success:
                return edit_success, edit_msg

    unseen_df, update_df, delta_df = pipe.filter_existing(df, debug=debug)
    if not unseen_df.empty:
        self.push_df(
            unseen_df,
            pipe.target,
            datetime_column=dt_col,
            debug=debug,
        )
    ### TODO implement updates
    return True, "Success"


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
