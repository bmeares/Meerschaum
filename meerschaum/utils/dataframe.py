#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for working with DataFrames.
"""

from __future__ import annotations
from meerschaum.utils.typing import (
    Optional, Dict, Any, List, Hashable, Generator,
    Iterator, Iterable,
)


def add_missing_cols_to_df(df: 'pd.DataFrame', dtypes: Dict[str, Any]) -> pd.DataFrame:
    """
    Add columns from the dtypes dictionary as null columns to a new DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The dataframe we should copy and add null columns.

    dtypes:
        The data types dictionary which may contain keys not present in `df.columns`.

    Returns
    -------
    A new `DataFrame` with the keys from `dtypes` added as null columns.
    If `df.dtypes` is the same as `dtypes`, then return a reference to `df`.
    NOTE: This will not ensure that dtypes are enforced!

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame([{'a': 1}])
    >>> dtypes = {'b': 'Int64'}
    >>> add_missing_cols_to_df(df, dtypes)
          a  b
       0  1  <NA>
    >>> add_missing_cols_to_df(df, dtypes).dtypes
    a    int64
    b    Int64
    dtype: object
    >>> add_missing_cols_to_df(df, {'a': 'object'}).dtypes
    a    int64
    dtype: object
    >>> 
    """
    if set(df.columns) == set(dtypes):
        return df

    import traceback
    from meerschaum.utils.packages import import_pandas, attempt_import
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.dtypes import to_pandas_dtype
    pandas = attempt_import('pandas')
    
    def build_series(dtype: str):
        return pandas.Series([], dtype=to_pandas_dtype(dtype))

    assign_kwargs = {
        str(col): build_series(str(typ))
        for col, typ in dtypes.items()
        if col not in df.columns
    }
    return df.assign(**assign_kwargs)


def filter_unseen_df(
        old_df: 'pd.DataFrame',
        new_df: 'pd.DataFrame',
        dtypes: Optional[Dict[str, Any]] = None,
        debug: bool = False,
    ) -> 'pd.DataFrame':
    """
    Left join two DataFrames to find the newest unseen data.

    Parameters
    ----------
    old_df: 'pd.DataFrame'
        The original (target) dataframe. Acts as a filter on the `new_df`.
        
    new_df: 'pd.DataFrame'
        The fetched (source) dataframe. Rows that are contained in `old_df` are removed.
        
    dtypes: Optional[Dict[str, Any]], default None
        Optionally specify the datatypes of the dataframe.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A pandas dataframe of the new, unseen rows in `new_df`.

    Examples
    --------
    ```python
    >>> import pandas as pd
    >>> df1 = pd.DataFrame({'a': [1,2]})
    >>> df2 = pd.DataFrame({'a': [2,3]})
    >>> filter_unseen_df(df1, df2)
       a
    0  3

    ```

    """
    if old_df is None:
        return new_df

    import json
    import functools
    import traceback
    from decimal import Decimal
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.packages import import_pandas, attempt_import
    from meerschaum.utils.dtypes import to_pandas_dtype, are_dtypes_equal, attempt_cast_to_numeric
    pd = import_pandas(debug=debug)
    is_dask = 'dask' in new_df.__module__
    if is_dask:
        pandas = attempt_import('pandas')
        dd = attempt_import('dask.dataframe')
        merge = dd.merge
        NA = pandas.NA
    else:
        merge = pd.merge
        NA = pd.NA

    new_df_dtypes = dict(new_df.dtypes)
    old_df_dtypes = dict(old_df.dtypes)

    same_cols = set(new_df.columns) == set(old_df.columns)
    if not same_cols:
        new_df = add_missing_cols_to_df(new_df, old_df_dtypes)
        old_df = add_missing_cols_to_df(old_df, new_df_dtypes)

    ### Edge case: two empty lists cast to DFs.
    elif len(new_df.columns) == 0:
        return new_df

    try:
        ### Order matters when checking equality.
        new_df = new_df[old_df.columns]
    except Exception as e:
        warn(
            "Was not able to cast old columns onto new DataFrame. " +
            f"Are both DataFrames the same shape? Error:\n{e}",
            stacklevel = 3,
        )
        return new_df[list(new_df_dtypes.keys())]

    ### assume the old_df knows what it's doing, even if it's technically wrong.
    if dtypes is None:
        dtypes = {col: str(typ) for col, typ in old_df.dtypes.items()}

    dtypes = {
        col: to_pandas_dtype(typ)
        for col, typ in dtypes.items()
        if col in new_df_dtypes and col in old_df_dtypes
    }
    for col, typ in new_df_dtypes.items():
        if col not in dtypes:
            dtypes[col] = typ
    
    cast_cols = True
    try:
        new_df = new_df.astype(dtypes)
        cast_cols = False
    except Exception as e:
        warn(
            f"Was not able to cast the new DataFrame to the given dtypes.\n{e}"
        )

    new_numeric_cols_existing = get_numeric_cols(new_df)
    old_numeric_cols = get_numeric_cols(old_df)
    for col, typ in {k: v for k, v in dtypes.items()}.items():
        if not are_dtypes_equal(new_df_dtypes.get(col, 'None'), old_df_dtypes.get(col, 'None')):
            new_is_float = are_dtypes_equal(new_df_dtypes.get(col, 'None'), 'float')
            new_is_int = are_dtypes_equal(new_df_dtypes.get(col, 'None'), 'int')
            new_is_numeric = col in new_numeric_cols_existing
            old_is_float = are_dtypes_equal(old_df_dtypes.get(col, 'None'), 'float')
            old_is_int = are_dtypes_equal(old_df_dtypes.get(col, 'None'), 'int')
            old_is_numeric = col in old_numeric_cols

            if (
                (new_is_float or new_is_int or new_is_numeric)
                and
                (old_is_float or old_is_int or old_is_numeric)
            ):
                dtypes[col] = attempt_cast_to_numeric
                cast_cols = True
                continue

            ### Fallback to object if the types don't match.
            warn(
                f"Detected different types for '{col}' "
                + f"({new_df_dtypes.get(col, None)} vs {old_df_dtypes.get(col, None)}), "
                + "falling back to 'object'..."
            )
            dtypes[col] = 'object'
            cast_cols = True

    if cast_cols:
        for col, dtype in dtypes.items():
            if col in new_df.columns:
                try:
                    new_df[col] = (
                        new_df[col].astype(dtype)
                        if not callable(dtype)
                        else new_df[col].apply(dtype)
                    )
                except Exception as e:
                    warn(f"Was not able to cast column '{col}' to dtype '{dtype}'.\n{e}")

    serializer = functools.partial(json.dumps, sort_keys=True, separators=(',', ':'), default=str)
    new_json_cols = get_json_cols(new_df)
    old_json_cols = get_json_cols(old_df)
    json_cols = set(new_json_cols + old_json_cols)
    for json_col in old_json_cols:
        old_df[json_col] = old_df[json_col].apply(serializer)
    for json_col in new_json_cols:
        new_df[json_col] = new_df[json_col].apply(serializer)

    new_numeric_cols = get_numeric_cols(new_df)
    numeric_cols = set(new_numeric_cols + old_numeric_cols)
    for numeric_col in old_numeric_cols:
        old_df[numeric_col] = old_df[numeric_col].apply(
            lambda x: f'{x:f}' if isinstance(x, Decimal) else x
        )
    for numeric_col in new_numeric_cols:
        new_df[numeric_col] = new_df[numeric_col].apply(
            lambda x: f'{x:f}' if isinstance(x, Decimal) else x
        )

    joined_df = merge(
        new_df.fillna(NA),
        old_df.fillna(NA),
        how = 'left',
        on = None,
        indicator = True,
    )
    changed_rows_mask = (joined_df['_merge'] == 'left_only')

    delta_df = joined_df[
        list(new_df_dtypes.keys())
    ][
        changed_rows_mask
    ].reset_index(drop=True)

    for json_col in json_cols:
        if json_col not in delta_df.columns:
            continue
        try:
            delta_df[json_col] = delta_df[json_col].apply(json.loads)
        except Exception as e:
            warn(f"Unable to deserialize JSON column '{json_col}':\n{traceback.format_exc()}")

    for numeric_col in numeric_cols:
        if numeric_col not in delta_df.columns:
            continue
        try:
            delta_df[numeric_col] = delta_df[numeric_col].apply(attempt_cast_to_numeric)
        except Exception as e:
            warn(f"Unable to parse numeric column '{numeric_col}':\n{traceback.format_exc()}")

    return delta_df


def parse_df_datetimes(
        df: 'pd.DataFrame',
        ignore_cols: Optional[Iterable[str]] = None,
        chunksize: Optional[int] = None,
        dtype_backend: str = 'numpy_nullable',
        debug: bool = False,
    ) -> 'pd.DataFrame':
    """
    Parse a pandas DataFrame for datetime columns and cast as datetimes.

    Parameters
    ----------
    df: pd.DataFrame
        The pandas DataFrame to parse.

    ignore_cols: Optional[Iterable[str]], default None
        If provided, do not attempt to coerce these columns as datetimes.

    chunksize: Optional[int], default None
        If the pandas implementation is `'dask'`, use this chunksize for the distributed dataframe.

    dtype_backend: str, default 'numpy_nullable'
        If `df` is not a DataFrame and new one needs to be constructed,
        use this as the datatypes backend.
        Accepted values are 'numpy_nullable' and 'pyarrow'.
        
    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A new pandas DataFrame with the determined datetime columns
    (usually ISO strings) cast as datetimes.

    Examples
    --------
    ```python
    >>> import pandas as pd
    >>> df = pd.DataFrame({'a': ['2022-01-01 00:00:00']}) 
    >>> df.dtypes
    a    object
    dtype: object
    >>> df = parse_df_datetimes(df)
    >>> df.dtypes
    a    datetime64[ns]
    dtype: object

    ```

    """
    from meerschaum.utils.packages import import_pandas, attempt_import
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    import traceback
    pd = import_pandas()
    pandas = attempt_import('pandas')
    pd_name = pd.__name__
    using_dask = 'dask' in pd_name
    df_is_dask = (hasattr(df, '__module__') and 'dask' in df.__module__)
    dask_dataframe = None
    if using_dask or df_is_dask:
        npartitions = chunksize_to_npartitions(chunksize)
        dask_dataframe = attempt_import('dask.dataframe')

    ### if df is a dict, build DataFrame
    if isinstance(df, pandas.DataFrame):
        pdf = df
    elif df_is_dask and isinstance(df, dask_dataframe.DataFrame):
        pdf = get_first_valid_dask_partition(df)
    else:
        if debug:
            dprint(f"df is of type '{type(df)}'. Building {pd.DataFrame}...")

        if using_dask:
            if isinstance(df, list):
                keys = set()
                for doc in df:
                    for key in doc:
                        keys.add(key)
                df = pd.DataFrame.from_dict(
                    {
                        k: [
                            doc.get(k, None)
                            for doc in df
                        ] for k in keys
                    },
                    npartitions = npartitions,
                )
            elif isinstance(df, dict):
                df = pd.DataFrame.from_dict(df, npartitions=npartitions)
            elif 'pandas.core.frame.DataFrame' in str(type(df)):
                df = pd.from_pandas(df, npartitions=npartitions)
            else:
                raise Exception("Can only parse dictionaries or lists of dictionaries with Dask.")
            pandas = attempt_import('pandas')
            pdf = get_first_valid_dask_partition(df)

        else:
            df = pd.DataFrame(df).convert_dtypes(dtype_backend=dtype_backend)
            pdf = df

    ### skip parsing if DataFrame is empty
    if len(pdf) == 0:
        if debug:
            dprint(f"df is empty. Returning original DataFrame without casting datetime columns...")
        return df

    ignore_cols = set(
        (ignore_cols or []) + [
            col
            for col, dtype in pdf.dtypes.items() 
            if 'datetime' in str(dtype)
        ]
    )
    cols_to_inspect = [col for col in pdf.columns if col not in ignore_cols]

    if len(cols_to_inspect) == 0:
        if debug:
            dprint(f"All columns are ignored, skipping datetime detection...")
        return df

    ### apply regex to columns to determine which are ISO datetimes
    iso_dt_regex = r'\d{4}-\d{2}-\d{2}.\d{2}\:\d{2}\:\d+'
    dt_mask = pdf[cols_to_inspect].astype(str).apply(
        lambda s: s.str.match(iso_dt_regex).all()
    )

    ### list of datetime column names
    datetime_cols = [col for col in pdf[cols_to_inspect].loc[:, dt_mask]]
    if not datetime_cols:
        if debug:
            dprint("No columns detected as datetimes, returning...")
        return df

    if debug:
        dprint("Converting columns to datetimes: " + str(datetime_cols))

    try:
        if not using_dask:
            df[datetime_cols] = df[datetime_cols].apply(pd.to_datetime, utc=True)
        else:
            df[datetime_cols] = df[datetime_cols].apply(
                pd.to_datetime,
                utc = True,
                axis = 1,
                meta = {
                    col: 'datetime64[ns]'
                    for col in datetime_cols
                }
            )
    except Exception as e:
        warn(
            f"Unable to apply `pd.to_datetime` to {items_str(datetime_cols)}:\n"
            + f"{traceback.format_exc()}"
        )

    for dt in datetime_cols:
        try:
            df[dt] = df[dt].dt.tz_localize(None)
        except Exception as e:
            warn(f"Unable to convert column '{dt}' to naive datetime:\n{traceback.format_exc()}")

    return df


def get_unhashable_cols(df: 'pd.DataFrame') -> List[str]:
    """
    Get the columns which contain unhashable objects from a Pandas DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame which may contain unhashable objects.

    Returns
    -------
    A list of columns.
    """
    if len(df) == 0:
        return []

    is_dask = 'dask' in df.__module__
    if is_dask:
        from meerschaum.utils.packages import attempt_import
        pandas = attempt_import('pandas')
        df = pandas.DataFrame(get_first_valid_dask_partition(df))
    return [
        col for col, val in df.iloc[0].items()
        if not isinstance(val, Hashable)
    ]


def get_json_cols(df: 'pd.DataFrame') -> List[str]:
    """
    Get the columns which contain unhashable objects from a Pandas DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame which may contain unhashable objects.

    Returns
    -------
    A list of columns to be encoded as JSON.
    """
    is_dask = 'dask' in df.__module__
    if is_dask:
        df = get_first_valid_dask_partition(df)
    
    if len(df) == 0:
        return []

    cols_indices = {
        col: df[col].first_valid_index()
        for col in df.columns
    }
    return [
        col
        for col, ix in cols_indices.items()
        if (
            ix is not None
            and
            not isinstance(df.loc[ix][col], Hashable)
        )
    ]


def get_numeric_cols(df: 'pd.DataFrame') -> List[str]:
    """
    Get the columns which contain `decimal.Decimal` objects from a Pandas DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame which may contain decimal objects.

    Returns
    -------
    A list of columns to treat as numerics.
    """
    from decimal import Decimal
    is_dask = 'dask' in df.__module__
    if is_dask:
        df = get_first_valid_dask_partition(df)
    
    if len(df) == 0:
        return []

    cols_indices = {
        col: df[col].first_valid_index()
        for col in df.columns
    }
    return [
        col
        for col, ix in cols_indices.items()
        if (
            ix is not None
            and
            isinstance(df.loc[ix][col], Decimal)
        )
    ]


def enforce_dtypes(
        df: 'pd.DataFrame',
        dtypes: Dict[str, str],
        debug: bool = False,
    ) -> 'pd.DataFrame':
    """
    Enforce the `dtypes` dictionary on a DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame on which to enforce dtypes.

    dtypes: Dict[str, str]
        The data types to attempt to enforce on the DataFrame.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    The Pandas DataFrame with the types enforced.
    """
    import json
    import traceback
    from decimal import Decimal
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.formatting import pprint
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.dtypes import (
        are_dtypes_equal,
        to_pandas_dtype,
        is_dtype_numeric,
        attempt_cast_to_numeric,
    )
    df_dtypes = {c: str(t) for c, t in df.dtypes.items()}
    if len(df_dtypes) == 0:
        if debug:
            dprint("Incoming DataFrame has no columns. Skipping enforcement...")
        return df

    pipe_pandas_dtypes = {
        col: to_pandas_dtype(typ)
        for col, typ in dtypes.items()
    }
    json_cols = [
        col
        for col, typ in dtypes.items()
        if typ == 'json'
    ]
    numeric_cols = [
        col
        for col, typ in dtypes.items()
        if typ == 'numeric'
    ]
    df_numeric_cols = get_numeric_cols(df)
    if debug:
        dprint(f"Desired data types:")
        pprint(dtypes)
        dprint(f"Data types for incoming DataFrame:")
        pprint(df_dtypes)

    if json_cols and len(df) > 0:
        if debug:
            dprint(f"Checking columns for JSON encoding: {json_cols}")
        for col in json_cols:
            if col in df.columns:
                try:
                    df[col] = df[col].apply(
                        (
                            lambda x: (
                                json.loads(x)
                                if isinstance(x, str)
                                else x
                            )
                        )
                    )
                except Exception as e:
                    if debug:
                        dprint(f"Unable to parse column '{col}' as JSON:\n{e}")

    if numeric_cols:
        if debug:
            dprint(f"Checking for numerics: {numeric_cols}")
        for col in numeric_cols:
            if col in df.columns:
                try:
                    df[col] = df[col].apply(attempt_cast_to_numeric)
                except Exception as e:
                    if debug:
                        dprint(f"Unable to parse column '{col}' as NUMERIC:\n{e}")

    if are_dtypes_equal(df_dtypes, pipe_pandas_dtypes):
        if debug:
            dprint(f"Data types match. Exiting enforcement...")
        return df

    common_dtypes = {}
    common_diff_dtypes = {}
    for col, typ in pipe_pandas_dtypes.items():
        if col in df_dtypes:
            common_dtypes[col] = typ
            if not are_dtypes_equal(typ, df_dtypes[col]):
                common_diff_dtypes[col] = df_dtypes[col]

    if debug:
        dprint(f"Common columns with different dtypes:")
        pprint(common_diff_dtypes)

    detected_dt_cols = {}
    for col, typ in common_diff_dtypes.items():
        if 'datetime' in typ and 'datetime' in common_dtypes[col]:
            df_dtypes[col] = typ
            detected_dt_cols[col] = (common_dtypes[col], common_diff_dtypes[col])
    for col in detected_dt_cols:
        del common_diff_dtypes[col]

    if debug:
        dprint(f"Common columns with different dtypes (after dates):")
        pprint(common_diff_dtypes)

    if are_dtypes_equal(df_dtypes, pipe_pandas_dtypes):
        if debug:
            dprint(
                "The incoming DataFrame has mostly the same types, skipping enforcement."
                + f"The only detected difference was in the following datetime columns.\n"
                + "    Timezone information may be stripped."
            )
            pprint(detected_dt_cols)
        return df

    for col, typ in {k: v for k, v in common_diff_dtypes.items()}.items():
        previous_typ = common_dtypes[col]
        mixed_numeric_types = (is_dtype_numeric(typ) and is_dtype_numeric(previous_typ))
        explicitly_float = are_dtypes_equal(dtypes.get(col, 'object'), 'float')
        explicitly_numeric = dtypes.get(col, 'numeric') == 'numeric'
        cast_to_numeric = (
            explicitly_numeric
            or col in df_numeric_cols
            or (mixed_numeric_types and not explicitly_float)
        )
        if cast_to_numeric:
            common_dtypes[col] = attempt_cast_to_numeric
            common_diff_dtypes[col] = attempt_cast_to_numeric

    for d in common_diff_dtypes:
        t = common_dtypes[d]
        if debug:
            dprint(f"Casting column {d} to dtype {t}.")
        try:
            df[d] = (
                df[d].apply(t)
                if callable(t)
                else df[d].astype(t)
            )
        except Exception as e:
            if debug:
                dprint(f"Encountered an error when casting column {d} to type {t}:\n{e}")
            if 'int' in str(t).lower():
                try:
                    df[d] = df[d].astype('float64').astype(t)
                except Exception as e:
                    if debug:
                        dprint(f"Was unable to convert to float then {t}.")
    return df


def get_datetime_bound_from_df(
        df: Union['pd.DataFrame', dict, list],
        datetime_column: str,
        minimum: bool = True,
    ) -> Union[int, 'datetime.datetime', None]:
    """
    Return the minimum or maximum datetime (or integer) from a DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame, list, or dict which contains the range axis.

    datetime_column: str
        The name of the datetime (or int) column.

    minimum: bool
        Whether to return the minimum (default) or maximum value.

    Returns
    -------
    The minimum or maximum datetime value in the dataframe, or `None`.
    """
    if not datetime_column:
        return None

    def compare(a, b):
        if a is None:
            return b
        if b is None:
            return a
        if minimum:
            return a if a < b else b
        return a if a > b else b

    if isinstance(df, list):
        if len(df) == 0:
            return None
        best_yet = df[0].get(datetime_column, None)
        for doc in df:
            val = doc.get(datetime_column, None)
            best_yet = compare(best_yet, val)
        return best_yet

    if isinstance(df, dict):
        if datetime_column not in df:
            return None
        best_yet = df[datetime_column][0]
        for val in df[datetime_column]:
            best_yet = compare(best_yet, val)
        return best_yet

    if 'DataFrame' in str(type(df)):
        if datetime_column not in df.columns:
            return None
        return (
            df[datetime_column].min(skipna=True)
            if minimum
            else df[datetime_column].max(skipna=True)
        )

    return None


def df_is_chunk_generator(df: Any) -> bool:
    """
    Determine whether to treat `df` as a chunk generator.

    Note this should only be used in a context where generators are expected,
    as it will return `True` for any iterable.

    Parameters
    ----------
    The DataFrame or chunk generator to evaluate.

    Returns
    -------
    A `bool` indicating whether to treat `df` as a generator.
    """
    return (
        not isinstance(df, (dict, list, str))
        and 'DataFrame' not in str(type(df))
        and isinstance(df, (Generator, Iterable, Iterator))
    )


def chunksize_to_npartitions(chunksize: Optional[int]) -> int:
    """
    Return the Dask `npartitions` value for a given `chunksize`.
    """
    if chunksize == -1:
        from meerschaum.config import get_config
        chunksize = get_config('system', 'connectors', 'sql', 'chunksize')
    if chunksize is None:
        return 1
    return -1 * chunksize


def df_from_literal(
        pipe: Optional['meerschaum.Pipe'] = None,
        literal: str = None,
        debug: bool = False
    ) -> 'pd.DataFrame':
    """
    Construct a dataframe from a literal value, using the pipe's datetime and value column names.

    Parameters
    ----------
    pipe: Optional['meerschaum.Pipe'], default None
        The pipe which will consume the literal value.

    Returns
    -------
    A 1-row pandas DataFrame from with the current UTC timestamp as the datetime columns
    and the literal as the value.
    """
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.warnings import error, warn
    from meerschaum.utils.debug import dprint

    if pipe is None or literal is None:
        error("Please provide a Pipe and a literal value")
    ### this will raise an error if the columns are undefined
    dt_name, val_name = pipe.get_columns('datetime', 'value')

    val = literal
    if isinstance(literal, str):
        if debug:
            dprint(f"Received literal string: '{literal}'")
        import ast
        try:
            val = ast.literal_eval(literal)
        except Exception as e:
            warn(
                "Failed to parse value from string:\n" + f"{literal}" +
                "\n\nWill cast as a string instead."\
            )
            val = literal

    import datetime
    now = datetime.datetime.utcnow()

    pd = import_pandas()
    return pd.DataFrame({dt_name: [now], val_name: [val]})


def get_first_valid_dask_partition(ddf: 'dask.dataframe.DataFrame') -> Union['pd.DataFrame', None]:
    """
    Return the first valid Dask DataFrame partition (if possible).
    """
    pdf = None
    for partition in ddf.partitions:
        try:
            pdf = partition.compute()
        except Exception as e:
            continue
        if len(pdf) > 0:
            return pdf
    return ddf.compute()
