#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for working with DataFrames.
"""

from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from collections import defaultdict

import meerschaum as mrsm
from meerschaum.utils.typing import (
    Optional, Dict, Any, List, Hashable, Generator,
    Iterator, Iterable, Union, TYPE_CHECKING,
)

if TYPE_CHECKING:
    pd, dask = mrsm.attempt_import('pandas', 'dask')


def add_missing_cols_to_df(
    df: 'pd.DataFrame',
    dtypes: Dict[str, Any],
) -> 'pd.DataFrame':
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

    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.dtypes import to_pandas_dtype
    pandas = attempt_import('pandas')

    def build_series(dtype: str):
        return pandas.Series([], dtype=to_pandas_dtype(dtype))

    assign_kwargs = {
        str(col): build_series(str(typ))
        for col, typ in dtypes.items()
        if col not in df.columns
    }
    df_with_cols = df.assign(**assign_kwargs)
    for col in assign_kwargs:
        df_with_cols[col] = df_with_cols[col].fillna(pandas.NA)
    return df_with_cols


def filter_unseen_df(
    old_df: 'pd.DataFrame',
    new_df: 'pd.DataFrame',
    safe_copy: bool = True,
    dtypes: Optional[Dict[str, Any]] = None,
    include_unchanged_columns: bool = False,
    coerce_mixed_numerics: bool = True,
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

    safe_copy: bool, default True
        If `True`, create a copy before comparing and modifying the dataframes.
        Setting to `False` may mutate the DataFrames.

    dtypes: Optional[Dict[str, Any]], default None
        Optionally specify the datatypes of the dataframe.

    include_unchanged_columns: bool, default False
        If `True`, include columns which haven't changed on rows which have changed.

    coerce_mixed_numerics: bool, default True
        If `True`, cast mixed integer and float columns between the old and new dataframes into
        numeric values (`decimal.Decimal`).

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

    if safe_copy:
        old_df = old_df.copy()
        new_df = new_df.copy()

    import json
    import functools
    import traceback
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.packages import import_pandas, attempt_import
    from meerschaum.utils.dtypes import (
        to_pandas_dtype,
        are_dtypes_equal,
        attempt_cast_to_numeric,
        attempt_cast_to_uuid,
        attempt_cast_to_bytes,
        attempt_cast_to_geometry,
        coerce_timezone,
        serialize_decimal,
    )
    from meerschaum.utils.dtypes.sql import get_numeric_precision_scale
    pd = import_pandas(debug=debug)
    is_dask = 'dask' in new_df.__module__
    if is_dask:
        pandas = attempt_import('pandas')
        _ = attempt_import('partd', lazy=False)
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

        new_types_missing_from_old = {
            col: typ
            for col, typ in new_df_dtypes.items()
            if col not in old_df_dtypes
        }
        old_types_missing_from_new = {
            col: typ
            for col, typ in new_df_dtypes.items()
            if col not in old_df_dtypes
        }
        old_df_dtypes.update(new_types_missing_from_old)
        new_df_dtypes.update(old_types_missing_from_new)

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
            stacklevel=3,
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

    numeric_cols_precisions_scales = {
        col: get_numeric_precision_scale(None, typ)
        for col, typ in dtypes.items()
        if col and str(typ).lower().startswith('numeric')
    }

    dt_dtypes = {
        col: typ
        for col, typ in dtypes.items()
        if are_dtypes_equal(typ, 'datetime')
    }
    non_dt_dtypes = {
        col: typ
        for col, typ in dtypes.items()
        if col not in dt_dtypes
    }

    cast_non_dt_cols = True
    try:
        new_df = new_df.astype(non_dt_dtypes)
        cast_non_dt_cols = False
    except Exception as e:
        warn(
            f"Was not able to cast the new DataFrame to the given dtypes.\n{e}"
        )

    cast_dt_cols = True
    try:
        for col, typ in dt_dtypes.items():
            strip_utc = (
                (dtypes or {}).get(col, 'datetime') == 'datetime64[ns]'
            )
            if col in old_df.columns:
                old_df[col] = coerce_timezone(old_df[col], strip_utc=strip_utc)
            if col in new_df.columns:
                new_df[col] = coerce_timezone(new_df[col], strip_utc=strip_utc)
        cast_dt_cols = False
    except Exception as e:
        warn(f"Could not cast datetime columns:\n{e}")

    cast_cols = cast_dt_cols or cast_non_dt_cols

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
                coerce_mixed_numerics
                and
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
        old_df[numeric_col] = old_df[numeric_col].apply(serialize_decimal)
    for numeric_col in new_numeric_cols:
        new_df[numeric_col] = new_df[numeric_col].apply(serialize_decimal)

    old_dt_cols = [
        col
        for col, typ in old_df.dtypes.items()
        if are_dtypes_equal(str(typ), 'datetime')
    ]
    for col in old_dt_cols:
        strip_utc = (
            (dtypes or {}).get(col, 'datetime') == 'datetime64[ns]'
        )
        old_df[col] = coerce_timezone(old_df[col], strip_utc=strip_utc)

    new_dt_cols = [
        col
        for col, typ in new_df.dtypes.items()
        if are_dtypes_equal(str(typ), 'datetime')
    ]
    for col in new_dt_cols:
        strip_utc = (
            (dtypes or {}).get(col, 'datetime') == 'datetime64[ns]'
        )
        new_df[col] = coerce_timezone(new_df[col], strip_utc=strip_utc)

    old_uuid_cols = get_uuid_cols(old_df)
    new_uuid_cols = get_uuid_cols(new_df)
    uuid_cols = set(new_uuid_cols + old_uuid_cols)

    old_bytes_cols = get_bytes_cols(old_df)
    new_bytes_cols = get_bytes_cols(new_df)
    bytes_cols = set(new_bytes_cols + old_bytes_cols)

    old_geometry_cols = get_geometry_cols(old_df)
    new_geometry_cols = get_geometry_cols(new_df)
    geometry_cols = set(new_geometry_cols + old_geometry_cols)

    joined_df = merge(
        new_df.infer_objects(copy=False).fillna(NA),
        old_df.infer_objects(copy=False).fillna(NA),
        how='left',
        on=None,
        indicator=True,
    )
    changed_rows_mask = (joined_df['_merge'] == 'left_only')
    new_cols = list(new_df_dtypes)
    delta_df = joined_df[new_cols][changed_rows_mask].reset_index(drop=True)

    for json_col in json_cols:
        if json_col not in delta_df.columns:
            continue
        try:
            delta_df[json_col] = delta_df[json_col].apply(json.loads)
        except Exception:
            warn(f"Unable to deserialize JSON column '{json_col}':\n{traceback.format_exc()}")

    for numeric_col in numeric_cols:
        if numeric_col not in delta_df.columns:
            continue
        try:
            delta_df[numeric_col] = delta_df[numeric_col].apply(
                functools.partial(
                    attempt_cast_to_numeric,
                    quantize=True,
                    precision=numeric_cols_precisions_scales.get(numeric_col, (None, None)[0]),
                    scale=numeric_cols_precisions_scales.get(numeric_col, (None, None)[1]),
                )
            )
        except Exception:
            warn(f"Unable to parse numeric column '{numeric_col}':\n{traceback.format_exc()}")

    for uuid_col in uuid_cols:
        if uuid_col not in delta_df.columns:
            continue
        try:
            delta_df[uuid_col] = delta_df[uuid_col].apply(attempt_cast_to_uuid)
        except Exception:
            warn(f"Unable to parse numeric column '{uuid_col}':\n{traceback.format_exc()}")

    for bytes_col in bytes_cols:
        if bytes_col not in delta_df.columns:
            continue
        try:
            delta_df[bytes_col] = delta_df[bytes_col].apply(attempt_cast_to_bytes)
        except Exception:
            warn(f"Unable to parse bytes column '{bytes_col}':\n{traceback.format_exc()}")

    for geometry_col in geometry_cols:
        if geometry_col not in delta_df.columns:
            continue
        try:
            delta_df[geometry_col] = delta_df[geometry_col].apply(attempt_cast_to_geometry)
        except Exception:
            warn(f"Unable to parse bytes column '{bytes_col}':\n{traceback.format_exc()}")

    return delta_df


def parse_df_datetimes(
    df: 'pd.DataFrame',
    ignore_cols: Optional[Iterable[str]] = None,
    strip_timezone: bool = False,
    chunksize: Optional[int] = None,
    dtype_backend: str = 'numpy_nullable',
    ignore_all: bool = False,
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

    strip_timezone: bool, default False
        If `True`, remove the UTC `tzinfo` property.

    chunksize: Optional[int], default None
        If the pandas implementation is `'dask'`, use this chunksize for the distributed dataframe.

    dtype_backend: str, default 'numpy_nullable'
        If `df` is not a DataFrame and new one needs to be constructed,
        use this as the datatypes backend.
        Accepted values are 'numpy_nullable' and 'pyarrow'.

    ignore_all: bool, default False
        If `True`, do not attempt to cast any columns to datetimes.

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
    from meerschaum.utils.misc import items_str
    from meerschaum.utils.dtypes import to_datetime
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
                    npartitions=npartitions,
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
            dprint("df is empty. Returning original DataFrame without casting datetime columns...")
        return df

    ignore_cols = set(
        (ignore_cols or []) + [
            col
            for col, dtype in pdf.dtypes.items() 
            if 'datetime' in str(dtype)
        ]
    )
    cols_to_inspect = [
        col
        for col in pdf.columns
        if col not in ignore_cols
    ] if not ignore_all else []

    if len(cols_to_inspect) == 0:
        if debug:
            dprint("All columns are ignored, skipping datetime detection...")
        return df.infer_objects(copy=False).fillna(pandas.NA)

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
        return df.infer_objects(copy=False).fillna(pandas.NA)

    if debug:
        dprint("Converting columns to datetimes: " + str(datetime_cols))

    try:
        if not using_dask:
            df[datetime_cols] = df[datetime_cols].apply(to_datetime)
        else:
            df[datetime_cols] = df[datetime_cols].apply(
                to_datetime,
                utc=True,
                axis=1,
                meta={
                    col: 'datetime64[ns, UTC]'
                    for col in datetime_cols
                }
            )
    except Exception:
        warn(
            f"Unable to apply `pd.to_datetime` to {items_str(datetime_cols)}:\n"
            + f"{traceback.format_exc()}"
        )

    if strip_timezone:
        for dt in datetime_cols:
            try:
                df[dt] = df[dt].dt.tz_localize(None)
            except Exception:
                warn(
                    f"Unable to convert column '{dt}' to naive datetime:\n"
                    + f"{traceback.format_exc()}"
                )

    return df.fillna(pandas.NA)


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
    if df is None:
        return []
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
    if df is None:
        return []

    is_dask = 'dask' in df.__module__ if hasattr(df, '__module__') else False
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
    if df is None:
        return []
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


def get_uuid_cols(df: 'pd.DataFrame') -> List[str]:
    """
    Get the columns which contain `uuid.UUID` objects from a Pandas DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame which may contain UUID objects.

    Returns
    -------
    A list of columns to treat as UUIDs.
    """
    if df is None:
        return []
    from uuid import UUID
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
            isinstance(df.loc[ix][col], UUID)
        )
    ]


def get_datetime_cols(
    df: 'pd.DataFrame',
    timezone_aware: bool = True,
    timezone_naive: bool = True,
) -> List[str]:
    """
    Get the columns which contain `datetime` or `Timestamp` objects from a Pandas DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame which may contain `datetime` or `Timestamp` objects.

    timezone_aware: bool, default True
        If `True`, include timezone-aware datetime columns.

    timezone_naive: bool, default True
        If `True`, include timezone-naive datetime columns.

    Returns
    -------
    A list of columns to treat as datetimes.
    """
    if not timezone_aware and not timezone_naive:
        raise ValueError("`timezone_aware` and `timezone_naive` cannot both be `False`.")

    if df is None:
        return []

    from datetime import datetime
    from meerschaum.utils.dtypes import are_dtypes_equal
    is_dask = 'dask' in df.__module__
    if is_dask:
        df = get_first_valid_dask_partition(df)

    known_dt_cols = [
        col
        for col, typ in df.dtypes.items()
        if are_dtypes_equal('datetime', str(typ))
    ]

    if len(df) == 0:
        return known_dt_cols

    cols_indices = {
        col: df[col].first_valid_index()
        for col in df.columns
        if col not in known_dt_cols
    }
    pydt_cols = [
        col
        for col, ix in cols_indices.items()
        if (
            ix is not None
            and
            isinstance(df.loc[ix][col], datetime)
        )
    ]
    dt_cols_set = set(known_dt_cols + pydt_cols)
    all_dt_cols = [
        col
        for col in df.columns
        if col in dt_cols_set
    ]
    if timezone_aware and timezone_naive:
        return all_dt_cols

    known_timezone_aware_dt_cols = [
        col
        for col in known_dt_cols
        if getattr(df[col], 'tz', None) is not None
    ]
    timezone_aware_pydt_cols = [
        col
        for col in pydt_cols
        if df.loc[cols_indices[col]][col].tzinfo is not None
    ]
    timezone_aware_dt_cols_set = set(known_timezone_aware_dt_cols + timezone_aware_pydt_cols)
    if timezone_aware:
        return [
            col
            for col in all_dt_cols
            if col in timezone_aware_pydt_cols
        ]

    return [
        col
        for col in all_dt_cols
        if col not in timezone_aware_dt_cols_set
    ]


def get_bytes_cols(df: 'pd.DataFrame') -> List[str]:
    """
    Get the columns which contain bytes strings from a Pandas DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame which may contain bytes strings.

    Returns
    -------
    A list of columns to treat as bytes.
    """
    if df is None:
        return []
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
            isinstance(df.loc[ix][col], bytes)
        )
    ]


def get_geometry_cols(
    df: 'pd.DataFrame',
    with_types_srids: bool = False,
) -> Union[List[str], Dict[str, Any]]:
    """
    Get the columns which contain shapely objects from a Pandas DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame which may contain bytes strings.

    with_types_srids: bool, default False
        If `True`, return a dictionary mapping columns to geometry types and SRIDs.

    Returns
    -------
    A list of columns to treat as `geometry`.
    If `with_types_srids`, return a dictionary mapping columns to tuples in the form (type, SRID).
    """
    if df is None:
        return []

    is_dask = 'dask' in df.__module__
    if is_dask:
        df = get_first_valid_dask_partition(df)

    if len(df) == 0:
        return []

    cols_indices = {
        col: df[col].first_valid_index()
        for col in df.columns
    }
    geo_cols = [
        col
        for col, ix in cols_indices.items()
        if (
            ix is not None
            and
            'shapely' in str(type(df.loc[ix][col]))
        )
    ]
    if not with_types_srids:
        return geo_cols

    gpd = mrsm.attempt_import('geopandas', lazy=False)
    geo_cols_types_srids = {}
    for col in geo_cols:
        try:
            sample_geo_series = gpd.GeoSeries(df[col], crs=None)
            geometry_types = {geom.geom_type for geom in sample_geo_series}
            srid = (
                (
                    sample_geo_series.crs.sub_crs_list[0].to_epsg()
                    if sample_geo_series.crs.is_compound
                    else sample_geo_series.crs.to_epsg()
                )
                if sample_geo_series.crs
                else 0
            )
            geometry_type = list(geometry_types)[0] if len(geometry_types) == 1 else 'geometry'
        except Exception:
            srid = 0
            geometry_type = 'geometry'
        geo_cols_types_srids[col] = (geometry_type, srid)

    return geo_cols_types_srids


def enforce_dtypes(
    df: 'pd.DataFrame',
    dtypes: Dict[str, str],
    safe_copy: bool = True,
    coerce_numeric: bool = True,
    coerce_timezone: bool = True,
    strip_timezone: bool = False,
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

    safe_copy: bool, default True
        If `True`, create a copy before comparing and modifying the dataframes.
        Setting to `False` may mutate the DataFrames.
        See `meerschaum.utils.dataframe.filter_unseen_df`.

    coerce_numeric: bool, default True
        If `True`, convert float and int collisions to numeric.

    coerce_timezone: bool, default True
        If `True`, convert datetimes to UTC.

    strip_timezone: bool, default False
        If `coerce_timezone` and `strip_timezone` are `True`,
        remove timezone information from datetimes.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    The Pandas DataFrame with the types enforced.
    """
    import json
    import functools
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.dtypes import (
        are_dtypes_equal,
        to_pandas_dtype,
        is_dtype_numeric,
        attempt_cast_to_numeric,
        attempt_cast_to_uuid,
        attempt_cast_to_bytes,
        attempt_cast_to_geometry,
        coerce_timezone as _coerce_timezone,
    )
    from meerschaum.utils.dtypes.sql import get_numeric_precision_scale
    pandas = mrsm.attempt_import('pandas')
    is_dask = 'dask' in df.__module__
    if safe_copy:
        df = df.copy()
    if len(df.columns) == 0:
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
        if typ.startswith('numeric')
    ]
    geometry_cols = [
        col
        for col, typ in dtypes.items()
        if typ.startswith('geometry') or typ.startswith('geography')
    ]
    uuid_cols = [
        col
        for col, typ in dtypes.items()
        if typ == 'uuid'
    ]
    bytes_cols = [
        col
        for col, typ in dtypes.items()
        if typ == 'bytes'
    ]
    datetime_cols = [
        col
        for col, typ in dtypes.items()
        if are_dtypes_equal(typ, 'datetime')
    ]
    df_numeric_cols = get_numeric_cols(df)
    if debug:
        dprint("Desired data types:")
        pprint(dtypes)
        dprint("Data types for incoming DataFrame:")
        pprint({_col: str(_typ) for _col, _typ in df.dtypes.items()})

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
            precision, scale = get_numeric_precision_scale(None, dtypes.get(col, ''))
            if col in df.columns:
                try:
                    df[col] = df[col].apply(
                        functools.partial(
                            attempt_cast_to_numeric,
                            quantize=True,
                            precision=precision,
                            scale=scale,
                        )
                    )
                except Exception as e:
                    if debug:
                        dprint(f"Unable to parse column '{col}' as NUMERIC:\n{e}")

    if uuid_cols:
        if debug:
            dprint(f"Checking for UUIDs: {uuid_cols}")
        for col in uuid_cols:
            if col in df.columns:
                try:
                    df[col] = df[col].apply(attempt_cast_to_uuid)
                except Exception as e:
                    if debug:
                        dprint(f"Unable to parse column '{col}' as UUID:\n{e}")

    if bytes_cols:
        if debug:
            dprint(f"Checking for bytes: {bytes_cols}")
        for col in bytes_cols:
            if col in df.columns:
                try:
                    df[col] = df[col].apply(attempt_cast_to_bytes)
                except Exception as e:
                    if debug:
                        dprint(f"Unable to parse column '{col}' as bytes:\n{e}")

    if datetime_cols and coerce_timezone:
        if debug:
            dprint(f"Checking for datetime conversion: {datetime_cols}")
        for col in datetime_cols:
            if col in df.columns:
                df[col] = _coerce_timezone(df[col], strip_utc=strip_timezone)

    if geometry_cols:
        geopandas = mrsm.attempt_import('geopandas')
        if debug:
            dprint(f"Checking for geometry: {geometry_cols}")
        parsed_geom_cols = []
        for col in geometry_cols:
            try:
                df[col] = df[col].apply(attempt_cast_to_geometry)
                parsed_geom_cols.append(col)
            except Exception as e:
                if debug:
                    dprint(f"Unable to parse column '{col}' as geometry:\n{e}")

        if parsed_geom_cols:
            if debug:
                dprint(f"Converting to GeoDataFrame (geometry column: '{parsed_geom_cols[0]}')...")
            df = geopandas.GeoDataFrame(df, geometry=parsed_geom_cols[0])
            try:
                df.rename_geometry(parsed_geom_cols[0], inplace=True)
            except ValueError:
                pass

    df_dtypes = {c: str(t) for c, t in df.dtypes.items()}
    if are_dtypes_equal(df_dtypes, pipe_pandas_dtypes):
        if debug:
            dprint("Data types match. Exiting enforcement...")
        return df

    common_dtypes = {}
    common_diff_dtypes = {}
    for col, typ in pipe_pandas_dtypes.items():
        if col in df_dtypes:
            common_dtypes[col] = typ
            if not are_dtypes_equal(typ, df_dtypes[col]):
                common_diff_dtypes[col] = df_dtypes[col]

    if debug:
        dprint("Common columns with different dtypes:")
        pprint(common_diff_dtypes)

    detected_dt_cols = {}
    for col, typ in common_diff_dtypes.items():
        if 'datetime' in typ and 'datetime' in common_dtypes[col]:
            df_dtypes[col] = typ
            detected_dt_cols[col] = (common_dtypes[col], common_diff_dtypes[col])
    for col in detected_dt_cols:
        del common_diff_dtypes[col]

    if debug:
        dprint("Common columns with different dtypes (after dates):")
        pprint(common_diff_dtypes)

    if are_dtypes_equal(df_dtypes, pipe_pandas_dtypes):
        if debug:
            dprint(
                "The incoming DataFrame has mostly the same types, skipping enforcement."
                + "The only detected difference was in the following datetime columns."
            )
            pprint(detected_dt_cols)
        return df

    for col, typ in {k: v for k, v in common_diff_dtypes.items()}.items():
        previous_typ = common_dtypes[col]
        mixed_numeric_types = (is_dtype_numeric(typ) and is_dtype_numeric(previous_typ))
        explicitly_float = are_dtypes_equal(dtypes.get(col, 'object'), 'float')
        explicitly_numeric = dtypes.get(col, 'numeric').startswith('numeric')
        cast_to_numeric = (
            explicitly_numeric
            or col in df_numeric_cols
            or (mixed_numeric_types and not explicitly_float)
        ) and coerce_numeric
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
                except Exception:
                    if debug:
                        dprint(f"Was unable to convert to float then {t}.")
    return df


def get_datetime_bound_from_df(
    df: Union['pd.DataFrame', Dict[str, List[Any]], List[Dict[str, Any]]],
    datetime_column: str,
    minimum: bool = True,
) -> Union[int, datetime, None]:
    """
    Return the minimum or maximum datetime (or integer) from a DataFrame.

    Parameters
    ----------
    df: Union['pd.DataFrame', Dict[str, List[Any]], List[Dict[str, Any]]]
        The DataFrame, list, or dict which contains the range axis.

    datetime_column: str
        The name of the datetime (or int) column.

    minimum: bool
        Whether to return the minimum (default) or maximum value.

    Returns
    -------
    The minimum or maximum datetime value in the dataframe, or `None`.
    """
    from meerschaum.utils.dtypes import to_datetime, value_is_null

    if df is None:
        return None
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
        from meerschaum.utils.dtypes import are_dtypes_equal
        pandas = mrsm.attempt_import('pandas')
        is_dask = 'dask' in df.__module__

        if datetime_column not in df.columns:
            return None

        try:
            dt_val = (
                df[datetime_column].min(skipna=True)
                if minimum
                else df[datetime_column].max(skipna=True)
            )
        except Exception:
            dt_val = pandas.NA
        if is_dask and dt_val is not None and dt_val is not pandas.NA:
            dt_val = dt_val.compute()

        return (
            to_datetime(dt_val, as_pydatetime=True)
            if are_dtypes_equal(str(type(dt_val)), 'datetime')
            else (dt_val if not value_is_null(dt_val) else None)
        )

    return None


def get_unique_index_values(
    df: Union['pd.DataFrame', Dict[str, List[Any]], List[Dict[str, Any]]],
    indices: List[str],
) -> Dict[str, List[Any]]:
    """
    Return a dictionary of the unique index values in a DataFrame.

    Parameters
    ----------
    df: Union['pd.DataFrame', Dict[str, List[Any]], List[Dict[str, Any]]]
        The dataframe (or list or dict) which contains index values.

    indices: List[str]
        The list of index columns.

    Returns
    -------
    A dictionary mapping indices to unique values.
    """
    if df is None:
        return {}
    if 'dataframe' in str(type(df)).lower():
        pandas = mrsm.attempt_import('pandas')
        return {
            col: list({
                (val if val is not pandas.NA else None)
                for val in df[col].unique()
            })
            for col in indices
            if col in df.columns
        }

    unique_indices = defaultdict(lambda: set())
    if isinstance(df, list):
        for doc in df:
            for index in indices:
                if index in doc:
                    unique_indices[index].add(doc[index])

    elif isinstance(df, dict):
        for index in indices:
            if index in df:
                unique_indices[index] = unique_indices[index].union(set(df[index]))

    return {key: list(val) for key, val in unique_indices.items()}


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
    pipe: Optional[mrsm.Pipe] = None,
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
        except Exception:
            warn(
                "Failed to parse value from string:\n" + f"{literal}" +
                "\n\nWill cast as a string instead."\
            )
            val = literal

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).replace(tzinfo=None)

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
        except Exception:
            continue
        if len(pdf) > 0:
            return pdf
    _ = mrsm.attempt_import('partd', lazy=False)
    return ddf.compute()


def query_df(
    df: 'pd.DataFrame',
    params: Optional[Dict[str, Any]] = None,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    datetime_column: Optional[str] = None,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    inplace: bool = False,
    reset_index: bool = False,
    coerce_types: bool = False,
    debug: bool = False,
) -> 'pd.DataFrame':
    """
    Query the dataframe with the params dictionary.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame to query against.

    params: Optional[Dict[str, Any]], default None
        The parameters dictionary to use for the query.

    begin: Union[datetime, int, None], default None
        If `begin` and `datetime_column` are provided, only return rows with a timestamp
        greater than or equal to this value.

    end: Union[datetime, int, None], default None
        If `begin` and `datetime_column` are provided, only return rows with a timestamp
        less than this value.

    datetime_column: Optional[str], default None
        A `datetime_column` must be provided to use `begin` and `end`.

    select_columns: Optional[List[str]], default None
        If provided, only return these columns.

    omit_columns: Optional[List[str]], default None
        If provided, do not include these columns in the result.

    inplace: bool, default False
        If `True`, modify the DataFrame inplace rather than creating a new DataFrame.

    reset_index: bool, default False
        If `True`, reset the index in the resulting DataFrame.

    coerce_types: bool, default False
        If `True`, cast the dataframe and parameters as strings before querying.

    Returns
    -------
    A Pandas DataFrame query result.
    """

    def _process_select_columns(_df):
        if not select_columns:
            return
        for col in list(_df.columns):
            if col not in select_columns:
                del _df[col]

    def _process_omit_columns(_df):
        if not omit_columns:
            return
        for col in list(_df.columns):
            if col in omit_columns:
                del _df[col]

    if not params and not begin and not end:
        if not inplace:
            df = df.copy()
        _process_select_columns(df)
        _process_omit_columns(df)
        return df

    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import get_in_ex_params
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.dtypes import are_dtypes_equal, value_is_null
    dateutil_parser = mrsm.attempt_import('dateutil.parser')
    pandas = mrsm.attempt_import('pandas')
    NA = pandas.NA

    if params:
        params = params.copy()
        for key, val in {k: v for k, v in params.items()}.items():
            if isinstance(val, (list, tuple)):
                if None in val:
                    val = [item for item in val if item is not None] + [NA]
                    params[key] = val
                if coerce_types:
                    params[key] = [str(x) for x in val]
            else:
                if value_is_null(val):
                    val = NA
                    params[key] = NA
                if coerce_types:
                    params[key] = str(val)

    dtypes = {col: str(typ) for col, typ in df.dtypes.items()}

    if inplace:
        df.fillna(NA, inplace=True)
    else:
        df = df.infer_objects().fillna(NA)

    if isinstance(begin, str):
        begin = dateutil_parser.parse(begin)
    if isinstance(end, str):
        end = dateutil_parser.parse(end)

    if begin is not None or end is not None:
        if not datetime_column or datetime_column not in df.columns:
            warn(
                f"The datetime column '{datetime_column}' is not present in the Dataframe, "
                + "ignoring begin and end...",
            )
            begin, end = None, None

    if debug:
        dprint(f"Querying dataframe:\n{params=} {begin=} {end=} {datetime_column=}")

    if datetime_column and (begin is not None or end is not None):
        if debug:
            dprint("Checking for datetime column compatability.")

        from meerschaum.utils.dtypes import coerce_timezone
        df_is_dt = are_dtypes_equal(str(df.dtypes[datetime_column]), 'datetime')
        begin_is_int = are_dtypes_equal(str(type(begin)), 'int')
        end_is_int = are_dtypes_equal(str(type(end)), 'int')

        if df_is_dt:
            df_tz = (
                getattr(df[datetime_column].dt, 'tz', None)
                if hasattr(df[datetime_column], 'dt')
                else None
            )

            if begin_is_int:
                begin = datetime.fromtimestamp(int(begin), timezone.utc).replace(tzinfo=None)
                if debug:
                    dprint(f"`begin` will be cast to '{begin}'.")
            if end_is_int:
                end = datetime.fromtimestamp(int(end), timezone.utc).replace(tzinfo=None)
                if debug:
                    dprint(f"`end` will be cast to '{end}'.")

            begin = coerce_timezone(begin, strip_utc=(df_tz is None)) if begin is not None else None
            end = coerce_timezone(end, strip_utc=(df_tz is None)) if begin is not None else None

    in_ex_params = get_in_ex_params(params)

    masks = [
        (
            (df[datetime_column] >= begin)
            if begin is not None and datetime_column
            else True
        ) & (
            (df[datetime_column] < end)
            if end is not None and datetime_column
            else True
        )
    ]

    masks.extend([
        (
            (
                (df[col] if not coerce_types else df[col].astype(str)).isin(in_vals)
                if in_vals
                else True
            ) & (
                ~(df[col] if not coerce_types else df[col].astype(str)).isin(ex_vals)
                if ex_vals
                else True
            )
        )
        for col, (in_vals, ex_vals) in in_ex_params.items()
        if col in df.columns
    ])
    query_mask = masks[0]
    for mask in masks[1:]:
        query_mask = query_mask & mask

    original_cols = df.columns

    ### NOTE: We must cast bool columns to `boolean[pyarrow]`
    ###       to allow for `<NA>` values.
    bool_cols = [
        col
        for col, typ in df.dtypes.items()
        if are_dtypes_equal(str(typ), 'bool')
    ]
    for col in bool_cols:
        df[col] = df[col].astype('boolean[pyarrow]')

    if not isinstance(query_mask, bool):
        df['__mrsm_mask'] = (
            query_mask.astype('boolean[pyarrow]')
            if hasattr(query_mask, 'astype')
            else query_mask
        )

        if inplace:
            df.where(query_mask, other=NA, inplace=True)
            df.dropna(how='all', inplace=True)
            result_df = df
        else:
            result_df = df.where(query_mask, other=NA)
            result_df.dropna(how='all', inplace=True)

    else:
        result_df = df

    if '__mrsm_mask' in df.columns:
        del df['__mrsm_mask']
    if '__mrsm_mask' in result_df.columns:
        del result_df['__mrsm_mask']

    if reset_index:
        result_df.reset_index(drop=True, inplace=True)

    result_df = enforce_dtypes(
        result_df,
        dtypes,
        safe_copy=False,
        debug=debug,
        coerce_numeric=False,
        coerce_timezone=False,
    )

    if select_columns == ['*']:
        select_columns = None

    if not select_columns and not omit_columns:
        return result_df[original_cols]

    _process_select_columns(result_df)
    _process_omit_columns(result_df)

    return result_df


def to_json(
    df: 'pd.DataFrame',
    safe_copy: bool = True,
    orient: str = 'records',
    date_format: str = 'iso',
    date_unit: str = 'us',
    double_precision: int = 15,
    geometry_format: str = 'geojson',
    **kwargs: Any
) -> str:
    """
    Serialize the given dataframe as a JSON string.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame to be serialized.

    safe_copy: bool, default True
        If `False`, modify the DataFrame inplace.

    date_format: str, default 'iso'
        The default format for timestamps.

    date_unit: str, default 'us'
        The precision of the timestamps.

    double_precision: int, default 15
        The number of decimal places to use when encoding floating point values (maximum 15).

    geometry_format: str, default 'geojson'
        The serialization format for geometry data.
        Accepted values are `geojson`, `wkb_hex`, and `wkt`.

    Returns
    -------
    A JSON string.
    """
    import warnings
    import functools
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.dtypes import (
        serialize_bytes,
        serialize_decimal,
        serialize_geometry,
    )
    pd = import_pandas()
    uuid_cols = get_uuid_cols(df)
    bytes_cols = get_bytes_cols(df)
    numeric_cols = get_numeric_cols(df)
    geometry_cols = get_geometry_cols(df)
    if safe_copy and bool(uuid_cols or bytes_cols or geometry_cols or numeric_cols):
        df = df.copy()
    for col in uuid_cols:
        df[col] = df[col].astype(str)
    for col in bytes_cols:
        df[col] = df[col].apply(serialize_bytes)
    for col in numeric_cols:
        df[col] = df[col].apply(serialize_decimal)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for col in geometry_cols:
            df[col] = df[col].apply(
                functools.partial(
                    serialize_geometry,
                    geometry_format=geometry_format,
                )
            )
    return df.infer_objects(copy=False).fillna(pd.NA).to_json(
        date_format=date_format,
        date_unit=date_unit,
        double_precision=double_precision,
        orient=orient,
        **kwargs
    )
