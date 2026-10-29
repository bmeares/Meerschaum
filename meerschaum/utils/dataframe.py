#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for working with DataFrames.
"""

from __future__ import annotations

from datetime import datetime, date
from collections import defaultdict

import meerschaum as mrsm
from meerschaum.utils.typing import (
    Optional, Dict, Any, List, Hashable, Generator,
    Iterator, Iterable, Union, TYPE_CHECKING, Tuple,
)
from meerschaum import _core

if TYPE_CHECKING:
    pd, dask = mrsm.attempt_import('pandas', 'dask')


#: The DataFrame libraries Meerschaum returns frames in (`meerschaum:dataframe`).
DATAFRAME_LIBRARIES: Tuple[str, ...] = ('polars', 'pandas')

#: The environment variable that overrides `meerschaum:dataframe` for one process (and the jobs
#: and workers it starts).
DATAFRAME_ENV_VAR: str = 'MRSM_DATAFRAME'

#: The library of a root that does not say (a fresh root writes `dataframe: polars`).
DEFAULT_DATAFRAME_LIBRARY: str = 'polars'

#: The extra that installs each library.
DATAFRAME_EXTRAS: Dict[str, str] = {'polars': 'polars', 'pandas': 'pandas'}


def _normalize_dataframe_library(value: Any, source: str) -> str:
    """Return `'polars'` or `'pandas'` for a configured value, or raise naming `source`."""
    if value is None:
        return DEFAULT_DATAFRAME_LIBRARY
    text = str(value).strip().lower()
    if not text:
        return DEFAULT_DATAFRAME_LIBRARY
    if text not in DATAFRAME_LIBRARIES:
        raise ValueError(
            f"Unknown DataFrame library '{value}' ({source}): use 'polars' or 'pandas'."
        )
    return text


def get_dataframe_library() -> str:
    """
    Return the DataFrame library Meerschaum returns frames in: `'polars'` or `'pandas'`.

    The setting is `dataframe` in `meerschaum.yaml` (config path `meerschaum:dataframe`), and the
    `MRSM_DATAFRAME` environment variable overrides it for one process (and the jobs it starts).
    A fresh root writes `dataframe: polars`; a root created by Meerschaum 4 or by an earlier 5.0
    release candidate is given `dataframe: pandas` the first time 5.0.0rc15 or newer opens it.

    Every API that returns a DataFrame follows it (`Pipe.get_data()`, `SQLConnector.read()`,
    `Pipe.fetch()`, `Plan.to_dataframe()`, the frames handed to plugin code, ...), and
    `as_polars=True` / `as_pandas=True` override it for one call.

    Returns
    -------
    `'polars'` or `'pandas'`.

    Examples
    --------
    >>> import os
    >>> os.environ['MRSM_DATAFRAME'] = 'pandas'
    >>> get_dataframe_library()
    'pandas'
    """
    import os
    value = os.environ.get(DATAFRAME_ENV_VAR, None)
    if value is not None and value.strip():
        return _normalize_dataframe_library(value, DATAFRAME_ENV_VAR)
    from meerschaum.config import get_config
    return _normalize_dataframe_library(
        get_config('meerschaum', 'dataframe', warn=False),
        'meerschaum:dataframe',
    )


def resolve_dataframe_library(
    as_polars: Optional[bool] = None,
    as_pandas: Optional[bool] = None,
) -> str:
    """
    Return the library one call returns its frames in: `'polars'` or `'pandas'`.

    Parameters
    ----------
    as_polars: Optional[bool], default None
        `True` asks for Polars and `False` for pandas (Meerschaum 4's meaning of
        `as_polars=False`). `None` follows `get_dataframe_library()`.

    as_pandas: Optional[bool], default None
        `True` asks for pandas and `False` for Polars. `None` follows the setting.

    Returns
    -------
    `'polars'` or `'pandas'`. Asking for both (or for neither) raises a `ValueError`.
    """
    wants_polars = None if as_polars is None else bool(as_polars)
    wants_pandas = None if as_pandas is None else bool(as_pandas)
    if wants_polars is not None and wants_pandas is not None and wants_polars == wants_pandas:
        raise ValueError(
            "`as_polars` and `as_pandas` contradict each other: pass only one of them."
        )
    if wants_polars is not None:
        return 'polars' if wants_polars else 'pandas'
    if wants_pandas is not None:
        return 'pandas' if wants_pandas else 'polars'
    return get_dataframe_library()


def wants_polars(
    as_polars: Optional[bool] = None,
    as_pandas: Optional[bool] = None,
) -> bool:
    """Return whether one call returns Polars frames (see `resolve_dataframe_library()`)."""
    return resolve_dataframe_library(as_polars=as_polars, as_pandas=as_pandas) == 'polars'


def import_dataframe_library(library: Optional[str] = None) -> Any:
    """
    Import the `polars` or `pandas` module (the configured one by default).

    A missing library is installed into the `mrsm` virtual environment when automatic installs are
    allowed (as for any other optional package); otherwise the `ImportError` names the extra to
    install (`pip install meerschaum[polars]` or `pip install meerschaum[pandas]`).
    """
    library = library or get_dataframe_library()
    from meerschaum.utils.packages import attempt_import, import_pandas
    module = None
    try:
        if library == 'pandas':
            module = import_pandas(warn=False)
        else:
            module = attempt_import(library, lazy=False, warn=False)
    except Exception:
        module = None
    if module is None:
        raise missing_dataframe_library_error(library)
    return module


def ensure_dataframe_library(library: Optional[str] = None) -> Any:
    """
    Return the `polars` or `pandas` module (the configured one by default), importing it on the
    first call (`import_dataframe_library()`: installed on first use when automatic installs
    are allowed, else an `ImportError` naming the extra). Cheap once the module is imported.
    """
    import sys
    library = library or get_dataframe_library()
    module = sys.modules.get(library, None)
    if module is not None and not getattr(getattr(module, '__spec__', None), '_initializing', False):
        if library != 'pandas' or 'pyarrow' in sys.modules:
            return module
    return import_dataframe_library(library)


def missing_dataframe_library_error(library: str) -> ImportError:
    """The `ImportError` for a DataFrame library that is not installed (names the extra)."""
    extra = DATAFRAME_EXTRAS.get(library, library)
    other = 'pandas' if library == 'polars' else 'polars'
    return ImportError(
        f"Meerschaum returns {library} DataFrames here, but {library} is not installed. "
        f"Install it with `pip install meerschaum[{extra}]`, "
        f"or set `dataframe: {other}` in meerschaum.yaml (`MRSM_DATAFRAME={other}`)."
    )


def is_polars_frame(df: Any) -> bool:
    """Whether `df` is a Polars DataFrame or LazyFrame."""
    return (type(df).__module__ or '').split('.')[0] == 'polars'


def is_pandas_frame(df: Any) -> bool:
    """Whether `df` is a pandas (or GeoPandas) DataFrame."""
    return (
        (type(df).__module__ or '').split('.')[0] in ('pandas', 'geopandas')
        and hasattr(df, 'iloc')
    )


def frame_library_of(df: Any) -> Optional[str]:
    """`'polars'`, `'pandas'` or `None`: the library of a DataFrame (or `None` for other data)."""
    if is_polars_frame(df):
        return 'polars'
    if is_pandas_frame(df):
        return 'pandas'
    return None


def output_library_for(
    df: Any,
    as_polars: Optional[bool] = None,
    as_pandas: Optional[bool] = None,
) -> str:
    """
    The library a function that transforms a frame returns: what the call asks for
    (`as_polars` / `as_pandas`), else the library `df` came in (a Polars frame stays Polars, a
    pandas frame pandas), else (documents, dictionaries, Arrow data) `get_dataframe_library()`.
    """
    if as_polars is not None or as_pandas is not None:
        return resolve_dataframe_library(as_polars=as_polars, as_pandas=as_pandas)
    return frame_library_of(df) or get_dataframe_library()


def frame_from_arrow(
    data: Any,
    *,
    as_polars: Optional[bool] = None,
    as_pandas: Optional[bool] = None,
    dtypes: Optional[Dict[str, str]] = None,
    debug: bool = False,
) -> Any:
    """
    Build the configured library's DataFrame from Arrow data (the one conversion every
    frame-returning API uses).

    Parameters
    ----------
    data: Any
        Anything that implements the Arrow PyCapsule interface (`__arrow_c_stream__` or
        `__arrow_c_array__`): a `meerschaum._core.ArrowStream`, a `pyarrow.Table`,
        `RecordBatch` or `RecordBatchReader`, ... or a list (or tuple) of record batches with one
        schema. A pandas or Polars DataFrame is converted to the requested library.
        `None` returns `None`.

    as_polars: Optional[bool], default None
        `True` returns Polars, `False` pandas; `None` follows `get_dataframe_library()`.

    as_pandas: Optional[bool], default None
        `True` returns pandas, `False` Polars; `None` follows the setting.

    dtypes: Optional[Dict[str, str]], default None
        Per-column dtypes: Meerschaum dtype strings (`'int'`, `'json'`, `'geometry[Point, 4326]'`,
        ...) or pandas dtype strings (`'Int64'`, `'int64[pyarrow]'`, ...).
        For pandas they restore the exact pandas dtype within the column's kind, as every read of
        the façade does (`int64` vs `Int64` vs `int64[pyarrow]`, the datetime unit); a dtype of
        another kind does not cast the column. For Polars, declared `json` and `geometry`
        columns become extension columns even when every value is null.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `polars.DataFrame` or a `pandas.DataFrame` (a `geopandas.GeoDataFrame` when there are
    geometry columns): JSON, UUID, numeric and geometry columns are restored from the Arrow
    field metadata the kernel writes (`ARROW:extension:name`, `meerschaum:dtype`). Polars frames
    carry geometry as GeoArrow WKB (binary) with the CRS in the field metadata.

    Examples
    --------
    >>> import pyarrow as pa
    >>> frame_from_arrow(pa.table({'a': [1, None]}), as_pandas=True).dtypes
    a    Int64
    dtype: object
    >>> frame_from_arrow(pa.table({'a': [1, None]}), as_polars=True).schema
    Schema([('a', Int64)])
    """
    if data is None:
        return None
    library = resolve_dataframe_library(as_polars=as_polars, as_pandas=as_pandas)
    to_polars_frame = library == 'polars'
    current = frame_library_of(data)
    if current is not None:
        if current == library:
            return data
        return to_polars(data) if to_polars_frame else to_pandas(data)
    ensure_dataframe_library(library)
    if isinstance(data, (list, tuple)):
        data = list(data)
    _rust = _core.frame_from_arrow
    return _rust(
        data,
        as_polars=to_polars_frame,
        dtypes=({str(col): str(typ) for col, typ in dtypes.items()} if dtypes else None),
        debug=debug,
    )


def to_pandas(df: Any) -> Any:
    """Convert a Polars DataFrame or LazyFrame to Pandas; otherwise return ``df``."""
    if df.__class__.__module__.split('.')[0] != 'polars':
        return df
    if df.__class__.__name__ == 'LazyFrame':
        df = df.collect()
    json_cols = [
        col
        for col, typ in df.schema.items()
        if getattr(typ, 'ext_name', lambda: None)() == 'arrow.json'
    ]
    if json_cols:
        df = df.with_columns(df[col].ext.storage() for col in json_cols)
    pandas_df = df.to_pandas(use_pyarrow_extension_array=True)
    if json_cols:
        import json
        for col in json_cols:
            pandas_df[col] = pandas_df[col].apply(
                lambda value: json.loads(value) if isinstance(value, str) else None
            )
    return pandas_df


def to_polars(
    df: Any,
    geometry_cols_types_srids: Optional[Dict[str, Tuple[str, Any]]] = None,
    json_cols: Optional[List[str]] = None,
) -> Any:
    """Convert a Pandas DataFrame to Polars; otherwise return ``df``."""
    if df.__class__.__module__.split('.')[0] == 'polars':
        return df
    polars = mrsm.attempt_import('polars')
    geometry_cols_types_srids = (
        get_geometry_cols(df, with_types_srids=True)
        if geometry_cols_types_srids is None and get_geometry_cols(df)
        else (geometry_cols_types_srids or {})
    )
    geometry_cols_types_srids = {
        col: type_srid
        for col, type_srid in geometry_cols_types_srids.items()
        if col in df.columns
    }
    json_cols = [col for col in (json_cols or []) if col in df.columns]
    if geometry_cols_types_srids or json_cols:
        try:
            import json
            geometry_cols = list(geometry_cols_types_srids)
            special_cols = list(dict.fromkeys(geometry_cols + json_cols))
            polars_df = to_polars(
                df.drop(columns=special_cols),
                geometry_cols_types_srids={},
                json_cols=[],
            )
            if geometry_cols:
                from meerschaum.utils.dtypes import attempt_cast_to_geometry
                shapely = mrsm.attempt_import('shapely', lazy=False)
                ### Import inside the `mrsm` venv context: callers (e.g. `mrsm.entry()`)
                ### may have deactivated it, and a bare import would then silently
                ### skip registration so Arrow exports plain binary storage.
                try:
                    geoarrow_pyarrow = mrsm.attempt_import(
                        'geoarrow.types.type_pyarrow',
                        lazy=False,
                        warn=False,
                        install=False,
                    )
                    geoarrow_pyarrow.register_extension_types()
                except Exception:
                    pass
            for col, (_, srid) in geometry_cols_types_srids.items():
                crs = str(srid) if srid else None
                if crs and ':' not in crs:
                    crs = 'EPSG:' + crs
                metadata = json.dumps(
                    ({'crs': crs, 'crs_type': 'authority_code'} if crs else {}),
                    separators=(',', ':'),
                )
                polars_df = polars_df.with_columns(polars.Series(
                    col,
                    shapely.to_wkb(
                        attempt_cast_to_geometry(df[col]),
                        hex=False,
                        include_srid=True,
                    ).tolist(),
                    dtype=polars.Extension('geoarrow.wkb', polars.Binary, metadata),
                ))
            if json_cols:
                from meerschaum.utils.dtypes import json_serialize_value, value_is_null
                for col in json_cols:
                    values = []
                    for value in df[col].tolist():
                        if value_is_null(value):
                            values.append(None)
                            continue
                        if isinstance(value, str):
                            try:
                                value = json.loads(value)
                            except json.JSONDecodeError:
                                pass
                        values.append(json.dumps(
                            value,
                            default=json_serialize_value,
                            separators=(',', ':'),
                            allow_nan=False,
                        ))
                    polars_df = polars_df.with_columns(polars.Series(
                        col,
                        values,
                        dtype=polars.Extension('arrow.json', polars.String, ''),
                    ))
            return polars_df.select(list(df.columns))
        except Exception:
            # ponytail: Preserve compatibility if Polars changes its unstable extension API.
            pass
    if get_uuid_cols(df):
        return polars.DataFrame(df.to_dict(orient='list'), strict=False)
    try:
        return polars.from_pandas(df, include_index=False)
    except (TypeError, ValueError):
        # ponytail: Preserve unsupported Python objects; remove when Polars supports UUIDs.
        return polars.DataFrame(df.to_dict(orient='list'), strict=False)


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
    _rust = _core.filter_unseen_df
    return _rust(
        old_df, new_df=new_df, safe_copy=safe_copy, dtypes=dtypes,
        include_unchanged_columns=include_unchanged_columns,
        coerce_mixed_numerics=coerce_mixed_numerics, debug=debug,
    )


def parse_df_datetimes(
    df: 'pd.DataFrame',
    ignore_cols: Optional[Iterable[str]] = None,
    strip_timezone: bool = False,
    chunksize: Optional[int] = None,
    dtype_backend: str = 'numpy_nullable',
    ignore_all: bool = False,
    precision_unit: Optional[str] = None,
    coerce_utc: bool = True,
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

    precision_unit: Optional[str], default None
        If provided, enforce the given precision on the coerced datetime columns.

    coerce_utc: bool, default True
        Coerce the datetime columns to UTC (see `meerschaum.utils.dtypes.to_datetime()`).

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
    >>> df2 = parse_df_datetimes(df)
    >>> df2.dtypes
    a    datetime64[us, UTC]
    dtype: object

    ```

    """
    from meerschaum.utils.packages import import_pandas, attempt_import
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.misc import items_str
    from meerschaum.utils.dtypes import to_datetime, MRSM_PD_DTYPES
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
        return df.infer_objects().fillna(pandas.NA)

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
        return df.infer_objects().fillna(pandas.NA)

    if debug:
        dprint("Converting columns to datetimes: " + str(datetime_cols))

    def _parse_to_datetime(x):
        return to_datetime(x, precision_unit=precision_unit, coerce_utc=coerce_utc)

    try:
        if not using_dask:
            df[datetime_cols] = df[datetime_cols].apply(_parse_to_datetime)
        else:
            df[datetime_cols] = df[datetime_cols].apply(
                _parse_to_datetime,
                utc=True,
                axis=1,
                meta={
                    col: MRSM_PD_DTYPES['datetime']
                    for col in datetime_cols
                }
            )
    except Exception:
        warn(
            f"Unable to apply `to_datetime()` to {items_str(datetime_cols)}:\n"
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
    _rust = _core.get_json_cols
    return _rust(df)


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
    _rust = _core.get_numeric_cols
    return _rust(df)


def get_bool_cols(df: 'pd.DataFrame') -> List[str]:
    """
    Get the columns which contain `bool` objects from a Pandas DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame which may contain bools.

    Returns
    -------
    A list of columns to treat as bools.
    """
    if df is None:
        return []

    is_dask = 'dask' in df.__module__
    if is_dask:
        df = get_first_valid_dask_partition(df)

    if len(df) == 0:
        return []

    from meerschaum.utils.dtypes import are_dtypes_equal

    return [
        col
        for col, typ in df.dtypes.items()
        if are_dtypes_equal(str(typ), 'bool')
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
    _rust = _core.get_uuid_cols
    return _rust(df)


def get_datetime_cols(
    df: 'pd.DataFrame',
    timezone_aware: bool = True,
    timezone_naive: bool = True,
    with_tz_precision: bool = False,
) -> Union[List[str], Dict[str, Tuple[Union[str, None], str]]]:
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

    with_tz_precision: bool, default False
        If `True`, return a dictionary mapping column names to tuples in the form
        `(timezone, precision)`.

    Returns
    -------
    A list of columns to treat as datetimes, or a dictionary of columns to tz+precision tuples
    (if `with_tz_precision` is `True`).
    """
    _rust = _core.get_datetime_cols
    return _rust(
        df, timezone_aware=timezone_aware, timezone_naive=timezone_naive,
        with_tz_precision=with_tz_precision,
    )


def get_datetime_cols_types(df: 'pd.DataFrame') -> Dict[str, str]:
    """
    Return a dictionary mapping datetime columns to specific types strings.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame which may contain datetime columns.

    Returns
    -------
    A dictionary mapping the datetime columns' names to dtype strings
    (containing timezone and precision metadata).

    Examples
    --------
    >>> from datetime import datetime, timezone
    >>> import pandas as pd
    >>> df = pd.DataFrame({'dt_tz_aware': [datetime(2025, 1, 1, tzinfo=timezone.utc)]})
    >>> get_datetime_cols_types(df)
    {'dt_tz_aware': 'datetime64[us, UTC]'}
    >>> df = pd.DataFrame({'distant_dt': [datetime(1, 1, 1)]})
    >>> get_datetime_cols_types(df)
    {'distant_dt': 'datetime64[us]'}
    >>> df = pd.DataFrame({'dt_second': datetime(2025, 1, 1)})
    >>> df['dt_second'] = df['dt_second'].astype('datetime64[s]')
    >>> get_datetime_cols_types(df)
    {'dt_second': 'datetime64[s]'}
    """
    from meerschaum.utils.dtypes import MRSM_PRECISION_UNITS_ABBREVIATIONS
    dt_cols_tuples = get_datetime_cols(df, with_tz_precision=True)
    if not dt_cols_tuples:
        return {}

    return {
        col: (
            f"datetime64[{MRSM_PRECISION_UNITS_ABBREVIATIONS[precision]}]"
            if tz is None
            else f"datetime64[{MRSM_PRECISION_UNITS_ABBREVIATIONS[precision]}, {tz}]"
        )
        for col, (tz, precision) in dt_cols_tuples.items()
    }


def get_date_cols(df: 'pd.DataFrame') -> List[str]:
    """
    Get the `date` columns from a Pandas DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame which may contain dates.

    Returns
    -------
    A list of columns to treat as dates.
    """
    from meerschaum.utils.dtypes import are_dtypes_equal
    if df is None:
        return []

    is_dask = 'dask' in df.__module__
    if is_dask:
        df = get_first_valid_dask_partition(df)

    known_date_cols = [
        col
        for col, typ in df.dtypes.items()
        if are_dtypes_equal(typ, 'date')
    ]

    if len(df) == 0:
        return known_date_cols

    cols_indices = {
        col: df[col].first_valid_index()
        for col in df.columns
        if col not in known_date_cols
    }
    object_date_cols = [
        col
        for col, ix in cols_indices.items()
        if (
            ix is not None
            and isinstance(df.loc[ix][col], date)
        )
    ]

    all_date_cols = set(known_date_cols + object_date_cols)

    return [
        col
        for col in df.columns
        if col in all_date_cols
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
    _rust = _core.get_bytes_cols
    return _rust(df)


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
    _rust = _core.get_geometry_cols
    return _rust(df, with_types_srids=with_types_srids)


def get_geometry_cols_types(df: 'pd.DataFrame') -> Dict[str, str]:
    """
    Return a dtypes dictionary mapping columns to specific geometry types (type, srid).
    """
    geometry_cols_types_srids = get_geometry_cols(df, with_types_srids=True)
    new_cols_types = {}
    for col, (geometry_type, srid) in geometry_cols_types_srids.items():
        new_dtype = "geometry"
        modifier = ""
        if not srid and geometry_type.lower() == 'geometry':
            new_cols_types[col] = new_dtype
            continue

        modifier = "["
        if geometry_type.lower() != 'geometry':
            modifier += f"{geometry_type}"

        if srid:
            if modifier != '[':
                modifier += ", "
            modifier += f"{srid}"
        modifier += "]"
        new_cols_types[col] = f"{new_dtype}{modifier}"
    return new_cols_types


def get_special_cols(df: 'pd.DataFrame') -> Dict[str, str]:
    """
    Return a dtypes dictionary mapping special columns to their dtypes.
    """
    return {
        **{col: 'json' for col in get_json_cols(df)},
        **{col: 'uuid' for col in get_uuid_cols(df)},
        **{col: 'bytes' for col in get_bytes_cols(df)},
        **{col: 'bool' for col in get_bool_cols(df)},
        **{col: 'numeric' for col in get_numeric_cols(df)},
        **{col: 'date' for col in get_date_cols(df)},
        **get_datetime_cols_types(df),
        **get_geometry_cols_types(df),
    }


def enforce_dtypes(
    df: 'pd.DataFrame',
    dtypes: Dict[str, str],
    explicit_dtypes: Optional[Dict[str, str]] = None,
    safe_copy: bool = True,
    coerce_numeric: bool = False,
    coerce_timezone: bool = True,
    strip_timezone: bool = False,
    as_polars: Optional[bool] = None,
    debug: bool = False,
    as_pandas: Optional[bool] = None,
) -> 'pd.DataFrame':
    """
    Enforce the `dtypes` dictionary on a DataFrame.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame on which to enforce dtypes.

    dtypes: Dict[str, str]
        The data types to attempt to enforce on the DataFrame.

    explicit_dtypes: Optional[Dict[str, str]], default None
        If provided, automatic dtype coersion will respect explicitly configured
        dtypes (`int`, `float`, `numeric`).

    safe_copy: bool, default True
        If `True`, create a copy before comparing and modifying the dataframes.
        Setting to `False` may mutate the DataFrames.
        See `meerschaum.utils.dataframe.filter_unseen_df`.

    coerce_numeric: bool, default False
        If `True`, convert float and int collisions to numeric.

    coerce_timezone: bool, default True
        If `True`, convert datetimes to UTC.

    strip_timezone: bool, default False
        If `coerce_timezone` and `strip_timezone` are `True`,
        remove timezone information from datetimes.

    as_polars: Optional[bool], default None
        `True` returns a Polars DataFrame, `False` a pandas one. By default the result is the
        library `df` came in (a Polars frame stays Polars, a pandas frame pandas).

    debug: bool, default False
        Verbosity toggle.

    as_pandas: Optional[bool], default None
        `True` returns a pandas DataFrame, `False` a Polars one (see `as_polars`).

    Returns
    -------
    The DataFrame with the types enforced, of the library `df` came in (or the one asked for).
    """
    if df is None:
        return df
    library = output_library_for(df, as_polars=as_polars, as_pandas=as_pandas)
    _rust = _core.enforce_dtypes
    return _rust(
        df, dtypes=dtypes, explicit_dtypes=explicit_dtypes, safe_copy=safe_copy,
        coerce_numeric=coerce_numeric, coerce_timezone=coerce_timezone,
        strip_timezone=strip_timezone, as_polars=(library == 'polars'), debug=debug,
    )


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
    literal: Optional[str] = None,
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
    from meerschaum.utils.dtypes import get_current_timestamp

    if pipe is None or literal is None:
        error("Please provide a Pipe and a literal value")

    dt_col = pipe.columns.get(
        'datetime',
        mrsm.get_config('pipes', 'autotime', 'column_name_if_datetime_missing')
    )
    val_col = pipe.get_val_column(debug=debug)

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

    now = get_current_timestamp(pipe.precision)
    pd = import_pandas()
    return pd.DataFrame({dt_col: [now], val_col: [val]})


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
    _rust = _core.query_df
    return _rust(
        df, params=params, begin=begin, end=end, datetime_column=datetime_column,
        select_columns=select_columns, omit_columns=omit_columns, inplace=inplace,
        reset_index=reset_index, coerce_types=coerce_types, debug=debug,
    )


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
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.dtypes import (
        serialize_bytes,
        serialize_decimal,
        serialize_geometry,
        serialize_date,
    )
    pd = import_pandas()
    uuid_cols = get_uuid_cols(df)
    bytes_cols = get_bytes_cols(df)
    numeric_cols = get_numeric_cols(df)
    date_cols = get_date_cols(df)
    geometry_cols = get_geometry_cols(df)
    geometry_cols_srids = {
        col: int((getattr(df[col].crs, 'srs', '') or '').split(':', maxsplit=1)[-1] or '0')
        for col in geometry_cols
    } if 'geodataframe' in str(type(df)).lower() else {}
    if safe_copy and bool(uuid_cols or bytes_cols or geometry_cols or numeric_cols):
        df = df.copy()
    if 'geodataframe' in str(type(df)).lower():
        geometry_data = {
            col: df[col]
            for col in geometry_cols
        }
        df = pd.DataFrame({
            col: df[col]
            for col in df.columns
            if col not in geometry_cols
        })
        for col in geometry_cols:
            df[col] = pd.Series(ob for ob in geometry_data[col])
    for col in uuid_cols:
        df[col] = df[col].astype(str)
    for col in bytes_cols:
        df[col] = df[col].apply(serialize_bytes)
    for col in numeric_cols:
        df[col] = df[col].apply(serialize_decimal)
    for col in date_cols:
        df[col] = df[col].apply(serialize_date)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for col in geometry_cols:
            srid = geometry_cols_srids.get(col, None) or None
            df[col] = pd.Series(
                serialize_geometry(val, geometry_format=geometry_format, srid=srid)
                for val in df[col]
            )
    return df.infer_objects().fillna(pd.NA).to_json(
        date_format=date_format,
        date_unit=date_unit,
        double_precision=double_precision,
        orient=orient,
        **kwargs
    )


def to_simple_lines(df: 'pd.DataFrame') -> str:
    """
    Serialize a Pandas Dataframe as lines of simple dictionaries.

    Parameters
    ----------
    df: pd.DataFrame
        The dataframe to serialize into simple lines text.

    Returns
    -------
    A string of simple line dictionaries joined by newlines.
    """
    from meerschaum.utils.misc import to_simple_dict
    if df is None or len(df) == 0:
        return ''

    docs = df.to_dict(orient='records')
    return '\n'.join(to_simple_dict(doc) for doc in docs)


def parse_simple_lines(data: str) -> 'pd.DataFrame':
    """
    Parse simple lines text into a DataFrame.

    Parameters
    ----------
    data: str
        The simple lines text to parse into a DataFrame.

    Returns
    -------
    A dataframe containing the rows serialized in `data`.
    """
    from meerschaum.utils.misc import string_to_dict
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas()
    lines = data.splitlines()
    try:
        docs = [string_to_dict(line) for line in lines]
        df = pd.DataFrame(docs)
    except Exception:
        df = None

    if df is None:
        raise ValueError("Cannot parse simple lines into a dataframe.")

    return df
