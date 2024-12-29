#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Enforce data types for a pipe's underlying table.
"""

from __future__ import annotations
from io import StringIO
import meerschaum as mrsm
from meerschaum.utils.typing import Dict, Any, Optional
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pd = mrsm.attempt_import('pandas')


def enforce_dtypes(
    self,
    df: 'pd.DataFrame',
    chunksize: Optional[int] = -1,
    enforce: bool = True,
    safe_copy: bool = True,
    debug: bool = False,
) -> 'pd.DataFrame':
    """
    Cast the input dataframe to the pipe's registered data types.
    If the pipe does not exist and dtypes are not set, return the dataframe.
    """
    import traceback
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.dataframe import parse_df_datetimes, enforce_dtypes as _enforce_dtypes
    from meerschaum.utils.dtypes import are_dtypes_equal
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas(debug=debug)
    if df is None:
        if debug:
            dprint(
                "Received None instead of a DataFrame.\n"
                + "    Skipping dtype enforcement..."
            )
        return df

    if not self.enforce:
        enforce = False
    pipe_dtypes = self.dtypes if enforce else {}

    try:
        if isinstance(df, str):
            df = parse_df_datetimes(
                pd.read_json(StringIO(df)),
                ignore_cols=[
                    col
                    for col, dtype in pipe_dtypes.items()
                    if (not enforce or not are_dtypes_equal(dtype, 'datetime'))
                ],
                ignore_all=(not enforce),
                strip_timezone=(self.tzinfo is None),
                chunksize=chunksize,
                debug=debug,
            )
        elif isinstance(df, (dict, list)):
            df = parse_df_datetimes(
                df,
                ignore_cols=[
                    col
                    for col, dtype in pipe_dtypes.items()
                    if (not enforce or not are_dtypes_equal(str(dtype), 'datetime'))
                ],
                strip_timezone=(self.tzinfo is None),
                chunksize=chunksize,
                debug=debug,
            )
    except Exception as e:
        warn(f"Unable to cast incoming data as a DataFrame...:\n{e}\n\n{traceback.format_exc()}")
        return None

    if not pipe_dtypes:
        if debug:
            dprint(
                f"Could not find dtypes for {self}.\n"
                + "    Skipping dtype enforcement..."
            )
        return df

    return _enforce_dtypes(
        df,
        pipe_dtypes,
        safe_copy=safe_copy,
        strip_timezone=(self.tzinfo is None),
        coerce_timezone=enforce,
        debug=debug,
    )


def infer_dtypes(self, persist: bool = False, debug: bool = False) -> Dict[str, Any]:
    """
    If `dtypes` is not set in `meerschaum.Pipe.parameters`,
    infer the data types from the underlying table if it exists.

    Parameters
    ----------
    persist: bool, default False
        If `True`, persist the inferred data types to `meerschaum.Pipe.parameters`.

    Returns
    -------
    A dictionary of strings containing the pandas data types for this Pipe.
    """
    if not self.exists(debug=debug):
        return {}

    from meerschaum.utils.dtypes.sql import get_pd_type_from_db_type
    from meerschaum.utils.dtypes import to_pandas_dtype

    ### NOTE: get_columns_types() may return either the types as
    ###       PostgreSQL- or Pandas-style.
    columns_types = self.get_columns_types(debug=debug)

    remote_pd_dtypes = {
        c: (
            get_pd_type_from_db_type(t, allow_custom_dtypes=True)
            if str(t).isupper()
            else to_pandas_dtype(t)
        )
        for c, t in columns_types.items()
    } if columns_types else {}
    if not persist:
        return remote_pd_dtypes

    dtypes = self.parameters.get('dtypes', {})
    dtypes.update({
        col: typ
        for col, typ in remote_pd_dtypes.items()
        if col not in dtypes
    })
    self.dtypes = dtypes
    self.edit(interactive=False, debug=debug)
    return remote_pd_dtypes
