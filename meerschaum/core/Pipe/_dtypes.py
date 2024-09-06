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
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas(debug=debug)
    if df is None:
        if debug:
            dprint(
                "Received None instead of a DataFrame.\n"
                + "    Skipping dtype enforcement..."
            )
        return df

    pipe_dtypes = self.dtypes

    try:
        if isinstance(df, str):
            df = parse_df_datetimes(
                pd.read_json(StringIO(df)),
                ignore_cols=[
                    col
                    for col, dtype in pipe_dtypes.items()
                    if 'datetime' not in str(dtype)
                ],
                chunksize=chunksize,
                debug=debug,
            )
        else:
            df = parse_df_datetimes(
                df,
                ignore_cols=[
                    col
                    for col, dtype in pipe_dtypes.items()
                    if 'datetime' not in str(dtype)
                ],
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

    return _enforce_dtypes(df, pipe_dtypes, safe_copy=safe_copy, debug=debug)


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
        dtypes = {}
        if not self.columns:
            return {}
        dt_col = self.columns.get('datetime', None)
        if dt_col:
            if not self.parameters.get('dtypes', {}).get(dt_col, None):
                dtypes[dt_col] = 'datetime64[ns]'
        return dtypes

    from meerschaum.utils.sql import get_pd_type
    from meerschaum.utils.misc import to_pandas_dtype
    columns_types = self.get_columns_types(debug=debug)

    ### NOTE: get_columns_types() may return either the types as
    ###       PostgreSQL- or Pandas-style.
    dtypes = {
        c: (
            get_pd_type(t, allow_custom_dtypes=True)
            if str(t).isupper()
            else to_pandas_dtype(t)
        )
        for c, t in columns_types.items()
    } if columns_types else {}
    if persist:
        self.dtypes = dtypes
        self.edit(interactive=False, debug=debug)
    return dtypes
