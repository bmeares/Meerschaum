#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Enfore data types for a pipe's underlying table.
"""

from __future__ import annotations
from meerschaum.utils.typing import Dict, Any, Optional

def enforce_dtypes(self, df: 'pd.DataFrame', debug: bool=False) -> 'pd.DataFrame':
    """
    Cast the input dataframe to the pipe's registered data types.
    If the pipe does not exist and dtypes are not set, return the dataframe.

    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import parse_df_datetimes, enforce_dtypes as _enforce_dtypes
    from meerschaum.utils.packages import import_pandas
    pd = import_pandas(debug=debug)
    if df is None:
        if debug:
            dprint(
                f"Received None instead of a DataFrame.\n"
                + "    Skipping dtype enforcement..."
            )
        return df

    pipe_dtypes = self.dtypes

    try:
        if isinstance(df, str):
            df = parse_df_datetimes(
                pd.read_json(df),
                ignore_cols = [
                    col
                    for col, dtype in pipe_dtypes.items()
                    if 'datetime' not in str(dtype)
                ],
                debug = debug,
            )
        else:
            df = parse_df_datetimes(
                df,
                ignore_cols = [
                    col
                    for col, dtype in pipe_dtypes.items()
                    if 'datetime' not in str(dtype)
                ],
                debug = debug,
            )
    except Exception as e:
        warn(f"Unable to cast incoming data as a DataFrame...:\n{e}")
        return df

    if not pipe_dtypes:
        if debug:
            dprint(
                f"Could not find dtypes for {self}.\n"
                + "    Skipping dtype enforcement..."
            )
        return df

    return _enforce_dtypes(df, pipe_dtypes, debug=debug)


def infer_dtypes(self, persist: bool=False, debug: bool=False) -> Dict[str, Any]:
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
    columns_types = self.get_columns_types(debug=debug)
    dtypes = {
        c: get_pd_type(t, allow_custom_dtypes=True)
        for c, t in columns_types.items()
    } if columns_types else {}
    if persist:
        self.dtypes = dtypes
        self.edit(interactive=False, debug=debug)
    return dtypes
