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
    import json
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.formatting import pprint
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.misc import parse_df_datetimes, to_pandas_dtype
    pd = import_pandas(debug=debug)
    if df is None:
        if debug:
            dprint(
                f"Received None instead of a DataFrame.\n"
                + "    Skipping dtype enforcement..."
            )
        return df

    try:
        if isinstance(df, str):
            df = parse_df_datetimes(pd.read_json(df), debug=debug)
        else:
            df = parse_df_datetimes(df, debug=debug)
    except Exception as e:
        warn(f"Unable to cast incoming data as a DataFrame...:\n{e}")
        return df

    if not self.dtypes:
        if debug:
            dprint(
                f"Could not find dtypes for {self}.\n"
                + "    Skipping dtype enforcement..."
            )
        return df

    df_dtypes = {c: str(t) for c, t in df.dtypes.items()}
    if len(df_dtypes) == 0:
        if debug:
            dprint("Incoming DataFrame has no columns. Skipping enforcement...")
        return df

    pipe_dtypes = self.dtypes
    pipe_pandas_dtypes = {
        col: to_pandas_dtype(typ)
        for col, typ in self.dtypes.items()
    }
    json_cols = [
        col
        for col, typ in pipe_dtypes.items()
        if typ == 'json'
    ]
    if debug:
        dprint(f"Data types for {self}:")
        pprint(pipe_dtypes)
        dprint(f"Data types for incoming DataFrame:")
        pprint(df_dtypes)

    if json_cols and len(df) > 0:
        if debug:
            dprint(f"Checking columns for JSON encoding: {json_cols}")
        for col in json_cols:
            if col in df.columns and isinstance(df.iloc[0][col], str):
                try:
                    df[col] = df[col].apply(json.loads)
                except Exception as e:
                    if debug:
                        dprint(f"Unable to parse column '{col}' as JSON:\n{e}")

    if df_dtypes == pipe_pandas_dtypes:
        if debug:
            dprint(f"Data types match. Exiting enforcement...")
        return df

    common_dtypes = {}
    common_diff_dtypes = {}
    for col, typ in pipe_pandas_dtypes.items():
        if col in df_dtypes:
            common_dtypes[col] = typ
            if typ != df_dtypes[col]:
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

    if df_dtypes == pipe_pandas_dtypes:
        if debug:
            dprint(
                "The incoming DataFrame has mostly the same types as {self}, skipping enforcement."
                + f"The only detected difference was in the following datetime columns.\n"
                + "    Timezone information may be stripped."
            )
            pprint(detected_dt_cols)
        return df

    if set(common_dtypes) == set(df_dtypes):
        min_ratio = STATIC_CONFIG['pipes']['dtypes']['min_ratio_columns_changed_for_full_astype']
        if (
            len(common_diff_dtypes) >= int(len(common_dtypes) * min_ratio)
        ):
            if debug:
                dprint(f"Enforcing data types for {self} on incoming DataFrame...")
            try:
                return df[
                    list(common_dtypes.keys())
                ].astype({
                    col: typ
                    for col, typ in pipe_pandas_dtypes.items()
                    if col in common_dtypes
                })
            except Exception as e:
                if debug:
                    dprint(f"Encountered an error when enforcing data types for {self}:\n{e}")
    
    new_df = df.copy()
    for d in common_diff_dtypes:
        t = common_dtypes[d]
        if debug:
            dprint(f"Casting column {d} to dtype {t}.")
        try:
            new_df[d] = new_df[d].astype(t)
        except Exception as e:
            if debug:
                dprint(f"Encountered an error when casting column {d} to type {t}:\n{e}")
            if 'int' in str(t.lower()):
                try:
                    new_df[d] = new_df[d].astype('float64').astype(t)
                except Exception as e:
                    if debug:
                        dprint(f"Was unable to convert to float then {t}.")
    return new_df


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
