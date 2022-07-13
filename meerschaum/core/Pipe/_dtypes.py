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
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.formatting import pprint
    from meerschaum.config.static import _static_config
    from meerschaum.utils.packages import import_pandas
    if df is None:
        if debug:
            dprint(
                f"Received None instead of a DataFrame.\n"
                + "    Skipping dtype enforcement..."
            )
        return df

    if not hasattr(df, 'dtypes'):
        pd = import_pandas(debug=debug)
        try:
            df = pd.DataFrame(df)
        except Exception as e:
            warn(f"Unable to cast incoming data as a DataFrame...")
            return df

    if not self.dtypes:
        if debug:
            dprint(
                f"Could not find dtypes for {self}.\n"
                + "    Skipping dtype enforcement..."
            )
        return df

    df_dtypes = {c: t.name for c, t in dict(df.dtypes).items()}
    if len(df_dtypes) == 0:
        if debug:
            dprint("Incoming DataFrame has no columns. Skipping enforcement...")
        return df

    if debug:
        dprint(f"Data types for {self}:")
        pprint(self.dtypes)
        dprint(f"Data types for incoming DataFrame:")
        pprint(df_dtypes)
    if df_dtypes == self.dtypes:
        if debug:
            dprint(f"Data types already match. Skipping enforcement...")
        return df

    common_dtypes = {}
    common_diff_dtypes = {}
    for d, t in self.dtypes.items():
        if d in df_dtypes:
            common_dtypes[d] = t
            if t != df_dtypes[d]:
                common_diff_dtypes[d] = df_dtypes[d]

    if debug:
        dprint(f"Common columns with different dtypes:")
        pprint(common_diff_dtypes)

    detected_dt_cols = {}
    for d, t in common_diff_dtypes.items():
        if 'datetime' in t and 'datetime' in common_dtypes[d]:
            df_dtypes[d] = t
            detected_dt_cols[d] = (common_dtypes[d], common_diff_dtypes[d])
    for d in detected_dt_cols:
        del common_diff_dtypes[d]

    if debug:
        dprint(f"Common columns with different dtypes (after dates):")
        pprint(common_diff_dtypes)

    if df_dtypes == self.dtypes:
        if debug:
            dprint(
                "The incoming DataFrame has mostly the same types as {self}, skipping enforcement."
                + f"The only detected difference was in the following datetime columns.\n"
                + "    Timezone information may be stripped."
            )
            pprint(detected_dt_cols)
        return df

    if len(common_dtypes) == len(df_dtypes):
        min_ratio = _static_config()['pipes']['dtypes']['min_ratio_columns_changed_for_full_astype']
        if (
            len(common_diff_dtypes) >= int(len(common_dtypes) * min_ratio)
        ):
            if debug:
                dprint(f"Enforcing data types for {self} on incoming DataFrame...")
            try:
                return df[list(common_dtypes.keys())].astype(self.dtypes)
            except Exception as e:
                warn(f"Encountered an error when enforcing data types for {self}:\n{e}")
                return df
    
    new_df = df.copy()
    for d in common_diff_dtypes:
        t = common_dtypes[d]
        if debug:
            dprint(f"Casting column {d} to dtype {t}.")
        try:
            new_df[d] = new_df[d].astype(t)
        except Exception as e:
            warn(f"Encountered an error when casting column {d} to type {t}:\n{e}")
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
        if 'datetime' in self.columns:
            dtypes[self.columns['datetime']] = 'datetime64[ns]'
        return dtypes
    from meerschaum.utils.sql import get_pd_type
    columns_types = self.get_columns_types(debug=debug)
    dtypes = {
        c: get_pd_type(t) for c, t in columns_types.items()
    } if columns_types else {}
    if persist:
        self.dtypes = dtypes
        self.edit(interactive=False, debug=debug)
    return dtypes
