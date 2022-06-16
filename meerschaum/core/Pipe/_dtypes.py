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
    if not self.dtypes:

        ### No underlying table and no dtypes: do nothing.
        if not self.exists(debug=debug):
            return df
        
        inferred_dtypes = self.infer_dtypes(debug=debug)
        if not inferred_dtypes:
            return df
        self.dtypes = inferred_dtypes
        self.edit(interactive=False, debug=debug)

    common_dtypes = {}
    df_dtypes = dict(df.dtypes)
    for d, t in self.dtypes.items():
        if d in df_dtypes:
            common_dtypes[d] = t
    if len(common_dtypes) == len(df_dtypes):
        return df[list(common_dtypes.keys())].astype(self.dtypes)
    
    warn(
        f"Incoming dataframe does not have the same columns as {self}.\n"
        + f"    Got {df_dtypes},\n"
        + f"    but {self} has {self.dtypes}."
    )
    new_df = df.copy()
    for d, t in common_dtypes.items():
        new_df[d] = new_df[d].astype(t)
    return new_df[common_dtypes]


def infer_dtypes(self, update: bool=True, debug: bool=False) -> Dict[str, Any]:
    """
    If `dtypes` is not set in `meerschaum.Pipe.parameters`,
    infer the data types from the underlying table if it exists.

    Parameters
    ----------
    update: bool, default True
        If `True`, set
    """
    if self.dtypes:
        return self.dtypes
    if not self.exists(debug=debug):
        return {}
    from meerschaum.utils.sql import get_pd_type
    sample_df = self.get_backtrack_data(0, debug=debug)
    columns_types = self.get_columns_types(debug=debug)
    return {c: get_pd_type(t) for c, t in columns_types.items()}
