#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Enforce data types for a pipe's underlying table.
"""

from __future__ import annotations
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
    dtypes: Optional[Dict[str, str]] = None,
    as_polars: Optional[bool] = None,
    debug: bool = False,
    as_pandas: Optional[bool] = None,
) -> 'pd.DataFrame':
    """
    Cast the input dataframe to the pipe's registered data types.
    If the pipe does not exist and dtypes are not set, return the dataframe.

    The result is the library `df` came in (a Polars frame stays Polars, a pandas frame
    pandas); documents, dictionaries and Arrow data come back in the library of the
    `meerschaum:dataframe` setting. `as_polars=True` / `as_pandas=True` ask for one library.
    """
    from meerschaum import _core
    from meerschaum.core.Pipe._data import _rust_context
    from meerschaum.core.Pipe._sync import is_accepted_data, unsupported_data_error
    from meerschaum.utils.dataframe import output_library_for, ensure_dataframe_library
    context = _rust_context(self, debug=debug)
    if df is not None and not is_accepted_data(df):
        raise unsupported_data_error(df, f"enforce the dtypes of {self}")
    library = output_library_for(df, as_polars=as_polars, as_pandas=as_pandas)
    if df is not None:
        ensure_dataframe_library(library)
    as_polars = library == 'polars'
    return _core.pipe_enforce_dtypes(
        self,
        context,
        df,
        enforce=enforce,
        safe_copy=safe_copy,
        dtypes=dtypes,
        as_polars=as_polars,
        debug=debug,
    )


def infer_dtypes(
    self,
    persist: bool = False,
    refresh: bool = False,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    If `dtypes` is not set in `meerschaum.Pipe.parameters`,
    infer the data types from the underlying table if it exists.

    Parameters
    ----------
    persist: bool, default False
        If `True`, persist the inferred data types to `meerschaum.Pipe.parameters`.
        NOTE: Use with caution! Generally `dtypes` is meant to be user-configurable only.

    refresh: bool, default False
        If `True`, retrieve the latest columns-types for the pipe.
        See `Pipe.get_columns.types()`.

    Returns
    -------
    A dictionary of strings containing the pandas data types for this Pipe.
    """
    from meerschaum import _core
    from meerschaum.core.Pipe._data import _rust_context
    context = _rust_context(self, debug=debug, refresh=refresh)
    remote_pd_dtypes = _core.pipe_infer_dtypes(self, context, refresh=refresh, debug=debug)
    ### A table that does not exist has no dtypes to persist.
    if not persist or not remote_pd_dtypes:
        return remote_pd_dtypes
    parameters = self.get_parameters(refresh=refresh, debug=debug)
    dtypes = parameters.get('dtypes', {})
    dtypes.update({
        col: typ
        for col, typ in remote_pd_dtypes.items()
        if col not in dtypes
    })
    self.dtypes = dtypes
    self.edit(interactive=False, debug=debug)
    return remote_pd_dtypes
