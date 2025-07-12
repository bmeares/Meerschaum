#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch and manipulate Pipes' attributes
"""

from __future__ import annotations

import uuid
from datetime import timezone

import meerschaum as mrsm
from meerschaum.utils.typing import Tuple, Dict, Any, Union, Optional, List
from meerschaum.utils.warnings import warn


@property
def attributes(self) -> Dict[str, Any]:
    """
    Return a dictionary of a pipe's keys and parameters.
    These values are reflected directly from the pipes table of the instance.
    """
    import time
    from meerschaum.config import get_config
    from meerschaum.config._patch import apply_patch_to_config
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    timeout_seconds = get_config('pipes', 'attributes', 'local_cache_timeout_seconds')

    if '_attributes' not in self.__dict__:
        self._attributes = {}

    now = time.perf_counter()
    last_refresh = self.__dict__.get('_attributes_sync_time', None)
    timed_out = (
        last_refresh is None
        or
        (timeout_seconds is not None and (now - last_refresh) >= timeout_seconds)
    )
    if not self.temporary and timed_out:
        self._attributes_sync_time = now
        local_attributes = self.__dict__.get('_attributes', {})
        with Venv(get_connector_plugin(self.instance_connector)):
            instance_attributes = self.instance_connector.get_pipe_attributes(self)
        self._attributes = apply_patch_to_config(instance_attributes, local_attributes)
    return self._attributes


def get_parameters(
    self,
    apply_symlinks: bool = True,
    _visited: 'Optional[set[mrsm.Pipe]]' = None,
) -> Dict[str, Any]:
    """
    Return the `parameters` dictionary of the pipe.
    """
    from meerschaum.config._patch import apply_patch_to_config
    from meerschaum.utils.warnings import warn
    from meerschaum.config._read_config import search_and_substitute_config

    if _visited is None:
        _visited = {self}

    raw_parameters = self.attributes.get('parameters', {})
    ref_keys = raw_parameters.get('reference')
    if not apply_symlinks:
        return raw_parameters

    if ref_keys:
        try:
            ref_pipe = mrsm.Pipe(**ref_keys)
            if ref_pipe in _visited:
                warn(f"Circular reference detected in {self}: chain involves {ref_pipe}.")
                return search_and_substitute_config(raw_parameters)

            _visited.add(ref_pipe)
            base_params = ref_pipe.get_parameters(_visited=_visited)
        except Exception as e:
            warn(f"Failed to resolve reference pipe for {self}: {e}")
            base_params = {}

        params_to_apply = {k: v for k, v in raw_parameters.items() if k != 'reference'}
        parameters = apply_patch_to_config(base_params, params_to_apply)
    else:
        parameters = raw_parameters

    from meerschaum.utils.pipes import replace_pipes_syntax
    self._symlinks = {}

    def recursive_replace(obj: Any, path: tuple) -> Any:
        if isinstance(obj, dict):
            return {k: recursive_replace(v, path + (k,)) for k, v in obj.items()}
        if isinstance(obj, list):
            return [recursive_replace(elem, path + (i,)) for i, elem in enumerate(obj)]
        if isinstance(obj, str):
            substituted_val = replace_pipes_syntax(obj)
            if substituted_val != obj:
                self._symlinks[path] = {
                    'original': obj,
                    'substituted': substituted_val,
                }
            return substituted_val
        return obj

    return search_and_substitute_config(recursive_replace(parameters, tuple()))


@property
def parameters(self) -> Optional[Dict[str, Any]]:
    """
    Return the parameters dictionary of the pipe.
    """
    if (_parameters := self.__dict__.get('_parameters', None)) is not None:
        return _parameters
    self._parameters = self.get_parameters()
    return self._parameters


@parameters.setter
def parameters(self, parameters: Dict[str, Any]) -> None:
    """
    Set the parameters dictionary of the in-memory pipe.
    Call `meerschaum.Pipe.edit()` to persist changes.
    """
    self._attributes['parameters'] = parameters
    if '_parameters' in self.__dict__:
        del self.__dict__['_parameters']


@property
def columns(self) -> Union[Dict[str, str], None]:
    """
    Return the `columns` dictionary defined in `meerschaum.Pipe.parameters`.
    """
    cols = self.parameters.get('columns', {})
    if not isinstance(cols, dict):
        return {}
    return {col_ix: col for col_ix, col in cols.items() if col}


@columns.setter
def columns(self, _columns: Union[Dict[str, str], List[str]]) -> None:
    """
    Override the columns dictionary of the in-memory pipe.
    Call `meerschaum.Pipe.edit()` to persist changes.
    """
    if isinstance(_columns, (list, tuple)):
        _columns = {col: col for col in _columns}
    if not isinstance(_columns, dict):
        warn(f"{self}.columns must be a dictionary, received {type(_columns)}.")
        return
    self.update_parameters({'columns': _columns}, persist=False)


@property
def indices(self) -> Union[Dict[str, Union[str, List[str]]], None]:
    """
    Return the `indices` dictionary defined in `meerschaum.Pipe.parameters`.
    """
    indices_key = (
        'indexes'
        if 'indexes' in self.parameters
        else 'indices'
    )
    if indices_key not in self.parameters:
        self.parameters[indices_key] = {}
    _indices = self.parameters[indices_key]
    _columns = self.columns
    dt_col = _columns.get('datetime', None)
    if not isinstance(_indices, dict):
        _indices = {}
        self.parameters[indices_key] = _indices
    unique_cols = list(set((
        [dt_col]
        if dt_col
        else []
    ) + [
        col
        for col_ix, col in _columns.items()
        if col and col_ix != 'datetime'
    ]))
    return {
        **({'unique': unique_cols} if len(unique_cols) > 1 else {}),
        **{col_ix: col for col_ix, col in _columns.items() if col},
        **_indices
    }


@property
def indexes(self) -> Union[Dict[str, Union[str, List[str]]], None]:
    """
    Alias for `meerschaum.Pipe.indices`.
    """
    return self.indices


@indices.setter
def indices(self, _indices: Union[Dict[str, Union[str, List[str]]], List[str]]) -> None:
    """
    Override the indices dictionary of the in-memory pipe.
    Call `meerschaum.Pipe.edit()` to persist changes.
    """
    if not isinstance(_indices, dict):
        warn(f"{self}.indices must be a dictionary, received {type(_indices)}.")
        return
    indices_key = (
        'indexes'
        if 'indexes' in self.parameters
        else 'indices'
    )
    self.update_parameters({indices_key: _indices}, persist=False)


@indexes.setter
def indexes(self, _indexes: Union[Dict[str, Union[str, List[str]]], List[str]]) -> None:
    """
    Alias for `meerschaum.Pipe.indices`.
    """
    self.indices = _indexes


@property
def tags(self) -> Union[List[str], None]:
    """
    If defined, return the `tags` list defined in `meerschaum.Pipe.parameters`.
    """
    return self.parameters.get('tags', [])


@tags.setter
def tags(self, _tags: List[str]) -> None:
    """
    Override the tags list of the in-memory pipe.
    Call `meerschaum.Pipe.edit` to persist changes.
    """
    from meerschaum.utils.warnings import error
    from meerschaum._internal.static import STATIC_CONFIG
    negation_prefix = STATIC_CONFIG['system']['fetch_pipes_keys']['negation_prefix']
    for t in _tags:
        if t.startswith(negation_prefix):
            error(f"Tags cannot begin with '{negation_prefix}'.")
    self.update_parameters({'tags': _tags}, persist=False)


@property
def dtypes(self) -> Union[Dict[str, Any], None]:
    """
    If defined, return the `dtypes` dictionary defined in `meerschaum.Pipe.parameters`.
    """
    from meerschaum.config._patch import apply_patch_to_config
    from meerschaum.utils.dtypes import MRSM_ALIAS_DTYPES
    configured_dtypes = self.parameters.get('dtypes', {})
    remote_dtypes = self.infer_dtypes(persist=False)
    patched_dtypes = apply_patch_to_config(remote_dtypes, configured_dtypes)
    dt_col = self.columns.get('datetime', None)
    primary_col = self.columns.get('primary', None)
    _dtypes = {
        col: MRSM_ALIAS_DTYPES.get(typ, typ)
        for col, typ in patched_dtypes.items()
        if col and typ
    }
    if dt_col and dt_col not in _dtypes:
        _dtypes[dt_col] = 'datetime'
    if primary_col and self.autoincrement and primary_col not in _dtypes:
        _dtypes[primary_col] = 'int'
    return _dtypes


@dtypes.setter
def dtypes(self, _dtypes: Dict[str, Any]) -> None:
    """
    Override the dtypes dictionary of the in-memory pipe.
    Call `meerschaum.Pipe.edit()` to persist changes.
    """
    self.update_parameters({'dtypes': _dtypes}, persist=False)


@property
def upsert(self) -> bool:
    """
    Return whether `upsert` is set for the pipe.
    """
    return self.parameters.get('upsert', False)


@upsert.setter
def upsert(self, _upsert: bool) -> None:
    """
    Set the `upsert` parameter for the pipe.
    """
    self.update_parameters({'upsert': _upsert}, persist=False)


@property
def static(self) -> bool:
    """
    Return whether `static` is set for the pipe.
    """
    return self.parameters.get('static', False)


@static.setter
def static(self, _static: bool) -> None:
    """
    Set the `static` parameter for the pipe.
    """
    self.update_parameters({'static': _static}, persist=False)


@property
def autoincrement(self) -> bool:
    """
    Return the `autoincrement` parameter for the pipe.
    """
    return self.parameters.get('autoincrement', False)


@autoincrement.setter
def autoincrement(self, _autoincrement: bool) -> None:
    """
    Set the `autoincrement` parameter for the pipe.
    """
    self.update_parameters({'autoincrement': _autoincrement}, persist=False)


@property
def autotime(self) -> bool:
    """
    Return the `autotime` parameter for the pipe.
    """
    return self.parameters.get('autotime', False)


@autotime.setter
def autotime(self, _autotime: bool) -> None:
    """
    Set the `autotime` parameter for the pipe.
    """
    self.update_parameters({'autotime': _autotime}, persist=False)


@property
def tzinfo(self) -> Union[None, timezone]:
    """
    Return `timezone.utc` if the pipe is timezone-aware.
    """
    dt_col = self.columns.get('datetime', None)
    if not dt_col:
        return None

    dt_typ = str(self.dtypes.get(dt_col, 'datetime64[ns, UTC]'))
    if 'utc' in dt_typ.lower() or dt_typ == 'datetime':
        return timezone.utc

    if dt_typ == 'datetime64[ns]':
        return None

    return None


@property
def enforce(self) -> bool:
    """
    Return the `enforce` parameter for the pipe.
    """
    return self.parameters.get('enforce', True)


@enforce.setter
def enforce(self, _enforce: bool) -> None:
    """
    Set the `enforce` parameter for the pipe.
    """
    self.update_parameters({'enforce': _enforce}, persist=False)


@property
def null_indices(self) -> bool:
    """
    Return the `null_indices` parameter for the pipe.
    """
    return self.parameters.get('null_indices', True)


@null_indices.setter
def null_indices(self, _null_indices: bool) -> None:
    """
    Set the `null_indices` parameter for the pipe.
    """
    self.update_parameters({'null_indices': _null_indices}, persist=False)


def get_columns(self, *args: str, error: bool = False) -> Union[str, Tuple[str]]:
    """
    Check if the requested columns are defined.

    Parameters
    ----------
    *args: str
        The column names to be retrieved.

    error: bool, default False
        If `True`, raise an `Exception` if the specified column is not defined.

    Returns
    -------
    A tuple of the same size of `args` or a `str` if `args` is a single argument.

    Examples
    --------
    >>> pipe = mrsm.Pipe('test', 'test')
    >>> pipe.columns = {'datetime': 'dt', 'id': 'id'}
    >>> pipe.get_columns('datetime', 'id')
    ('dt', 'id')
    >>> pipe.get_columns('value', error=True)
    Exception:  🛑 Missing 'value' column for Pipe('test', 'test').
    """
    from meerschaum.utils.warnings import error as _error
    if not args:
        args = tuple(self.columns.keys())
    col_names = []
    for col in args:
        col_name = None
        try:
            col_name = self.columns[col]
            if col_name is None and error:
                _error(f"Please define the name of the '{col}' column for {self}.")
        except Exception:
            col_name = None
        if col_name is None and error:
            _error(f"Missing '{col}'" + f" column for {self}.")
        col_names.append(col_name)
    if len(col_names) == 1:
        return col_names[0]
    return tuple(col_names)


def get_columns_types(
    self,
    refresh: bool = False,
    debug: bool = False,
) -> Union[Dict[str, str], None]:
    """
    Get a dictionary of a pipe's column names and their types.

    Parameters
    ----------
    refresh: bool, default False
        If `True`, invalidate the cache and fetch directly from the instance connector.

    debug: bool, default False:
        Verbosity toggle.

    Returns
    -------
    A dictionary of column names (`str`) to column types (`str`).

    Examples
    --------
    >>> pipe.get_columns_types()
    {
      'dt': 'TIMESTAMP WITH TIMEZONE',
      'id': 'BIGINT',
      'val': 'DOUBLE PRECISION',
    }
    >>>
    """
    import time
    from meerschaum.connectors import get_connector_plugin
    from meerschaum._internal.static import STATIC_CONFIG
    from meerschaum.utils.warnings import dprint

    now = time.perf_counter()
    cache_seconds = STATIC_CONFIG['pipes']['static_schema_cache_seconds']
    if not self.static:
        refresh = True
    if refresh:
        _ = self.__dict__.pop('_columns_types_timestamp', None)
        _ = self.__dict__.pop('_columns_types', None)
    _columns_types = self.__dict__.get('_columns_types', None)
    if _columns_types:
        columns_types_timestamp = self.__dict__.get('_columns_types_timestamp', None)
        if columns_types_timestamp is not None:
            delta = now - columns_types_timestamp
            if delta < cache_seconds:
                if debug:
                    dprint(
                        f"Returning cached `columns_types` for {self} "
                        f"({round(delta, 2)} seconds old)."
                    )
                return _columns_types

    with mrsm.Venv(get_connector_plugin(self.instance_connector)):
        _columns_types = (
            self.instance_connector.get_pipe_columns_types(self, debug=debug)
            if hasattr(self.instance_connector, 'get_pipe_columns_types')
            else None
        )

    self.__dict__['_columns_types'] = _columns_types
    self.__dict__['_columns_types_timestamp'] = now
    return _columns_types or {}


def get_columns_indices(
    self,
    debug: bool = False,
    refresh: bool = False,
) -> Dict[str, List[Dict[str, str]]]:
    """
    Return a dictionary mapping columns to index information.
    """
    import time
    from meerschaum.connectors import get_connector_plugin
    from meerschaum._internal.static import STATIC_CONFIG
    from meerschaum.utils.warnings import dprint

    now = time.perf_counter()
    cache_seconds = (
        STATIC_CONFIG['pipes']['static_schema_cache_seconds']
        if self.static
        else STATIC_CONFIG['pipes']['exists_timeout_seconds']
    )
    if refresh:
        _ = self.__dict__.pop('_columns_indices_timestamp', None)
        _ = self.__dict__.pop('_columns_indices', None)
    _columns_indices = self.__dict__.get('_columns_indices', None)
    if _columns_indices:
        columns_indices_timestamp = self.__dict__.get('_columns_indices_timestamp', None)
        if columns_indices_timestamp is not None:
            delta = now - columns_indices_timestamp
            if delta < cache_seconds:
                if debug:
                    dprint(
                        f"Returning cached `columns_indices` for {self} "
                        f"({round(delta, 2)} seconds old)."
                    )
                return _columns_indices

    with mrsm.Venv(get_connector_plugin(self.instance_connector)):
        _columns_indices = (
            self.instance_connector.get_pipe_columns_indices(self, debug=debug)
            if hasattr(self.instance_connector, 'get_pipe_columns_indices')
            else None
        )

    self.__dict__['_columns_indices'] = _columns_indices
    self.__dict__['_columns_indices_timestamp'] = now
    return {k: v for k, v in _columns_indices.items() if k and v} or {}


def get_id(self, **kw: Any) -> Union[int, None]:
    """
    Fetch a pipe's ID from its instance connector.
    If the pipe does not exist, return `None`.
    """
    if self.temporary:
        return None
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    with Venv(get_connector_plugin(self.instance_connector)):
        if hasattr(self.instance_connector, 'get_pipe_id'):
            return self.instance_connector.get_pipe_id(self, **kw)

    return None


@property
def id(self) -> Union[int, str, uuid.UUID, None]:
    """
    Fetch and cache a pipe's ID.
    """
    if not ('_id' in self.__dict__ and self._id):
        self._id = self.get_id()
    return self._id


def get_val_column(self, debug: bool = False) -> Union[str, None]:
    """
    Return the name of the value column if it's defined, otherwise make an educated guess.
    If not set in the `columns` dictionary, return the first numeric column that is not
    an ID or datetime column.
    If none may be found, return `None`.

    Parameters
    ----------
    debug: bool, default False:
        Verbosity toggle.

    Returns
    -------
    Either a string or `None`.
    """
    from meerschaum.utils.debug import dprint
    if debug:
        dprint('Attempting to determine the value column...')
    try:
        val_name = self.get_columns('value')
    except Exception:
        val_name = None
    if val_name is not None:
        if debug:
            dprint(f"Value column: {val_name}")
        return val_name

    cols = self.columns
    if cols is None:
        if debug:
            dprint('No columns could be determined. Returning...')
        return None
    try:
        dt_name = self.get_columns('datetime', error=False)
    except Exception:
        dt_name = None
    try:
        id_name = self.get_columns('id', errors=False)
    except Exception:
        id_name = None

    if debug:
        dprint(f"dt_name: {dt_name}")
        dprint(f"id_name: {id_name}")

    cols_types = self.get_columns_types(debug=debug)
    if cols_types is None:
        return None
    if debug:
        dprint(f"cols_types: {cols_types}")
    if dt_name is not None:
        cols_types.pop(dt_name, None)
    if id_name is not None:
        cols_types.pop(id_name, None)

    candidates = []
    candidate_keywords = {'float', 'double', 'precision', 'int', 'numeric',}
    for search_term in candidate_keywords:
        for col, typ in cols_types.items():
            if search_term in typ.lower():
                candidates.append(col)
                break
    if not candidates:
        if debug:
            dprint("No value column could be determined.")
        return None

    return candidates[0]


@property
def parents(self) -> List[mrsm.Pipe]:
    """
    Return a list of `meerschaum.Pipe` objects to be designated as parents.
    """
    if 'parents' not in self.parameters:
        return []
    from meerschaum.utils.warnings import warn
    _parents_keys = self.parameters['parents']
    if not isinstance(_parents_keys, list):
        warn(
            f"Please ensure the parents for {self} are defined as a list of keys.",
            stacklevel = 4
        )
        return []
    from meerschaum import Pipe
    _parents = []
    for keys in _parents_keys:
        try:
            p = Pipe(**keys)
        except Exception as e:
            warn(f"Unable to build parent with keys '{keys}' for {self}:\n{e}")
            continue
        _parents.append(p)
    return _parents


@property
def parent(self) -> Union[mrsm.Pipe, None]:
    """
    Return the first pipe in `self.parents` or `None`.
    """
    parents = self.parents
    if not parents:
        return None
    return parents[0]


@property
def children(self) -> List[mrsm.Pipe]:
    """
    Return a list of `meerschaum.Pipe` objects to be designated as children.
    """
    if 'children' not in self.parameters:
        return []
    from meerschaum.utils.warnings import warn
    _children_keys = self.parameters['children']
    if not isinstance(_children_keys, list):
        warn(
            f"Please ensure the children for {self} are defined as a list of keys.",
            stacklevel = 4
        )
        return []
    from meerschaum import Pipe
    _children = []
    for keys in _children_keys:
        try:
            p = Pipe(**keys)
        except Exception as e:
            warn(f"Unable to build parent with keys '{keys}' for {self}:\n{e}")
            continue
        _children.append(p)
    return _children


@property
def target(self) -> str:
    """
    The target table name.
    You can set the target name under on of the following keys
    (checked in this order):
      - `target`
      - `target_name`
      - `target_table`
      - `target_table_name`
    """
    if 'target' not in self.parameters:
        default_target = self._target_legacy()
        default_targets = {default_target}
        potential_keys = ('target_name', 'target_table', 'target_table_name')
        _target = None
        for k in potential_keys:
            if k in self.parameters:
                _target = self.parameters[k]
                break

        _target = _target or default_target

        if self.instance_connector.type == 'sql':
            from meerschaum.utils.sql import truncate_item_name
            truncated_target = truncate_item_name(_target, self.instance_connector.flavor)
            default_targets.add(truncated_target)
            warned_target = self.__dict__.get('_warned_target', False)
            if truncated_target != _target and not warned_target:
                if not warned_target:
                    warn(
                        f"The target '{_target}' is too long for '{self.instance_connector.flavor}', "
                        + f"will use {truncated_target} instead."
                    )
                    self.__dict__['_warned_target'] = True
                _target = truncated_target

        if _target in default_targets:
            return _target
        self.target = _target
    return self.parameters['target']


def _target_legacy(self) -> str:
    """
    The old method of determining a pipe's table name by joining the keys with underscores.
    **NOTE:** Converts the `':'` in the `connector_keys` to an `'_'`.
    """
    name = f"{self.connector_keys.replace(':', '_')}_{self.metric_key}"
    if self.location_key is not None:
        name += f"_{self.location_key}"
    return name


@target.setter
def target(self, _target: str) -> None:
    """
    Override the target of the in-memory pipe.
    Call `meerschaum.Pipe.edit` to persist changes.
    """
    self.update_parameters({'target': _target}, persist=False)


def guess_datetime(self) -> Union[str, None]:
    """
    Try to determine a pipe's datetime column.
    """
    _dtypes = self.dtypes

    ### Abort if the user explictly disallows a datetime index.
    if 'datetime' in _dtypes:
        if _dtypes['datetime'] is None:
            return None

    from meerschaum.utils.dtypes import are_dtypes_equal
    dt_cols = [
        col
        for col, typ in _dtypes.items()
        if are_dtypes_equal(typ, 'datetime')
    ]
    if not dt_cols:
        return None
    return dt_cols[0]


def get_indices(self) -> Dict[str, str]:
    """
    Return a dictionary mapping index keys to their names in the database.

    Returns
    -------
    A dictionary of index keys to index names.
    """
    from meerschaum.connectors import get_connector_plugin
    with mrsm.Venv(get_connector_plugin(self.instance_connector)):
        if hasattr(self.instance_connector, 'get_pipe_index_names'):
            result = self.instance_connector.get_pipe_index_names(self)
        else:
            result = {}
    
    return result


def update_parameters(
    self,
    parameters_patch: Dict[str, Any],
    persist: bool = True,
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Apply a patch to a pipe's `parameters` dictionary.

    Parameters
    ----------
    parameters_patch: Dict[str, Any]
        The patch to be applied to `Pipe.parameters`.

    persist: bool, default True
        If `True`, call `Pipe.edit()` to persist the new parameters.
    """
    from meerschaum.config import apply_patch_to_config
    if '_parameters' in self.__dict__:
        del self.__dict__['_parameters']
    if 'parameters' not in self._attributes:
        self._attributes['parameters'] = {}

    self._attributes['parameters'] = apply_patch_to_config(
        self._attributes['parameters'],
        parameters_patch,
    )

    if not persist:
        return True, "Success"
    return self.edit(debug=debug)
