#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch and manipulate Pipes' attributes
"""

from __future__ import annotations
from meerschaum.utils.typing import Tuple, Dict, SuccessTuple, Any, Union, Optional, List

@property
def attributes(self) -> Union[Dict[str, Any], None]:
    """
    Return a dictionary of a pipe's keys and parameters.
    Is a superset of `meerschaum.Pipe.parameters` and
    **ONLY** returns a dictionary if the pipe is registered.
    An unregistered pipe may still set its parameters.
    Use `meerschaum.Pipe.meta` to retrieve keys from unregistered pipes.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    if '_attributes' not in self.__dict__:
        if self.id is None:
            return None
        self._attributes = self.instance_connector.get_pipe_attributes(self)
    return self._attributes


@property
def parameters(self) -> Optional[Dict[str, Any]]:
    """
    Return the parameters dictionary of the pipe.
    """
    if '_parameters' not in self.__dict__:
        if not self.attributes:
            return None
        self._parameters = self.attributes['parameters']
    return self._parameters


@parameters.setter
def parameters(self, parameters : Dict[str, Any]) -> None:
    """
    Set the parameters dictionary of the in-memory pipe.
    Call `meerschaum.Pipe.edit` to persist changes.
    """
    self._parameters = parameters


@property
def columns(self) -> Union[Dict[str, str], None]:
    """
    If defined, return the `columns` dictionary defined in `meerschaum.Pipe.parameters`.
    """
    if not self.parameters:
        if '_columns' in self.__dict__:
            return self._columns
        return None
    if 'columns' not in self.parameters:
        return None
    return self.parameters['columns']


@columns.setter
def columns(self, columns: Dict[str, str]) -> None:
    """
    Override the columns dictionary of the in-memory pipe.
    Call `meerschaum.Pipe.edit` to persist changes.
    """
    if not self.parameters:
        self._columns = columns
    else:
        self._parameters['columns'] = columns

@property
def tags(self) -> Union[List[str], None]:
    """
    If defined, return the `tags` list defined in `meerschaum.Pipe.parameters`.
    """
    if not self.parameters:
        if '_tags' in self.__dict__:
            return self._tags
        return None
    if 'tags' not in self.parameters:
        return None
    return self.parameters['tags']


@tags.setter
def tags(self, _tags: List[str, str]) -> None:
    """
    Override the tags list of the in-memory pipe.
    Call `meerschaum.Pipe.edit` to persist changes.
    """
    from meerschaum.utils.warnings import error
    from meerschaum.config.static import _static_config
    negation_prefix = _static_config()['system']['fetch_pipes_keys']['negation_prefix']
    for t in _tags:
        if t.startswith(negation_prefix):
            error(f"Tags cannot begin with '{negation_prefix}'.")
    if not self.parameters:
        self._tags = _tags
    else:
        self._parameters['tags'] = _tags

@property
def dtypes(self) -> Union[Dict[str, Any], None]:
    """
    If defined, return the `dtypes` dictionary defined in `meerschaum.Pipe.parameters`.
    """
    if self.parameters is None or self.parameters.get('dtypes', None) is None:
        if '_dtypes' in self.__dict__:
            return self._dtypes
        self._dtypes = self.infer_dtypes(persist=False)
        return self._dtypes

    return self.parameters['dtypes']


@dtypes.setter
def dtypes(self, _dtypes: Dict[str, Any]) -> None:
    """
    Override the columns dictionary of the in-memory pipe.
    Call `meerschaum.Pipe.edit` to persist changes.
    """
    if not self.parameters:
        self._dtypes = _dtypes
    else:
        self._parameters['dtypes'] = _dtypes


def get_columns(self, *args: str, error : bool = True) -> Tuple[str]:
    """
    Check if the requested columns are defined.

    Parameters
    ----------
    *args : str :
        The column names to be retrieved.
        
    error : bool, default True:
        If `True`, raise an `Exception` if the specified column is not defined.

    Returns
    -------
    A tuple of the same size of `args`.

    Examples
    --------
    >>> pipe = mrsm.Pipe('test', 'test')
    >>> pipe.columns = {'datetime': 'dt', 'id': 'id'}
    >>> pipe.get_columns('datetime', 'id')
    ('dt', 'id')
    >>> pipe.get_columns('value')
    Exception:  🛑 Missing 'value' column for Pipe('test', 'test').
    """
    from meerschaum.utils.warnings import error as _error, warn
    if not args:
        args = tuple(self.columns.keys())
    col_names = []
    for col in args:
        col_name = None
        try:
            col_name = self.columns[col]
            if col_name is None and error:
                _error(f"Please define the name of the '{col}' column for {self}.")
        except Exception as e:
            col_name = None
        if col_name is None and error:
            _error(f"Missing '{col}'" + f" column for {self}.")
        col_names.append(col_name)
    if len(col_names) == 1:
        return col_names[0]
    return tuple(col_names)

def get_columns_types(self, debug: bool = False) -> Union[Dict[str, str], None]:
    """
    Get a dictionary of a pipe's column names and their types.

    Parameters
    ----------
    debug : bool, default False:
        Verbosity toggle.

    Returns
    -------
    A dictionary of column names (`str`) to column types (`str`).

    Examples
    --------
    >>> pipe.get_columns_types()
    {
      'dt': 'TIMESTAMP WITHOUT TIMEZONE',
      'id': 'BIGINT',
      'val': 'DOUBLE PRECISION',
    }
    >>>
    """
    return self.instance_connector.get_pipe_columns_types(self, debug=debug)

def get_id(self, **kw: Any) -> Union[int, None]:
    """
    Fetch a pipe's ID from its instance connector.
    If the pipe does not exist, return `None`.
    """
    return self.instance_connector.get_pipe_id(self, **kw)

@property
def id(self) -> Union[int, None]:
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
    except Exception as e:
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
        dt_name = self.get_columns('datetime')
    except Exception as e:
        dt_name = None
    try:
        id_name = self.get_columns('id')
    except Exception as e:
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
            dprint(f"No value column could be determined.")
        return None

    return candidates[0]


@property
def parents(self) -> List[meerschaum.Pipe]:
    """
    Return a list of `meerschaum.Pipe` objects.
    These pipes will be synced before this pipe.

    NOTE: Not yet in use!
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
    _target_key = '_target'
    if _target_key not in self.__dict__:
        if not self.parameters:
            self.__dict__[_target_key] = self._target_legacy()
        else:
            potential_keys = ('target', 'target_name', 'target_table', 'target_table_name')
            for k in potential_keys:
                if k in self.parameters:
                    self.__dict__[_target_key] = self.parameters[k]
                    break
    return self.__dict__.get(_target_key, self._target_legacy())


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
    if not self.parameters:
        self._target = _target
    else:
        self._parameters['target'] = _target


