#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch and manipulate Pipes' attributes
"""

from __future__ import annotations
from meerschaum.utils.typing import Tuple, Dict, SuccessTuple, Any, Union, Optional

@property
def attributes(self) -> Optional[Dict[str, Any]]:
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    if '_attributes' not in self.__dict__:
        if self.id is None:
            return None
        self._attributes = self.instance_connector.get_pipe_attributes(self)
    return self._attributes

@property
def parameters(self) -> Optional[Dict[str, Any]]:
    if '_parameters' not in self.__dict__:
        if not self.attributes:
            return None
        self._parameters = self.attributes['parameters']
    return self._parameters

@parameters.setter
def parameters(self, parameters : Dict[str, Any]) -> None:
    self._parameters = parameters

@property
def columns(self):
    if not self.parameters:
        if '_columns' in self.__dict__:
            return self._columns
        return None
    if 'columns' not in self.parameters:
        return None
    return self.parameters['columns']

@columns.setter
def columns(self, columns : Dict[str, str]) -> None:
    if not self.parameters:
        self._columns = columns
    else:
        self._parameters['columns'] = columns

def get_columns(self, *args : str, error : bool = True) -> Tuple[str]:
    """
    Check if the requested columns are defined.
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
                _error(f"Please define the name of the '{col}' column for Pipe '{self}'.")
        except Exception as e:
            col_name = None
        if col_name is None and error:
            _error(f"Missing '{col}'" + f" column for Pipe '{self}'.")
        col_names.append(col_name)
    if len(col_names) == 1:
        return col_names[0]
    return tuple(col_names)

def get_columns_types(self, debug : bool = False) -> Optional[Dict[str, str]]:
    """
    Return a dictionary of a pipe's table's columns to their data types.

    E.g. An example dictionary for a small table.

    ```
    >>> {
    ...   'dt': 'TIMESTAMP WITHOUT TIMEZONE',
    ...   'id': 'BIGINT',
    ...   'val': 'DOUBLE PRECISION',
    ... }
    >>> 
    ```
    """
    return self.instance_connector.get_pipe_columns_types(self, debug=debug)

def get_id(self, **kw : Any) -> Optional[int]:
    """
    Fetch a pipe's ID from its instance connector.
    """
    return self.instance_connector.get_pipe_id(self, **kw)

@property
def id(self) -> Optional[int]:
    if not ('_id' in self.__dict__ and self._id):
        self._id = self.get_id()
    return self._id

def get_val_column(self, debug: bool = False) -> Union[str, None]:
    """
    Return the name of the value of column.
    If not is set in the `columns` dictionary, return the first numeric column that is not
    an ID or datetime column.
    If none may be found, return `None`.
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

