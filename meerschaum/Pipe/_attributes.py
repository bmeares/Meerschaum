#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch and manipulate Pipes' attributes
"""

@property
def attributes(self):
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    if '_attributes' not in self.__dict__:
        if self.id is None: return None
        self._attributes = self.instance_connector.get_pipe_attributes(self)
    return self._attributes

@property
def parameters(self):
    if '_parameters' not in self.__dict__:
        if not self.attributes:
            return None
        self._parameters = self.attributes['parameters']
    return self._parameters

@parameters.setter
def parameters(self, parameters):
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
def columns(self, columns):
    if not self.parameters:
        self._columns = columns
    else:
        self._parameters['columns'] = columns

def get_columns(self, *args):
    """
    Check if the requested columns are defined
    """
    from meerschaum.utils.warnings import error, warn
    col_names = []
    for col in args:
        col_name = None
        try:
            col_name = self.columns[col]
            if col_name is None:
                error(f"Please define the name of the '{col}' column for Pipe '{self}'.")
        except:
            col_name = None
        if col_name is None: error(f"Missing '{col}'" + f" column for Pipe '{self}'.")
        col_names.append(col_name)
    if len(col_names) == 1:
        return col_names[0]
    return tuple(col_names)
