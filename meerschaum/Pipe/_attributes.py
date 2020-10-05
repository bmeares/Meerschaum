#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch and manipulate Pipes' attributes
"""

@property
def attributes(self):
    if '_attributes' not in self.__dict__:
        ### TODO add API connector
        from meerschaum import get_connector
        meta_connector = get_connector('sql', 'meta')
        try:
            self._attributes = meta_connector.read(
                ("SELECT * " +
                 "FROM pipes " +
                f"WHERE pipe_id = {self.id}")
            ).to_dict('records')[0]
        except:
            return None

    return self._attributes

@property
def parameters(self):
    if '_parameters' not in self.__dict__:
        if not self.attributes:
            return None
        self._parameters = self.attributes['parameters']
    return self._parameters

### set to setter in class definition
@parameters.setter
def parameters(self, parameters):
    self._parameters = parameters

