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
        if self.id is None: return None
        try:
            self._attributes = meta_connector.read(
                ("SELECT * " +
                 "FROM pipes " +
                f"WHERE pipe_id = {self.id}"),
            ).to_dict('records')[0]

        except:
            return None
        
        ### handle non-PostgreSQL databases (text vs JSON)
        if not isinstance(self._attributes['parameters'], dict):
            try:
                import json
                self._attributes['parameters'] = json.loads(self._attributes['parameters'])
            except:
                self._attributes['parameters'] = dict()

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
    if 'columns' not in self.parameters:
        return dict()
    return self.parameters['columns']
