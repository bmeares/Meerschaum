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
        self._attributes = meta_connector.value("SELECT * FROM ") 

    return self._attributes
