#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Delete a Pipe's contents and registration
"""

def delete(
        self,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Call the Pipe's instance connector's delete_pipe method
    """
    result = self.instance_connector.delete_pipe(self, debug=debug, **kw)
    if not isinstance(result, tuple):
        return False, f"Received unexpected result from '{self.instance_connector}': {result}"
    if result[0]:
        to_delete = ['_id', '_attributes', '_parameters', '_columns']
        for member in to_delete:
            if member in self.__dict__:
                del self.__dict__[member]
    return result
