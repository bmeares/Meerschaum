#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register a Pipe object
"""

def register(
        self,
        debug : bool = False
    ):
    """
    Send a POST to the Meerschaum API to register a new Pipe
    """
    if not self.parameters:
        self.parameters = {
            'columns' : self.columns,
        }
    return self.instance_connector.register_pipe(self, debug=debug)

