#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Delete a Pipe's contents and registration
"""

def delete(
        self,
        debug : bool = False,
    ) -> tuple:
    """
    Call the Pipe's instance connector's delete_pipe method
    """
    return self.instance_connector.delete_pipe(self, debug=debug)
    
