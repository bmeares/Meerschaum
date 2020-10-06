#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Show information about a Pipe
"""

def show(
        self,
        nopretty : bool = False,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Show aspects about a Pipe
    """
    from pprintpp import pprint
    if not nopretty:
        print(f"Information for Pipe '{self}':\n")
        print("Attributes:\n-----------")
    pprint(self.attributes)

    return True, "Success"
