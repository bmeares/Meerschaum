#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Show information about a Pipe
"""

def show(
        self,
        action : list = [''],
        debug : bool = False
    ) -> tuple:
    """
    Show aspects about a Pipe
    """
    from pprintpp import pprint
    print(f"Information for Pipe '{self}':\n")
    print("Attributes:\n___________")
    pprint(self.attributes)

    return True, "Success"
