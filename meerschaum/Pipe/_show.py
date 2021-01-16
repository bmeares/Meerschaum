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
    from meerschaum.utils.formatting import pprint, make_header
    from meerschaum.utils.warnings import info
    if not nopretty:
        print(make_header(f"Attributes for pipe '{self}':"))
        pprint(self.attributes)
    else: print(self.attributes)

    return True, "Success"
