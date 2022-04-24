#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Show information about a Pipe
"""

from meerschaum.utils.typing import SuccessTuple

def show(
        self,
        nopretty: bool = False,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Show attributes of a Pipe.

    Parameters
    ----------
    nopretty: bool, default False
        If `True`, simply print the JSON of the pipe's attributes.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` of success, message.

    """
    import json
    from meerschaum.utils.formatting import pprint, make_header
    from meerschaum.utils.warnings import info
    if not nopretty:
        print(make_header(f"Attributes for pipe '{self}':"))
        pprint(self.attributes)
    else:
        print(json.dumps(self.attributes))

    return True, "Success"
