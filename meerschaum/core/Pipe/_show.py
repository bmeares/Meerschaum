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
    from meerschaum.utils.formatting import pprint, make_header, ANSI, highlight_pipes, fill_ansi
    from meerschaum.utils.warnings import info
    if not nopretty:
        _to_print = f"Attributes for {self}:"
        if ANSI:
            _to_print = fill_ansi(highlight_pipes(make_header(_to_print)), 'magenta')
        print(_to_print)
        pprint(self.attributes)
    else:
        print(json.dumps(self.attributes))

    return True, "Success"
