#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Functions for clearing elements
"""

from meerschaum.utils.typing import List, SuccessTuple, Any, Optional

def clear(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> SuccessTuple:
    """
    Clear pipes of their data, or clear the screen.
    """
    from meerschaum.utils.misc import choose_subaction
    if not action:
        from meerschaum.utils.formatting._shell import clear_screen
        return clear_screen(kw.get('debug', False)), ''

    options = {
        'pipes' : _clear_pipes,
    }
    return choose_subaction(action, options, **kw)

def _clear_pipes(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> SuccessTuple:
    """
    Clear pipes' data without dropping any tables.
    """
    return False, "Not implemented"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
clear.__doc__ += _choices_docstring('clear')
