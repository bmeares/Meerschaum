#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Functions for dropping elements
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Union, Any, Optional, Sequence

def drop(
        action : Sequence[str] = [''],
        **kw : Any
    ) -> SuccessTuple:
    """
    Drop pipe data (maintaining registration) or tables.
    """
    from meerschaum.utils.misc import choose_subaction

    options = {
        'pipes'  : _drop_pipes,
        'tables' : _drop_tables,
    }
    return choose_subaction(action, options, **kw)

def _drop_pipes(
        action : Sequence[str] = [''],
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Drop pipes' tables but keep pipe metadata registration.
    """
    from meerschaum import get_pipes
    pipes = get_pipes(as_list=True, debug=debug, **kw)
    for p in pipes:
        p.drop()
    return False, "Not implemented"

def _drop_tables(
    action : Sequence[str] = [''],
    **kw : Any
) -> SuccessTuple:
    """
    Drop SQL tables. WARNING: This is very dangerous!
    """
    return False, "Not implemented"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
drop.__doc__ += _choices_docstring('drop')
