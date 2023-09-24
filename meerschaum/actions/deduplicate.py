#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Deduplicate pipes' tables.
"""

from __future__ import annotations
from meerschaum.utils.typing import Union, Any, Sequence, SuccessTuple, Optional, Tuple, List

def deduplicate(
        action: Optional[List[str]] = None,
        **kw
    ) -> SuccessTuple:
    """
    Deduplicate pipes' tables, chunking across their datetime axes.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes': _deduplicate_pipes,
    }
    return choose_subaction(action, options, **kw)


def _deduplicate_pipes(**kwargs) -> SuccessTuple:
    """
    Deduplicate pipes' tables, chunking across their datetime axes.
    """
    from meerschaum.actions.sync import _sync_pipes
    kwargs.update({
        'verify': False,
        'deduplicate': True,
    })
    return _sync_pipes(**kwargs)


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
deduplicate.__doc__ += _choices_docstring('deduplicate')
