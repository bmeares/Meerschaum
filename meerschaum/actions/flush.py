#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for flushing and recreating pipes and indices.
"""

from __future__ import annotations

from datetime import datetime
from meerschaum.utils.typing import SuccessTuple, Any, Optional, List, Union, Dict


def flush(
    action: Optional[List[str]] = None,
    **kw: Any
) -> SuccessTuple:
    """
    Drop and resync pipes or indices.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes'  : _flush_pipes,
        'indices': _flush_indices,
        'index': _flush_indices,
        'indexes': _flush_indices,
    }
    return choose_subaction(action, options, **kw)


def _flush_pipes(
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    **kwargs: Any
) -> SuccessTuple:
    """
    Drop and resync pipes.
    Equivalent to `drop pipes + sync pipes`.
    """
    from meerschaum.actions import get_action
    drop_or_clear_pipes_func = get_action([
        ('drop' if begin is None and end is None and not params else 'clear'),
        'pipes'
    ])
    drop_or_clear_success, drop_or_clear_message = drop_or_clear_pipes_func(
        begin=begin,
        end=end,
        params=params,
        **kwargs
    )
    if not drop_or_clear_success:
        return drop_or_clear_success, drop_or_clear_message

    sync_pipes_func = get_action(['sync', 'pipes'])
    sync_success, sync_message = sync_pipes_func(
        begin=begin,
        end=end,
        params=params,
        **kwargs
    )
    return sync_success, sync_message


def _flush_indices(
    **kwargs: Any
) -> SuccessTuple:
    """
    Drop and rebuild pipes' indices.
    """
    from meerschaum.actions import get_action
    drop_indices_func = get_action(['drop', 'indices'])
    drop_success, drop_message = drop_indices_func(**kwargs)
    if not drop_success:
        return drop_success, drop_message

    index_pipes_func = get_action(['index', 'pipes'])
    index_success, index_message = index_pipes_func(**kwargs)
    return index_success, index_message


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
flush.__doc__ += _choices_docstring('flush')
