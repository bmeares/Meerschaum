#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing elements belong here.
"""

from __future__ import annotations
import meerschaum as mrsm
from meerschaum.utils.typing import List, Any, SuccessTuple, Optional, Dict

def tag(
        action: Optional[List[str]] = None,
        **kwargs: Any
    ) -> SuccessTuple:
    """
    Edit an existing element.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes': _tag_pipes,
    }
    return choose_subaction(action, options, **kwargs)


def _tag_pipes(
        action: Optional[List[str]] = None,
        debug: bool = False,
        **kwargs: Any
    ) -> SuccessTuple:
    """
    Add or remove tags to registered pipes.
    Prefix a tag with a leading underscore to remove it.

    Note that the `--tags` flag applies to existing tags.
    Specify the tags you wish to add or remove as positional arguments, e.g.:

    ```
    mrsm tag pipes production sync-daily -c sql:main
    mrsm tag pipes _production --tags production
    ```
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.misc import separate_negation_values
    if not action:
        return False, "No tags were provided."

    pipes = mrsm.get_pipes(as_list=True, debug=debug, **kwargs)
    if not pipes:
        return False, "No pipes were found with the given keys."
    success_msg = f"Updated tags for {len(pipes)} pipe" + ('s' if len(pipes) != 1 else '') + '.'

    add_tags, remove_tags = separate_negation_values(action)
    edited_pipes = []
    for pipe in pipes:
        pipe_was_edited = False
        pipe_tags = pipe.tags
        existing_tags = set(pipe_tags)
        for tag_to_add in add_tags:
            if tag_to_add not in existing_tags:
                pipe_tags.append(tag_to_add)
                pipe_was_edited = True

        for tag_to_remove in remove_tags:
            if tag_to_remove in existing_tags:
                pipe_tags.remove(tag_to_remove)
                pipe_was_edited = True

        if pipe_was_edited:
            edited_pipes.append(pipe)

    if not edited_pipes:
        return True, success_msg

    failed_edit_msgs = []
    for pipe in edited_pipes:
        edit_success, edit_msg = pipe.edit(debug=debug)
        if not edit_success:
            warn(f"Failed to update tags for {pipe}:\n{edit_msg}", stack=False)
            failed_edit_msgs.append(edit_msg)

    num_failures = len(failed_edit_msgs)
    success = num_failures == 0
    msg = (
        success_msg
        if success else (
            f"Failed to update tags for {num_failures} pipe"
            + ('s' if num_failures != 1 else '')
            + ":\n"
            + '\n'.join(failed_edit_msgs)
        )
    )
    return success, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
tag.__doc__ += _choices_docstring('tag')
