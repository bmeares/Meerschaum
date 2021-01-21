#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with the user.
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, Union, Optional, Tuple

def prompt(question : str, **kw : Any) -> str:
    """
    Ask the user a question and return the answer.
    Wrapper around prompt_toolkit.prompt().
    """
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.formatting import colored, ANSI, CHARSET
    from meerschaum.config import get_config
    prompt_toolkit = attempt_import('prompt_toolkit')
    question_config = get_config('system', 'question', patch=True)
    
    ### Add the icon and only color the first line.
    lines = question.split('\n')
    first_line = lines[0]
    other_lines = '' if len(lines) <= 1 else '\n'.join(lines[1:])

    if ANSI:
        first_line = colored(question, *question_config['ansi']['color'])

    icon = question_config[CHARSET]['icon']
    question = ' ' + icon + ' ' + first_line
    if len(other_lines) > 0:
        question += '\n' + other_lines

    return prompt_toolkit.prompt(
        prompt_toolkit.formatted_text.ANSI(question),
        **kw
    )

def yes_no(
        question : str = '',
        options : Tuple[str, str] = ('y', 'n'),
        default : str = 'y',
        wrappers : Tuple[str, str] = ('[', ']'),
    ) -> bool:
    """
    Print a question and prompt the user with a yes / no input.

    Returns bool (answer).
    """
    from meerschaum.utils.warnings import error, warn
    ending = f" {wrappers[0]}" + "/".join(
        [
            o.upper() if o.lower() == default.lower()
            else o.lower() for o in options
        ]
    ) + f"{wrappers[1]} "
    try:
        answer = prompt(question + ending)
        success = True
    except:
        success = False
    
    if not success:
        error(f"Error getting response. Aborting...", stack=False)
    if answer == "":
        answer = default
    return answer.lower() == options[0].lower()

