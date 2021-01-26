#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with the user.
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, Union, Optional, Tuple, List

def prompt(
        question : str,
        icon : bool = True,
        default : Union[str, Tuple[str, str], None] = None,
        detect_password : bool = True,
        **kw : Any
    ) -> str:
    """
    Ask the user a question and return the answer.
    Wrapper around prompt_toolkit.prompt() with modified behavior.
    For example, an empty string returns default instead of printing it for the user to delete
    (prompt_toolkit behavior).

    :param question:
        The question to print to the user.

    :param icon:
        If True, prepend the configured icon.

    :param default:
        If the response is '', return the default value.
    """
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.formatting import colored, ANSI, CHARSET
    from meerschaum.config import get_config
    prompt_toolkit = attempt_import('prompt_toolkit')
    question_config = get_config('system', 'question', patch=True)

    ### if a default is provided, append it to the question.
    default_answer = default
    if default is not None:
        question += f" (default: "
        if isinstance(default, tuple) and len(default) > 1:
            question += f"{default[0]} [{default[1]}]"
            default_answer = default[0]
        else:
            question += f"{default}"
        question += ")"
    #  if not question.endswith(': '):
        #  if question.endswith(':'): question += " "
        #  else: question += ": "

    ### detect password
    if detect_password and 'password' in question.lower():
        kw['is_password'] = True
  
    ### Add the icon and only color the first line.
    lines = question.split('\n')
    first_line = lines[0]
    other_lines = '' if len(lines) <= 1 else '\n'.join(lines[1:])

    if ANSI:
        first_line = colored(first_line, *question_config['ansi']['color'])

    _icon = question_config[CHARSET]['icon']
    question = (' ' + _icon + ' ') if icon and len(_icon) > 0 else ''
    question += first_line
    if len(other_lines) > 0:
        question += '\n' + other_lines
    question += ' '

    answer = prompt_toolkit.prompt(
        prompt_toolkit.formatted_text.ANSI(question),
        **kw
    )
    if answer == '' and default is not None:
        return default_answer
    return answer

def yes_no(
        question : str = '',
        options : Tuple[str, str] = ('y', 'n'),
        default : str = 'y',
        wrappers : Tuple[str, str] = ('[', ']'),
        icon : bool = True,
        interactive : bool = False
    ) -> bool:
    """
    Print a question and prompt the user with a yes / no input.
    Returns True for 'yes', False for 'no'.

    :param question:
        The question to print to the user.

    :param options:
        The y/n options. The first is always considered True.
        This behavior may be modifiable change in the future.

    :param default:
        The default option. Is represented with a capital to distinguish that it's the default.\
        E.g. [y/N] would return False by default.

    :param wrappers:
        Text to print around the '[y/n]' options.
        Defaults to ('[', ']').

    :param icon:
        If True, prepend the configured question icon.

    :param interactive:
        Not implemented. Was planning on using prompt_toolkit, but for some reason
        I can't figure out how to make the default selection 'No'.
    """
    from meerschaum.utils.warnings import error, warn
    from meerschaum.utils.formatting import ANSI, UNICODE
    from meerschaum.utils.packages import attempt_import

    ### TODO interactive mode
    #  word_mapping = { 'y' : 'Yes', 'n' : 'No' }
    #  if ANSI and UNICODE and interactive:
        #  prompt_toolkit = attempt_import('prompt_toolkit')
        #  buttons = [
            #  word_mapping.get(default, default)
        #  ]
        #  return prompt_toolkit.shortcuts.button_dialog(
            #  buttons = []
        #  )


    ending = f" {wrappers[0]}" + "/".join(
        [
            o.upper() if o.lower() == default.lower()
            else o.lower() for o in options
        ]
    ) + f"{wrappers[1]}"
    while True:
        try:
            answer = prompt(question + ending, icon=icon)
            success = True
        except:
            success = False
        
        if not success:
            error(f"Error getting response. Aborting...", stack=False)
        if answer == "":
            answer = default

        if answer in options:
            break
        warn('Please enter a valid reponse.', stack=False)
    
    return answer.lower() == options[0].lower()

def choose(
        question : str,
        choices : List[str],
        default : Optional[str] = None,
        numeric : bool = True,
        icon : bool = True,
        warn : bool = True
    ) -> str:
    from meerschaum.utils.warnings import warn as _warn

    ### Handle empty choices.
    if len(choices) == 0:
        _warn(f"No available choices. Returning default value '{default}'.", stacklevel=3)
        return default

    ### Throw a warning if the default isn't a choice.
    if default is not None and default not in choices and warn:
        _warn(f"Default choice '{default}' is not contained in the choices {choices}. Setting numeric = False.", stacklevel=3)
        numeric = False

    _default = default
    _choices = choices
    if numeric:
        _choices = [str(i + 1) for i, c in enumerate(choices)]
        if default in choices:
            _default = str(choices.index(default) + 1)
        question += '\n'
        for i, c in enumerate(choices):
            question += f"  {i + 1}. {c}\n"
        default_tuple = (_default, default)
    else:
        default_tuple = default
        question += '\n'
        for c in choices:
            question += f"  - {c}\n"

    while True:
        answer = prompt(question, icon=icon, default=default_tuple)
        if answer in _choices or answer == default:
            break
        _warn(f"Please pick a valid choice.", stack=False)

    if numeric:
        return choices[int(answer) - 1]
    return answer

