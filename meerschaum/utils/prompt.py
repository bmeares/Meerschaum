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
        is_password : bool = False,
        wrap_lines : bool = True,
        noask : bool = False,
        **kw : Any
    ) -> str:
    """
    Ask the user a question and return the answer.
    Wrapper around `prompt_toolkit.prompt()` with modified behavior.
    For example, an empty string returns default instead of printing it for the user to delete
    (`prompt_toolkit` behavior).

    :param question:
        The question to print to the user.

    :param icon:
        If True, prepend the configured icon.

    :param default:
        If the response is '', return the default value.

    :param detect_password:
        If `True`, set the input method to a censored password box if the word `password`
        appears in the question.
        Defaults to `True`.

    :param is_password:
        If `True`, set the input method to a censored password box.
        May be overridden by `detect_password` unless `detect_password` is set to `False`.
        Defaults to `False`.

    :param wrap_lines:
        If `True`, wrap the text across multiple lines.
        Flag is passed onto `prompt_toolkit`.

    :param noask:
        If True, only print the question and return the default answer.
    """
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.formatting import colored, ANSI, CHARSET
    from meerschaum.config import get_config
    from meerschaum.config.static import _static_config
    if not noask:
        prompt_toolkit = attempt_import('prompt_toolkit')
    question_config = get_config('formatting', 'question', patch=True)

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
    if (detect_password and 'password' in question.lower()) or is_password:
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

    answer = (
        prompt_toolkit.prompt(
            prompt_toolkit.formatted_text.ANSI(question),
            wrap_lines = wrap_lines,
            **kw
        ) if not noask else ''
    )
    if noask:
        print(question)
    if answer == '' and default is not None:
        return default_answer
    return answer

def yes_no(
        question : str = '',
        options : Tuple[str, str] = ('y', 'n'),
        default : str = 'y',
        wrappers : Tuple[str, str] = ('[', ']'),
        icon : bool = True,
        yes : bool = False,
        noask : bool = False,
        interactive : bool = False,
        **kw : Any
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

    default = options[0] if yes else default
    noask = yes or noask

    ending = f" {wrappers[0]}" + "/".join(
        [
            o.upper() if o.lower() == default.lower()
            else o.lower() for o in options
        ]
    ) + f"{wrappers[1]}"
    while True:
        try:
            answer = prompt(question + ending, icon=icon, detect_password=False, noask=noask)
            success = True
        except KeyboardInterrupt:
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
        warn : bool = True,
        noask : bool = False,
        **kw
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
        choices_digits = len(str(len(choices)))
        for i, c in enumerate(choices):
            question += f"  {i + 1}. " + (" " * (choices_digits - len(str(i + 1)))) + f"{c}\n"
        default_tuple = (_default, default) if default is not None else None
    else:
        default_tuple = default
        question += '\n'
        for c in choices:
            question += f"  - {c}\n"

    while True:
        answer = prompt(question, icon=icon, default=default_tuple, noask=noask, **kw)
        if answer in _choices or answer == default or noask:
            break
        _warn(f"Please pick a valid choice.", stack=False)

    if numeric:
        try:
            return choices[int(answer) - 1]
        except Exception as e:
            _warn(f"Could not cast answer '{answer}' to an integer.", stacklevel=3)

    return answer

def get_password(
        username : Optional[str] = None,
        minimum_length : Optional[int] = None,
        confirm : bool = True,
        **kw : Any
    ) -> str:
    """
    Prompt the user for a password.
    """
    from meerschaum.utils.warnings import warn
    while True:
        password = prompt(
            f"Password" + (f" for user '{username}':" if username is not None else ":"),
            is_password = True,
            **kw
        )
        if minimum_length is not None and len(password) < minimum_length:
            warn(
                "Password is too short. " +
                f"Please enter a password that is at least {minimum_length} characters.",
                stack = False
            )
            continue

        if not confirm:
            return password

        _password = prompt(
            f"Confirm password" + (f" for user '{username}':") if username is not None else ":",
            is_password = True,
            **kw
        )
        if password != _password:
            warn(f"Passwords do not match! Please try again.", stack=False)
            continue
        else:
            return password

def get_email(username : Optional[str] = None, allow_omit : bool = True, **kw : Any) -> str:
    """
    Prompt the user for an email and enforce that it's valid.

    :param username:
        Include an optional username to print.

    :param allow_omit:
        Allow the user to omit the email.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.misc import is_valid_email
    while True:
        email = prompt(
            f"Email for user" + (f" '{username}'" if username is not None else "") +
            (" (empty to omit): " if allow_omit else ": "),
            **kw
        )
        if (allow_omit and email == '') or is_valid_email(email):
            return email
        warn(f"Invalid email! Please try again.", stack=False)
