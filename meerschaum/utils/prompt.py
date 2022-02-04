#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with the user.
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, Union, Optional, Tuple, List

def prompt(
        question: str,
        icon: bool = True,
        default: Union[str, Tuple[str, str], None] = None,
        detect_password: bool = True,
        is_password: bool = False,
        wrap_lines: bool = True,
        noask: bool = False,
        **kw: Any
    ) -> str:
    """Ask the user a question and return the answer.
    Wrapper around `prompt_toolkit.prompt()` with modified behavior.
    For example, an empty string returns default instead of printing it for the user to delete
    (`prompt_toolkit` behavior).

    Parameters
    ----------
    question :
        The question to print to the user.
    icon :
        If True, prepend the configured icon.
    default :
        If the response is '', return the default value.
    detect_password :
        If `True`, set the input method to a censored password box if the word `password`
        appears in the question.
        Defaults to `True`.
    is_password :
        If `True`, set the input method to a censored password box.
        May be overridden by `detect_password` unless `detect_password` is set to `False`.
        Defaults to `False`.
    wrap_lines :
        If `True`, wrap the text across multiple lines.
        Flag is passed onto `prompt_toolkit`.
    noask :
        If `True`, only print the question and return the default answer.
        Defaults to `False`.
    question: str :
        
    icon: bool :
         (Default value = True)
    default: Union[str :
        
    Tuple[str :
        
    str] :
        
    None] :
         (Default value = None)
    detect_password: bool :
         (Default value = True)
    is_password: bool :
         (Default value = False)
    wrap_lines: bool :
         (Default value = True)
    noask: bool :
         (Default value = False)
    **kw: Any :
        

    Returns
    -------

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
        first_line = colored(first_line, **question_config['ansi']['rich'])

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
        question: str = '',
        options: Tuple[str, str] = ('y', 'n'),
        default: str = 'y',
        wrappers: Tuple[str, str] = ('[', ']'),
        icon: bool = True,
        yes: bool = False,
        noask: bool = False,
        interactive: bool = False,
        **kw : Any
    ) -> bool:
    """Print a question and prompt the user with a yes / no input.
    Returns True for 'yes', False for 'no'.

    Parameters
    ----------
    question :
        The question to print to the user.
    options :
        The y/n options. The first is always considered `True`, and all options must be lower case.
        This behavior may be modifiable change in the future.
    default :
        The default option. Is represented with a capital to distinguish that it's the default.\
        E.g. [y/N] would return False by default.
    wrappers :
        Text to print around the '[y/n]' options.
        Defaults to ('[', ']').
    icon :
        If True, prepend the configured question icon.
    interactive :
        Not implemented. Was planning on using prompt_toolkit, but for some reason
        I can't figure out how to make the default selection 'No'.
    question: str :
         (Default value = '')
    options: Tuple[str :
        
    str] :
         (Default value = ('[')
    'n') :
        
    default: str :
         (Default value = 'y')
    wrappers: Tuple[str :
        
    ']') :
        
    icon: bool :
         (Default value = True)
    yes: bool :
         (Default value = False)
    noask: bool :
         (Default value = False)
    interactive: bool :
         (Default value = False)
    **kw : Any :
        

    Returns
    -------

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

        if answer.lower() in options:
            break
        warn('Please enter a valid reponse.', stack=False)
    
    return answer.lower() == options[0].lower()

def choose(
        question: str,
        choices: List[str],
        default: Optional[str] = None,
        numeric: bool = True,
        multiple: bool = False,
        delimiter: str = ',',
        icon: bool = True,
        warn: bool = True,
        noask: bool = False,
        **kw
    ) -> Union[str, Tuple[str], None]:
    """Present a list of options and return the user's choice.

    Parameters
    ----------
    question :
        The question to be printed.
    choices :
        A list of options.
    default :
        If the user declines to enter a choice, return this value.
        Defaults to `None`.
    numeric :
        If `True`, number the items in the list and ask for a number as input.
        If `False`, require the user to type the complete string.
        Defaults to `True`.
    multiple :
        If `True`, allow the user to choose multiple answers separated by `delimiter`.
        Defaults to `False`.
    delimiter :
        If `multiple`, separate answers by this string. Raise a warning if this string is contained
        in any of the choices.
        Defaults to ','.
    icon :
        If `True`, include the question icon.
        Defaults to `True`.
    warn :
        If `True`, raise warnings when invalid input is entered.
        Defaults to `True`.
    noask :
        If `True`, skip printing the question and return the default value.
        Defaults to `False`.
    question: str :
        
    choices: List[str] :
        
    default: Optional[str] :
         (Default value = None)
    numeric: bool :
         (Default value = True)
    multiple: bool :
         (Default value = False)
    delimiter: str :
         (Default value = ')
    ' :
        
    icon: bool :
         (Default value = True)
    warn: bool :
         (Default value = True)
    noask: bool :
         (Default value = False)
    **kw :
        

    Returns
    -------

    """
    from meerschaum.utils.warnings import warn as _warn

    ### Handle empty choices.
    if len(choices) == 0:
        _warn(f"No available choices. Returning default value '{default}'.", stacklevel=3)
        return default

    ### If the default case is to include multiple answers, allow for multiple inputs.
    if isinstance(default, list):
        multiple = True

    def _enforce_default(d):
        if d is not None and d not in choices and warn:
            _warn(
                f"Default choice '{default}' is not contained in the choices {choices}. "
                + "Setting numeric = False.",
                stacklevel = 3
            )
            return False
        return True

    ### Throw a warning if the default isn't a choice.
    for d in (default if isinstance(default, list) else [default]):
        if not _enforce_default(d):
            numeric = False
            break

    _default = default
    _choices = choices
    if multiple:
        question += f"\n    Enter your choices, separated by '{delimiter}'."

    altered_choices = {}
    altered_indices = {}
    altered_default_indices = {}
    delim_replacement = '_' if delimiter != '_' else '-'
    can_strip_start_spaces, can_strip_end_spaces = True, True
    for c in choices:
        if can_strip_start_spaces and c.startswith(' '):
            can_strip_start_spaces = False
        if can_strip_end_spaces and c.endswith(' '):
            can_strip_end_spaces = False

    if multiple:
        ### Check if the defaults have the delimiter.
        for i, d in enumerate(default if isinstance(default, list) else [default]):
            if d is None or delimiter not in d:
                continue
            new_d = d.replace(delimiter, delim_replacement)
            altered_choices[new_d] = d
            altered_default_indices[i] = new_d
        for i, new_d in altered_default_indices.items():
            if not isinstance(default, list):
                default = new_d
                break
            default[i] = new_d

        ### Check if the choices have the delimiter.
        for i, c in enumerate(choices):
            if delimiter in c and warn:
                _warn(
                    f"The delimiter '{delimiter}' is contained within choice '{c}'.\n"
                    + f"Replacing the string '{delimiter}' with '{delim_replacement}' in "
                    + "the choice for correctly parsing input (will be replaced upon returning the prompt).",
                    stacklevel = 3,
                )
                new_c = c.replace(delimiter, delim_replacement)
                altered_choices[new_c] = c
                altered_indices[i] = new_c
        for i, new_c in altered_indices.items():
            choices[i] = new_c
        default = delimiter.join(default) if isinstance(default, list) else default

    if numeric:
        _choices = [str(i + 1) for i, c in enumerate(choices)]
        _default = ''
        if default is not None:
            for d in (default.split(delimiter) if multiple else [default]):
                _d = str(choices.index(d) + 1)
                _default += _d + delimiter
        _default = _default[:-1 * len(delimiter)]
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

    valid = False
    while not valid:
        answer = prompt(
            question,
            icon = icon,
            default = default_tuple,
            noask = noask,
            **kw
        )
        ### Split along the delimiter.
        _answers = [answer] if not multiple else [a for a in answer.split(delimiter)]

        ### Remove trailing spaces if possible.
        _answers = [(_a.rstrip(' ') if can_strip_end_spaces else _a) for _a in _answers]

        ### Remove leading spaces if possible.
        _answers = [(_a.lstrip(' ') if can_strip_start_spaces else _a) for _a in _answers]

        ### Remove empty strings.
        _answers = [_a for _a in _answers if _a]

        if multiple and len(_answers) == 0:
            _answers = default_tuple if isinstance(default_tuple, list) else [default_tuple]
        answers = [altered_choices.get(a, a) for a in _answers]

        valid = (len(answers) > 1 or not (len(answers) == 1 and answers[0] is None))
        for a in answers:
            if (
                not a in {_original for _new, _original in altered_choices.items()}
                and not a in _choices
                and a != default
                and not noask
            ):
                valid = False
                break
        if valid:
            break
        if warn:
            _warn(f"Please pick a valid choice.", stack=False)

    if not multiple:
        if not numeric:
            return answer
        try:
            return choices[int(answer) - 1]
        except Exception as e:
            _warn(f"Could not cast answer '{answer}' to an integer.", stacklevel=3)

    if not numeric:
        return answers
    _answers = []
    for a in answers:
        try:
            _answer = choices[int(a) - 1]
            _answers.append(altered_choices.get(_answer, _answer))
        except Exception as e:
            _warn(f"Could not cast answer '{a}' to an integer.", stacklevel=3)
    return _answers


def get_password(
        username: Optional[str] = None,
        minimum_length: Optional[int] = None,
        confirm: bool = True,
        **kw: Any
    ) -> str:
    """Prompt the user for a password.

    Parameters
    ----------
    username: Optional[str] :
         (Default value = None)
    minimum_length: Optional[int] :
         (Default value = None)
    confirm: bool :
         (Default value = True)
    **kw: Any :
        

    Returns
    -------

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

def get_email(username: Optional[str] = None, allow_omit: bool = True, **kw: Any) -> str:
    """Prompt the user for an email and enforce that it's valid.

    Parameters
    ----------
    username :
        Include an optional username to print.
    allow_omit :
        Allow the user to omit the email.
    username: Optional[str] :
         (Default value = None)
    allow_omit: bool :
         (Default value = True)
    **kw: Any :
        

    Returns
    -------

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
