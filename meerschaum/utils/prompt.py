#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with the user.
"""

from __future__ import annotations
import os
from meerschaum.utils.typing import Any, Union, Optional, Tuple, List

def prompt(
        question: str,
        icon: bool = True,
        default: Union[str, Tuple[str, str], None] = None,
        default_editable: Optional[str] = None,
        detect_password: bool = True,
        is_password: bool = False,
        wrap_lines: bool = True,
        noask: bool = False,
        **kw: Any
    ) -> str:
    """
    Ask the user a question and return the answer.
    Wrapper around `prompt_toolkit.prompt()` with modified behavior.
    For example, an empty string returns default instead of printing it for the user to delete
    (`prompt_toolkit` behavior).

    Parameters
    ----------
    question: str
        The question to print to the user.

    icon: bool, default True
        If True, prepend the configured icon.

    default: Union[str, Tuple[str, str], None], default None
        If the response is '', return the default value.

    default_editable: Optional[str], default None
        If provided, auto-type this user-editable string in the prompt.

    detect_password: bool, default True
        If `True`, set the input method to a censored password box if the word `password`
        appears in the question.

    is_password: default False
        If `True`, set the input method to a censored password box.
        May be overridden by `detect_password` unless `detect_password` is set to `False`.

    wrap_lines: bool, default True
        If `True`, wrap the text across multiple lines.
        Flag is passed onto `prompt_toolkit`.

    noask: bool, default False
        If `True`, only print the question and return the default answer.

    Returns
    -------
    A `str` of the input provided by the user.

    """
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.formatting import colored, ANSI, CHARSET, highlight_pipes, fill_ansi
    from meerschaum.config import get_config
    from meerschaum.config.static import _static_config
    from meerschaum.utils.misc import filter_keywords
    from meerschaum.utils.daemon import running_in_daemon
    noask = check_noask(noask)
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

    ### detect password
    if (detect_password and 'password' in question.lower()) or is_password:
        kw['is_password'] = True
  
    ### Add the icon and only color the first line.
    lines = question.split('\n')
    first_line = lines[0]
    other_lines = '' if len(lines) <= 1 else '\n'.join(lines[1:])

    if ANSI:
        first_line = fill_ansi(highlight_pipes(first_line), **question_config['ansi']['rich'])
        other_lines = highlight_pipes(other_lines)

    _icon = question_config[CHARSET]['icon']
    question = (' ' + _icon + ' ') if icon and len(_icon) > 0 else ''
    question += first_line
    if len(other_lines) > 0:
        question += '\n' + other_lines
    question += ' '

    if not running_in_daemon():
        answer = (
            prompt_toolkit.prompt(
                prompt_toolkit.formatted_text.ANSI(question),
                wrap_lines = wrap_lines,
                default = default_editable or '',
                **filter_keywords(prompt_toolkit.prompt, **kw)
            ) if not noask else ''
        )
    else:
        print(question, end='\n', flush=True)
        try:
            answer = input()
        except EOFError:
            answer = ''
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
        **kw : Any
    ) -> bool:
    """
    Print a question and prompt the user with a yes / no input.
    Returns `True` for `'yes'`, False for `'no'`.

    Parameters
    ----------
    question: str, default ''
        The question to print to the user.

    options: Tuple[str, str], default ('y', 'n')
        The `y/n` options. The first is considered `True`, and all options must be lower case.

    default: str, default y
        The default option. Is represented with a capital to distinguish that it's the default.

    wrappers: Tuple[str, str], default ('[', ']')
        Text to print around the '[y/n]' options.

    icon: bool, default True
        If True, prepend the configured question icon.

    Returns
    -------
    A bool indicating the user's choice.

    Examples
    --------
    ```python-repl
    >>> yes_no("Do you like me?", default='y')
     ❓ Do you like me? [Y/n]
    True
    >>> yes_no("Cats or dogs?", options=('cats', 'dogs'))
     ❓ Cats or dogs? [cats/dogs]
     Please enter a valid response.
     ❓ Cats or dogs? [cats/dogs] dogs
    False
    ```
    """
    from meerschaum.utils.warnings import error, warn
    from meerschaum.utils.formatting import ANSI, UNICODE
    from meerschaum.utils.packages import attempt_import

    default = options[0] if yes else default
    noask = yes or check_noask(noask)

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
        choices: List[Union[str, Tuple[str, str]]],
        default: Union[str, List[str], None] = None,
        numeric: bool = True,
        multiple: bool = False,
        as_indices: bool = False,
        delimiter: str = ',',
        icon: bool = True,
        warn: bool = True,
        noask: bool = False,
        **kw
    ) -> Union[str, Tuple[str], None]:
    """
    Present a list of options and return the user's choice.

    Parameters
    ----------
    question: str
        The question to be printed.

    choices: List[Union[str, Tuple[str, str]]
        A list of options.
        If an option is a tuple of two strings, the first string is treated as the index
        and not displayed. In this case, set `as_indices` to `True` to return the index.

    default: Union[str, List[str], None], default None
        If the user declines to enter a choice, return this value.

    numeric: bool, default True
        If `True`, number the items in the list and ask for a number as input.
        If `False`, require the user to type the complete string.

    multiple: bool, default False
        If `True`, allow the user to choose multiple answers separated by `delimiter`.

    as_indices: bool, default False
        If `True`, return the indices for the choices.
        If a choice is a tuple of two strings, the first is assumed to be the index.
        Otherwise the index in the list is returned.

    delimiter: str, default ','
        If `multiple`, separate answers by this string. Raise a warning if this string is contained
        in any of the choices.

    icon: bool, default True
        If `True`, include the question icon.

    warn: bool, default True
        If `True`, raise warnings when invalid input is entered.

    noask: bool, default False
        If `True`, skip printing the question and return the default value.

    Returns
    -------
    A string for a single answer or a tuple of strings if `multiple` is `True`.

    """
    from meerschaum.utils.warnings import warn as _warn
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.misc import print_options
    noask = check_noask(noask)

    ### Handle empty choices.
    if len(choices) == 0:
        _warn(f"No available choices. Returning default value '{default}'.", stacklevel=3)
        return default

    ### If the default case is to include multiple answers, allow for multiple inputs.
    if isinstance(default, list):
        multiple = True

    choices_indices = {}
    for i, c in enumerate(choices):
        if isinstance(c, tuple):
            i, c = c
        choices_indices[i] = c

    def _enforce_default(d):
        if d is not None and d not in choices and d not in choices_indices and warn:
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
    _choices = list(choices_indices.values())
    if multiple:
        question += f"\n    Enter your choices, separated by '{delimiter}'.\n"

    altered_choices = {}
    altered_indices = {}
    altered_default_indices = {}
    delim_replacement = '_' if delimiter != '_' else '-'
    can_strip_start_spaces, can_strip_end_spaces = True, True
    for i, c in choices_indices.items():
        if isinstance(c, tuple):
            key, c = c
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
        for i, c in choices_indices.items():
            if delimiter in c and not numeric and warn:
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
            choices_indices[i] = new_c
        default = delimiter.join(default) if isinstance(default, list) else default

    question_options = []
    if numeric:
        _choices = [str(i + 1) for i, c in enumerate(choices)]
        _default = ''
        if default is not None:
            for d in (default.split(delimiter) if multiple else [default]):
                if d not in choices and d in choices_indices:
                    d_index = d
                    d_value = choices_indices[d]
                    for _i, _option in enumerate(choices):
                        if (
                            isinstance(_option, tuple) and (
                                _option[1] == d_value
                                or
                                _option[0] == d_index
                            )
                        ) or d_index == _i:
                            d = _option

                _d = str(choices.index(d) + 1)
                _default += _d + delimiter
        _default = _default[:-1 * len(delimiter)]
        #  question += '\n'
        choices_digits = len(str(len(choices)))
        for i, c in enumerate(choices_indices.values()):
            question_options.append(
                f"  {i + 1}. "
                + (" " * (choices_digits - len(str(i + 1))))
                + f"{c}\n"
            )
        default_tuple = (_default, default) if default is not None else None
    else:
        default_tuple = default
        #  question += '\n'
        for c in choices_indices.values():
            question_options.append(f"{c}\n")

    if 'completer' not in kw:
        WordCompleter = attempt_import('prompt_toolkit.completion').WordCompleter
        kw['completer'] = WordCompleter(choices_indices.values(), sentence=True)

    valid = False
    while not valid:
        print_options(question_options, header='')
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
            _answer = choices[int(answer) - 1]
            if as_indices and isinstance(choice, tuple):
                return _answer[0]
            return _answer
        except Exception as e:
            _warn(f"Could not cast answer '{answer}' to an integer.", stacklevel=3)

    if not numeric:
        return answers
    _answers = []
    for a in answers:
        try:
            _answer = choices[int(a) - 1]
            _answer_to_return = altered_choices.get(_answer, _answer)
            if isinstance(_answer_to_return, tuple) and as_indices:
                _answer_to_return = _answer_to_return[0]
            _answers.append(_answer_to_return)
        except Exception as e:
            _warn(f"Could not cast answer '{a}' to an integer.", stacklevel=3)
    return _answers


def get_password(
        username: Optional[str] = None,
        minimum_length: Optional[int] = None,
        confirm: bool = True,
        **kw: Any
    ) -> str:
    """
    Prompt the user for a password.

    Parameters
    ----------
    username: Optional[str], default None
        If provided, print the username when asking for a password.

    minimum_length: Optional[int], default None
        If provided, enforce a password of at least this length.

    confirm: bool, default True
        If `True`, prompt the user for a password twice.

    Returns
    -------
    The password string (censored from terminal output when typing).

    Examples
    --------
    ```python-repl
    >>> get_password()
     ❓ Password: *******
     ❓ Confirm password: *******
    'hunter2'
    ```

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
            f"Confirm password" + (f" for user '{username}':" if username is not None else ":"),
            is_password = True,
            **kw
        )
        if password != _password:
            warn(f"Passwords do not match! Please try again.", stack=False)
            continue
        else:
            return password


def get_email(username: Optional[str] = None, allow_omit: bool = True, **kw: Any) -> str:
    """
    Prompt the user for an email and enforce that it's valid.

    Parameters
    ----------
    username: Optional[str], default None
        If provided, print the username in the prompt.

    allow_omit: bool, default True
        If `True`, allow the user to omit the email.

    Returns
    -------
    The provided email string.

    Examples
    --------
    ```python-repl
    >>> get_email()
     ❓ Email (empty to omit): foo@foo
     Invalid email! Please try again.
     ❓ Email (empty to omit): foo@foo.com
    'foo@foo.com'
    ```
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.misc import is_valid_email
    while True:
        email = prompt(
            f"Email" + (f" for user '{username}'" if username is not None else "") +
            (" (empty to omit):" if allow_omit else ": "),
            **kw
        )
        if (allow_omit and email == '') or is_valid_email(email):
            return email
        warn(f"Invalid email! Please try again.", stack=False)


def check_noask(noask: bool = False) -> bool:
    """
    Flip `noask` to `True` if `MRSM_NOASK` is set.
    """
    from meerschaum.config.static import STATIC_CONFIG
    NOASK = STATIC_CONFIG['environment']['noask']
    if noask:
        return True
    return (
        os.environ.get(NOASK, 'false').lower()
        in ('1', 'true')
    )
