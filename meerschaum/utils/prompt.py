#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with the user.
"""

from __future__ import annotations

import os
import meerschaum as mrsm
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
    silent: bool = False,
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

    silent: bool, default False
        If `True` do not print anything to the screen, but still block for input.

    Returns
    -------
    A `str` of the input provided by the user.

    """
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.formatting import ANSI, CHARSET, highlight_pipes, fill_ansi
    from meerschaum.config import get_config
    from meerschaum.utils.misc import filter_keywords, remove_ansi
    from meerschaum.utils.daemon import running_in_daemon
    noask = check_noask(noask)
    if not noask:
        prompt_toolkit = attempt_import('prompt_toolkit')
    question_config = get_config('formatting', 'question', patch=True)

    ### if a default is provided, append it to the question.
    default_answer = default
    if default is not None:
        question += " (default: "
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

    if not remove_ansi(question).endswith(' '):
        question += ' '

    if not running_in_daemon():
        answer = (
            prompt_toolkit.prompt(
                prompt_toolkit.formatted_text.ANSI(question) if not silent else '',
                wrap_lines=wrap_lines,
                default=default_editable or '',
                **filter_keywords(prompt_toolkit.prompt, **kw)
            ) if not noask else ''
        )
    else:
        if not silent:
            print(question, end='', flush=True)
        try:
            answer = input() if not noask else ''
        except EOFError:
            answer = ''

    if noask and not silent:
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
            error("Error getting response. Aborting...", stack=False)
        if answer == "":
            answer = default

        if answer.lower() in options:
            break
        warn('Please enter a valid reponse.', stack=False)
    
    return answer.lower() == options[0].lower()


def choose(
    question: str,
    choices: Union[List[str], List[Tuple[str, str]]],
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
    if not choices:
        if warn:
            _warn(f"No available choices. Returning default value '{default}'.", stacklevel=3)
        return default

    ### If the default case is to include multiple answers, allow for multiple inputs.
    if isinstance(default, list):
        multiple = True

    choices_indices = {}
    for i, c in enumerate(choices, start=1):
        if isinstance(c, tuple):
            i, c = c
        choices_indices[i] = c

    choices_values_indices = {v: k for k, v in choices_indices.items()}
    ordered_keys = list(choices_indices.keys())
    numeric_map = {str(i): key for i, key in enumerate(ordered_keys, 1)}

    def _enforce_default(d):
        if d is None:
            return True
        if d not in choices_values_indices and d not in choices_indices:
            if warn:
                _warn(
                    f"Default choice '{d}' is not contained in the choices. "
                    + "Setting numeric = False.",
                    stacklevel=3
                )
            return False
        return True

    ### Throw a warning if the default isn't a choice.
    for d in (default if isinstance(default, list) else [default]):
        if not _enforce_default(d):
            numeric = False
            break

    _choices = (
        [str(k) for k in choices_indices] if numeric
        else list(choices_indices.values())
    )
    if multiple:
        question += f"\n    Enter your choices, separated by '{delimiter}'.\n"

    altered_choices = {}
    if multiple and not numeric:
        delim_replacement = '_' if delimiter != '_' else '-'
        ### Check if the choices have the delimiter.
        for i, c in choices_indices.items():
            if delimiter not in c:
                continue
            if warn:
                _warn(
                    f"The delimiter '{delimiter}' is contained within choice '{c}'.\n"
                    + f"Replacing the string '{delimiter}' with '{delim_replacement}' in "
                    + "the choice for correctly parsing input (will be replaced upon returning the prompt).",
                    stacklevel=3,
                )
            new_c = c.replace(delimiter, delim_replacement)
            altered_choices[new_c] = c
            choices_indices[i] = new_c

    question_options = []
    default_tuple = None
    if numeric:
        _default_prompt_str = ''
        if default is not None:
            default_list = default if isinstance(default, list) else [default]
            if multiple and isinstance(default, str):
                default_list = default.split(delimiter)

            _default_indices = []
            for d in default_list:
                key = None
                if d in choices_values_indices:  # is a value
                    key = choices_values_indices[d]
                elif d in choices_indices:  # is an index
                    key = d

                if key in ordered_keys:
                    _default_indices.append(str(ordered_keys.index(key) + 1))

            _default_prompt_str = delimiter.join(_default_indices)
        
        choices_digits = len(str(len(choices)))
        for choice_ix, c in enumerate(choices_indices.values(), start=1):
            question_options.append(
                f"  {choice_ix}. "
                + (" " * (choices_digits - len(str(choice_ix))))
                + f"{c}\n"
            )
        default_tuple = (_default_prompt_str, default) if default is not None else None
    else:
        default_tuple = default
        for c in choices_indices.values():
            question_options.append(f"  • {c}\n")

    if 'completer' not in kw:
        WordCompleter = attempt_import('prompt_toolkit.completion', lazy=False).WordCompleter
        kw['completer'] = WordCompleter(
            [str(v) for v in choices_indices.values()] + [str(i) for i in choices_indices],
            sentence=True,
        )

    answers = []
    while not answers:
        print_options(question_options, header='')
        answer = prompt(
            question,
            icon=icon,
            default=default_tuple,
            noask=noask,
            **kw
        )
        if not answer and default is not None:
            answer = default if isinstance(default, str) else delimiter.join(default)

        if not answer:
            if warn:
                _warn("Please pick a valid choice.", stack=False)
            continue

        _answers = [answer] if not multiple else [a.strip() for a in answer.split(delimiter)]
        _answers = [a for a in _answers if a]

        if numeric:
            _raw_answers = list(_answers)
            _answers = []
            for _a in _raw_answers:
                if _a in choices_values_indices:
                    _answers.append(str(choices_values_indices[_a]))
                elif _a in numeric_map:
                    _answers.append(str(numeric_map[_a]))
                else:
                    _answers.append(_a)

        _processed_answers = [altered_choices.get(a, a) for a in _answers]

        valid_answers = []
        for a in _processed_answers:
            if a in _choices:
                valid_answers.append(a)

        if len(valid_answers) != len(_processed_answers):
            if warn:
                _warn("Please pick a valid choice.", stack=False)
            continue
        answers = valid_answers

    def get_key(key_str):
        try:
            return int(key_str)
        except (ValueError, TypeError):
            return key_str

    if not multiple:
        answer = answers[0]
        if not numeric:
            return choices_values_indices.get(answer, answer) if as_indices else answer
        
        key = get_key(answer)
        return key if as_indices else choices_indices[key]

    if not numeric:
        return [choices_values_indices.get(a, a) for a in answers] if as_indices else answers

    final_answers = []
    for a in answers:
        key = get_key(a)
        final_answers.append(key if as_indices else choices_indices[key])
    return final_answers



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
            "Password" + (f" for user '{username}':" if username is not None else ":"),
            is_password=True,
            **kw
        )
        if minimum_length is not None and len(password) < minimum_length:
            warn(
                "Password is too short. " +
                f"Please enter a password that is at least {minimum_length} characters.",
                stack=False
            )
            continue

        if not confirm:
            return password

        _password = prompt(
            "Confirm password" + (f" for user '{username}':" if username is not None else ":"),
            is_password=True,
            **kw
        )
        if password != _password:
            warn("Passwords do not match! Please try again.", stack=False)
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
            "Email" + (f" for user '{username}'" if username is not None else "") +
            (" (empty to omit):" if allow_omit else ": "),
            **kw
        )
        if (allow_omit and email == '') or is_valid_email(email):
            return email
        warn("Invalid email! Please try again.", stack=False)


def check_noask(noask: bool = False) -> bool:
    """
    Flip `noask` to `True` if `MRSM_NOASK` is set.
    """
    from meerschaum._internal.static import STATIC_CONFIG
    NOASK = STATIC_CONFIG['environment']['noask']
    if noask:
        return True
    return (
        os.environ.get(NOASK, 'false').lower()
        in ('1', 'true')
    )


def get_connectors_completer(*types: str):
    """
    Return a prompt-toolkit Completer object to pass into `prompt()`.
    """
    from meerschaum.utils.misc import get_connector_labels
    prompt_toolkit_completion = mrsm.attempt_import('prompt_toolkit.completion', lazy=False)
    Completer = prompt_toolkit_completion.Completer
    Completion = prompt_toolkit_completion.Completion

    class ConnectorCompleter(Completer):
        def get_completions(self, document, complete_event):
            for label in get_connector_labels(*types, search_term=document.text):
                yield Completion(label, start_position=(-1 * len(document.text)))

    return ConnectorCompleter()
